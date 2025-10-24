"""Tushare exchange subclass providing Chinese stock market data."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from pandas import DataFrame, concat

from freqtrade.enums import CandleType, MarginMode, TradingMode
from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import FtHas, OHLCVResponse
from freqtrade.exchange.exchange_utils_timeframe import timeframe_to_msecs
from freqtrade.util import dt_ts

logger = logging.getLogger(__name__)


class _TushareClientBase:
    """
    Minimal client to satisfy the ccxt interface the parent Exchange expects.
    """

    def __init__(self, timeframes: dict[str, str]):
        self.name = "Tushare"
        self.id = "tushare"
        self.timeframes: dict[str, str] = timeframes
        self.markets: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        self.precisionMode: int = 2  # Daily data, ignore precision handling
        self.session = None
        self.has: dict[str, bool] = {
            "fetchOHLCV": True,
            "fetchTicker": False,
            "fetchOrderBook": False,
            "fetchTrades": False,
        }
        self.features: dict[str, Any] = {
            "spot": {"fetchOHLCV": {"limit": 5000}},
        }

    def set_markets_from_exchange(self, other: "_TushareClientBase") -> None:
        self.markets = getattr(other, "markets", {})

    def load_markets(self, reload: bool = False, params: dict | None = None) -> dict[str, Any]:
        return self.markets

    def describe(self) -> dict[str, Any]:
        return {"fees": {}}

    def close(self):
        return None


class _TushareAsyncClient(_TushareClientBase):
    async def load_markets(self, reload: bool = False, params: dict | None = None) -> dict[str, Any]:
        return self.markets

    async def close(self):
        return None


class Tushare(Exchange):
    """
    Exchange implementation retrieving OHLCV data from the Tushare `pro` API.
    """

    _ft_has: FtHas = {
        "always_require_api_keys": True,
        "ohlcv_partial_candle": False,
        "ohlcv_has_history": True,
        "trades_has_history": False,
        "download_data_parallel_quick": False,
        "needs_trading_fees": False,
        "tickers_have_price": False,
        "tickers_have_percentage": False,
        "tickers_have_bid_ask": False,
        "tickers_have_quoteVolume": False,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE)
    ]

    # Officially documented timeframe diff between days is the only stable option for daily data
    _SUPPORTED_TIMEFRAMES: dict[str, str] = {"1d": "1d"}
    _DEFAULT_QUOTE = "CNY"

    def __init__(self, *args, **kwargs) -> None:
        validate_requested = kwargs.pop("validate", True)

        config = kwargs.get("config")
        if not config:
            raise OperationalException("Missing configuration for Tushare exchange.")
        exchange_conf = kwargs.get("exchange_config") or config.get("exchange", {})

        # Enforce spot trading mode since tushare only supports spot data.
        if config.get("trading_mode") and config["trading_mode"] != TradingMode.SPOT.value:
            logger.warning(
                "Tushare exchange only supports spot mode. Overriding trading_mode=%s to spot.",
                config["trading_mode"],
            )
        config["trading_mode"] = TradingMode.SPOT.value
        if config.get("margin_mode") and config["margin_mode"] != MarginMode.NONE.value:
            logger.warning(
                "Tushare exchange does not support margin modes. Overriding margin_mode=%s to none.",
                config["margin_mode"],
            )
        config["margin_mode"] = MarginMode.NONE.value

        try:
            import tushare as ts  # type: ignore[import]
        except ImportError as exc:  # pragma: no cover - defensive import guard
            raise OperationalException(
                "Tushare exchange requires the `tushare` package. "
                "Install it with `pip install tushare`."
            ) from exc

        token = (
            exchange_conf.get("token")
            or exchange_conf.get("api_token")
            or exchange_conf.get("apiKey")
            or exchange_conf.get("key")
        )
        if not token:
            raise OperationalException(
                "Tushare exchange requires an API token. "
                "Set `exchange.token` (or `api_token`) in the configuration."
            )

        self._ts_module = ts
        try:
            self._pro = ts.pro_api(token)
        except Exception as exc:  # pragma: no cover - library specific error
            raise OperationalException(f"Could not initialize tushare client: {exc}") from exc

        self._quote_currency = exchange_conf.get("quote_currency", self._DEFAULT_QUOTE)
        self._markets_filter: list[str] = exchange_conf.get("stock_exchanges", [])
        self._timeframes_map = self._SUPPORTED_TIMEFRAMES.copy()
        self._last_metadata_fetch_ms: int = 0

        super().__init__(*args, validate=False, **kwargs)

        # Override ccxt placeholders with supported timeframes
        self._api.timeframes = self._timeframes_map
        self._api_async.timeframes = self._timeframes_map

        self.reload_markets(force=True)
        self._set_startup_candle_count(self._config)

        if validate_requested:
            self.validate_config(self._config)

    def validate_config(self, config):
        """
        Reduced validation compared to ccxt exchanges - focused on data requirements.
        """
        self.validate_timeframes(config.get("timeframe"))
        stake_currency = config.get("stake_currency")
        if stake_currency:
            self.validate_stakecurrency(stake_currency)
        self.validate_freqai(config)
        self._set_startup_candle_count(config)

    def _init_ccxt(
        self, exchange_config: dict[str, Any], sync: bool, ccxt_kwargs: dict[str, Any]
    ):
        if sync:
            return _TushareClientBase(self._SUPPORTED_TIMEFRAMES.copy())
        return _TushareAsyncClient(self._SUPPORTED_TIMEFRAMES.copy())

    def reload_markets(self, force: bool = False, *, load_leverage_tiers: bool = True) -> None:
        """
        Populate self._markets with instruments returned by Tushare.
        """
        if (
            not force
            and self._last_markets_refresh > 0
            and (self._last_markets_refresh + self.markets_refresh_interval > dt_ts())
        ):
            return

        now_ms = dt_ts()
        if (
            self._last_metadata_fetch_ms
            and now_ms - self._last_metadata_fetch_ms < 60_000
        ):
            logger.info(
                "Skipping Tushare market reload: last metadata fetch was %s seconds ago.",
                int((now_ms - self._last_metadata_fetch_ms) / 1000),
            )
            return

        try:
            markets_df = self._load_stock_metadata()
        except Exception as exc:  # pragma: no cover - defensive logging
            raise TemporaryError(f"Could not load markets from Tushare: {exc}") from exc

        markets: dict[str, dict[str, Any]] = {}
        if markets_df is None or markets_df.empty:
            raise OperationalException("No markets returned by Tushare.")

        for row in markets_df.to_dict("records"):
            ts_code = row["ts_code"]
            quote = row.get("curr_type") or self._quote_currency
            pair = f"{ts_code}/{quote}"
            markets[pair] = {
                "symbol": pair,
                "id": ts_code,
                "ts_code": ts_code,
                "base": ts_code,
                "quote": quote,
                "name": row.get("name"),
                "active": (row.get("list_status") or "L").upper() == "L",
                "type": "spot",
                "spot": True,
                "info": row,
            }

        self._markets = markets
        self._api.markets = markets
        self._api_async.markets = markets
        self._last_markets_refresh = dt_ts()
        self._last_metadata_fetch_ms = now_ms

    def _load_stock_metadata(self):
        """
        Retrieve stock listings from tushare, filtered by configured exchanges if provided.
        """
        fields = "ts_code,name,exchange,list_status,market,curr_type"
        # Temporarily disable stock_basic calls to avoid exhausting Tushare quota.
        # Original implementation kept for reference once API quota is available.
        # if self._markets_filter:
        #     frames = []
        #     for exchange in self._markets_filter:
        #         frames.append(
        #             self._pro.stock_basic(
        #                 exchange=exchange,
        #                 list_status="L",
        #                 fields=fields,
        #             )
        #         )
        #     return DataFrame() if not frames else concat(frames, ignore_index=True)
        # return self._pro.stock_basic(list_status="L", fields=fields)

        exchange_conf = self._config.get("exchange", {})
        pairs_cfg = set(exchange_conf.get("pair_whitelist", []))
        pairs_cfg.update(exchange_conf.get("pairs", []))
        pairs_cfg.update(self._config.get("pairs", []))
        for pairlist_conf in self._config.get("pairlists", []):
            # Allow both legacy (pairlist.config.*) and top-level definitions.
            config = pairlist_conf.get("config", {})
            pairs_cfg.update(config.get("pair_whitelist", []))
            pairs_cfg.update(config.get("pairs", []))

            pairs_cfg.update(pairlist_conf.get("pair_whitelist", []))
            pairs_cfg.update(pairlist_conf.get("pairs", []))

            if pairlist_conf.get("method") == "RemotePairList":
                url = pairlist_conf.get("pairlist_url", "")
                if url.startswith("file:///"):
                    file_path = Path(url.split("file:///", 1)[1])
                    if file_path.exists():
                        try:
                            with file_path.open() as json_file:
                                data = json.load(json_file)
                            pairs_cfg.update(data.get("pairs", []))
                        except Exception as exc:
                            logger.warning(
                                "Failed to load remote pairlist from %s: %s", file_path, exc
                            )

        pairs = self.normalize_pairs(list(pairs_cfg))
        if pairs:
            self._config.setdefault("exchange", {})["pair_whitelist"] = pairs
        if not pairs:
            logger.warning(
                "pair_whitelist is empty and stock_basic is disabled. No markets will be available."
            )
            return DataFrame(columns=fields.split(","))

        rows: list[dict[str, Any]] = []
        for pair in pairs:
            if "/" in pair:
                ts_code, quote = pair.split("/", 1)
            else:
                ts_code, quote = pair, self._quote_currency
            rows.append(
                {
                    "ts_code": ts_code,
                    "name": ts_code,
                    "exchange": "",
                    "list_status": "L",
                    "market": "",
                    "curr_type": quote,
                }
            )

        return DataFrame(rows, columns=fields.split(","))

    def normalize_pairs(self, pairs: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for pair in pairs:
            if "/" in pair:
                if pair not in seen:
                    normalized.append(pair)
                    seen.add(pair)
            else:
                norm = f"{pair}/{self._quote_currency}"
                if norm not in seen:
                    normalized.append(norm)
                    seen.add(norm)
        return normalized

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        """
        Override default tradability check for static tushare markets.
        """
        return bool(market.get("base")) and bool(market.get("quote"))

    async def _async_get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        raise_: bool = False,
        until_ms: int | None = None,
    ) -> OHLCVResponse:
        ticks = await asyncio.to_thread(
            self._fetch_tushare_ohlcv,
            pair,
            timeframe,
            candle_type,
            since_ms,
            until_ms,
        )
        return pair, timeframe, candle_type, ticks, False

    def refresh_latest_ohlcv(
        self,
        pair_list,
        *,
        since_ms: int | None = None,
        cache: bool = True,
        drop_incomplete: bool | None = None,
    ) -> dict:
        """
        Sequential version tailored for the synchronous tushare client.
        """
        logger.debug(
            "Tushare refresh_latest_ohlcv called for %d pairs (since_ms=%s, cache=%s).",
            len(pair_list),
            since_ms,
            cache,
        )
        results: dict = {}
        drop_incomplete = False if drop_incomplete is None else drop_incomplete
        for pair, timeframe, candle_type in set(pair_list):
            if candle_type != CandleType.SPOT:
                continue

            fetch_since = since_ms
            if cache and fetch_since is None and (pair, timeframe, candle_type) in self._klines:
                cached_df = self._klines[(pair, timeframe, candle_type)]
                if not cached_df.empty:
                    last_date = cached_df.iloc[-1]["date"]
                    fetch_since = int(last_date.timestamp() * 1000) - timeframe_to_msecs(timeframe)

            try:
                ticks = self._fetch_tushare_ohlcv(pair, timeframe, candle_type, fetch_since)
            except TemporaryError as exc:
                logger.warning(
                    "Failed to fetch OHLCV for %s due to temporary error: %s", pair, exc
                )
                continue

            if not ticks:
                logger.debug("No OHLCV ticks returned for %s.", pair)
                continue

            df = self._process_ohlcv_df(
                pair, timeframe, candle_type, ticks, cache, drop_incomplete
            )
            results[(pair, timeframe, candle_type)] = df

        return results

    def _fetch_tushare_ohlcv(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: int | None = None,
        until_ms: int | None = None,
    ) -> list[list]:
        if candle_type != CandleType.SPOT:
            raise OperationalException(
                f"Tushare supports only spot candles. Requested candle type: {candle_type}"
            )

        if timeframe not in self._SUPPORTED_TIMEFRAMES:
            raise OperationalException(
                f"Tushare supports only {', '.join(self._SUPPORTED_TIMEFRAMES)} timeframe(s). "
                f"Requested: {timeframe}"
            )

        ts_code = pair.split("/")[0]
        start = self._ms_to_trade_date(since_ms)
        end = self._ms_to_trade_date(until_ms) if until_ms else None

        params: dict[str, Any] = {"ts_code": ts_code}
        if start:
            params["start_date"] = start
        if end:
            params["end_date"] = end

        try:
            df = self._pro.daily(**params)
            logger.info(
                "Fetched %s daily OHLCV entries for %s from Tushare (since: %s, until: %s).",
                len(df) if df is not None else 0,
                pair,
                start,
                end,
            )
        except Exception as exc:  # pragma: no cover - API/network error
            logger.warning("Tushare daily data request failed for %s: %s", pair, exc)
            raise TemporaryError(f"Tushare daily data request failed: {exc}") from exc

        if df is None or df.empty:
            logger.warning("Tushare returned no daily data for %s (params: %s)", pair, params)
            return []

        df = df.sort_values("trade_date")

        ticks: list[list] = []
        for row in df.itertuples():
            trade_date: datetime = datetime.strptime(row.trade_date, "%Y%m%d").replace(tzinfo=UTC)
            ts = int(trade_date.timestamp() * 1000)
            if since_ms and ts < since_ms:
                continue
            if until_ms and ts > until_ms:
                break
            ticks.append([ts, row.open, row.high, row.low, row.close, row.vol])

        return ticks

    def _ms_to_trade_date(self, ms: int | None) -> str | None:
        if not ms:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y%m%d")
