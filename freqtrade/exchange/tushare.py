"""Tushare exchange subclass providing Chinese stock market data."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from datetime import UTC, datetime, timedelta, time as dt_time
from functools import partial
from pathlib import Path
from typing import Any

from pandas import DataFrame, concat, to_datetime
from pandas.api.types import is_datetime64_any_dtype

from freqtrade.enums import CandleType, MarginMode, TradingMode
from freqtrade.exceptions import OperationalException, PricingError, TemporaryError
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import FtHas, OHLCVResponse
from freqtrade.constants import BuySell
from freqtrade.exchange.exchange_utils import ROUND_DOWN, ROUND_UP
from freqtrade.exchange.exchange_utils_timeframe import timeframe_to_msecs
from freqtrade.util import dt_ts
from zoneinfo import ZoneInfo
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from freqtrade.data.history import get_datahandler

logger = logging.getLogger(__name__)

try:
    import holidays
except ImportError:  # pragma: no cover - optional dependency
    holidays = None


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
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"market": "market", "limit": "limit"},
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
        rate_limit_conf = exchange_conf.get("rate_limit", {})
        self._daily_rate_limit = int(rate_limit_conf.get("daily_limit_per_minute", 50))
        self._daily_period = int(rate_limit_conf.get("daily_period_seconds", 60))
        self._daily_call_times: deque[float] = deque()
        self._holiday_country = exchange_conf.get("holiday_country", "CN")
        self._copy_holiday_candles: bool = exchange_conf.get("copy_holiday_candles", False)
        if self._copy_holiday_candles and holidays is None:
            logger.warning(
                "copy_holiday_candles is enabled but the 'holidays' package is not installed. "
                "Disabling holiday candle copying."
            )
            self._copy_holiday_candles = False
        self._holiday_cache: dict[int, Any] = {}
        tz_name = exchange_conf.get("market_timezone", "Asia/Shanghai")
        try:
            self._market_timezone = ZoneInfo(tz_name)
        except Exception:
            logger.warning("Invalid market_timezone '%s'. Falling back to Asia/Shanghai", tz_name)
            self._market_timezone = ZoneInfo("Asia/Shanghai")
        close_time_str = exchange_conf.get("market_close_time", "15:30")
        try:
            close_hour, close_minute = [int(x) for x in close_time_str.split(":", 1)]
            self._market_close_time = dt_time(close_hour, close_minute)
        except Exception:
            logger.warning("Invalid market_close_time '%s'. Falling back to 15:30", close_time_str)
            self._market_close_time = dt_time(15, 30)
        self._no_data_until: dict[str, datetime] = {}

        super().__init__(*args, validate=False, **kwargs)

        # Override ccxt placeholders with supported timeframes
        self._api.timeframes = self._timeframes_map
        self._api_async.timeframes = self._timeframes_map

        self._datadir = Path(self._config.get("datadir", "user_data/data"))
        self._datadir.mkdir(parents=True, exist_ok=True)
        from freqtrade.data.history import get_datahandler

        self._datahandler = get_datahandler(
            self._datadir, data_format=self._config.get("dataformat_ohlcv")
        )

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
                "precision": {"price": 4, "amount": 0},
                "limits": {
                    "amount": {"min": 1.0, "max": None},
                    "price": {"min": None, "max": None},
                    "cost": {"min": 0.0, "max": None},
                },
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
            self._store_cached_dataframe(pair, timeframe, df)
            results[(pair, timeframe, candle_type)] = self._apply_holiday_fill(
                df.copy(), timeframe
            )

        return results
    def get_rate(
        self,
        pair: str,
        *,
        side: str = "entry",
        is_short: bool = False,
        refresh: bool = False,
        price_type: str = "last",
        order_type: str | None = None,
        price: float | None = None,
    ) -> float:
        """
        Return latest price for the pair without relying on orderbook/ticker endpoints.
        """
        if refresh:
            # Ensure cache is up-to-date when explicitly requested.
            self.refresh_latest_ohlcv(
                [(pair, self._config.get("timeframe", "1d"), CandleType.SPOT)],
                cache=True,
            )

        price_value = self._get_latest_price(pair)
        if price_value is None:
            raise PricingError(f"Price data unavailable for {pair}.")
        return price_value

    def fetch_l2_order_book(self, pair: str, limit: int | None = None) -> dict[str, Any]:
        """
        Provide a minimal synthetic order book for compatibility with freqtrade core logic.
        """
        price_value = self._get_latest_price(pair)
        if price_value is None:
            raise PricingError(f"Order book unavailable for {pair}.")

        depth = max(limit or 1, 1)
        bids = [[price_value, 1.0] for _ in range(depth)]
        asks = [[price_value, 1.0] for _ in range(depth)]
        now_ms = dt_ts()
        return {
            "symbol": pair,
            "bids": bids,
            "asks": asks,
            "timestamp": now_ms,
            "datetime": datetime.fromtimestamp(now_ms / 1000, tz=UTC).isoformat(),
            "nonce": None,
        }

    def fetch_ticker(self, pair: str) -> dict[str, Any]:
        """
        Return a minimal ticker structure used by freqtrade for pricing.
        """
        price_value = self._get_latest_price(pair)
        if price_value is None:
            raise PricingError(f"Ticker unavailable for {pair}.")
        now_ms = dt_ts()
        return {
            "symbol": pair,
            "bid": price_value,
            "ask": price_value,
            "last": price_value,
            "close": price_value,
            "timestamp": now_ms,
            "datetime": datetime.fromtimestamp(now_ms / 1000, tz=UTC).isoformat(),
        }

    def get_fee(
        self,
        symbol: str,
        order_type: str = "",
        side: str = "",
        amount: float = 1.0,
        price: float = 1.0,
        taker_or_maker: str = "taker",
    ) -> float:
        """Return a simplified fee rate for tushare (defaults to config fee or zero)."""
        config_fee = self._config.get("fee")
        if self._config.get("dry_run") and config_fee is not None:
            return config_fee
        if isinstance(config_fee, (int, float)) and config_fee is not None:
            return float(config_fee)
        return 0.0

    def create_stoploss(
        self,
        pair: str,
        amount: float,
        stop_price: float,
        order_types: dict,
        side: BuySell,
        leverage: float,
    ) -> dict[str, Any]:
        """Create a synthetic stoploss order for dry-run support."""
        if not self._config.get("dry_run", False):
            raise OperationalException("Tushare stoploss is only available in dry_run mode.")

        round_mode = ROUND_DOWN if side == "buy" else ROUND_UP
        stop_price_norm = self.price_to_precision(pair, stop_price, rounding_mode=round_mode)

        return self.create_dry_run_order(
            pair,
            order_types.get("stoploss", "market"),
            side,
            amount,
            stop_price_norm,
            stop_loss=True,
            leverage=leverage,
        )

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

        cached_ticks = self._get_cached_ticks(pair, timeframe, since_ms)
        if cached_ticks is not None:
            return cached_ticks

        key = (pair, timeframe, candle_type)
        cached_df_full = self._klines.get(key)
        if cached_df_full is not None and not cached_df_full.empty:
            last_cached_dt = cached_df_full.iloc[-1]["date"]
            if isinstance(last_cached_dt, datetime):
                last_cached_dt = (
                    last_cached_dt.astimezone(UTC)
                    if last_cached_dt.tzinfo
                    else last_cached_dt.replace(tzinfo=UTC)
                )
            last_cached_ms = int(last_cached_dt.timestamp() * 1000)
            since_ms = max((since_ms or 0), last_cached_ms + 1)

        ts_code = pair.split("/")[0]
        start = self._ms_to_trade_date(since_ms)
        end = self._ms_to_trade_date(until_ms) if until_ms else None

        skip_until = self._no_data_until.get(pair)
        now_utc = datetime.now(UTC)
        if skip_until and now_utc < skip_until:
            logger.debug(
                "Skipping OHLCV fetch for %s until %s due to previous no-data response.",
                pair,
                skip_until,
            )
            return []

        now_market = datetime.now(self._market_timezone)
        today_date = now_market.date()
        if not self._is_trading_day(today_date):
            logger.debug(
                "Today (%s) is not a trading day. Skipping fetch for %s.", today_date, pair
            )
            self._no_data_until[pair] = self._calculate_next_fetch_time()
            return self._get_cached_ticks(pair, timeframe, since_ms) or []

        close_dt_market = datetime.combine(today_date, self._market_close_time, tzinfo=self._market_timezone)
        if now_market < close_dt_market:
            logger.debug(
                "Market still open (closes %s). Using cached data for %s.",
                close_dt_market,
                pair,
            )
            cached_ticks = self._get_cached_ticks(pair, timeframe, since_ms)
            self._no_data_until[pair] = close_dt_market.astimezone(UTC)
            return cached_ticks or []

        params: dict[str, Any] = {"ts_code": ts_code}
        if start:
            params["start_date"] = start
        if end:
            params["end_date"] = end

        try:
            df = self._call_daily_with_rate_limit(params)
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
            self._no_data_until[pair] = self._calculate_next_fetch_time()
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

        self._no_data_until.pop(pair, None)

        return ticks

    def _ms_to_trade_date(self, ms: int | None) -> str | None:
        if not ms:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y%m%d")

    def _get_cached_dataframe(self, pair: str, timeframe: str) -> DataFrame:
        key = (pair, timeframe, CandleType.SPOT)
        df = self._klines.get(key)
        if df is not None and not df.empty:
            return df

        try:
            df = self._datahandler.ohlcv_load(
                pair, timeframe, candle_type=CandleType.SPOT
            )
        except FileNotFoundError:
            df = DataFrame()
        except Exception as exc:
            logger.warning("Failed to load cached OHLCV for %s: %s", pair, exc)
            df = DataFrame()

        if df is None:
            df = DataFrame()

        if not df.empty:
            if "date" in df.columns:
                if not is_datetime64_any_dtype(df["date"]):
                    df["date"] = to_datetime(df["date"], utc=True)
                elif df["date"].dt.tz is None:
                    df["date"] = df["date"].dt.tz_localize(UTC)
            df = df.sort_values("date").reset_index(drop=True)
            self._klines[key] = df

        return df

    def _get_cached_ticks(
        self, pair: str, timeframe: str, since_ms: int | None
    ) -> list[list] | None:
        df = self._get_cached_dataframe(pair, timeframe)
        if df.empty:
            return None

        since_dt = datetime.fromtimestamp(since_ms / 1000, tz=UTC) if since_ms else None
        filtered = df[df["date"] >= since_dt] if since_dt else df
        if filtered.empty:
            return None

        filled_df = self._apply_holiday_fill(filtered.copy(), timeframe)
        latest_date = filled_df.iloc[-1]["date"]
        if isinstance(latest_date, datetime):
            latest_date = (
                latest_date.astimezone(UTC)
                if latest_date.tzinfo
                else latest_date.replace(tzinfo=UTC)
            )
        today = datetime.now(UTC).date()

        now = datetime.now(UTC)
        if since_dt is None:
            # logger.info(
            #     "Using cached historical OHLCV for %s (%d rows).", pair, len(filled_df)
            # )
            return self._df_to_ticks(filled_df)

        if latest_date.date() >= today:
            logger.debug(
                "Using cached OHLCV for %s - latest candle %s.", pair, latest_date
            )
            return self._df_to_ticks(filled_df)

        if not self._is_trading_day(now.date()):
            logger.debug(
                "Non-trading day detected - using cached OHLCV for %s (latest candle %s).",
                pair,
                latest_date,
            )
            return self._df_to_ticks(filled_df)

        return None

    def _df_to_ticks(self, df: DataFrame) -> list[list]:
        if df.empty:
            return []
        df_sorted = df.sort_values("date")
        ticks: list[list] = []
        for row in df_sorted.itertuples(index=False):
            ts = row.date
            if isinstance(ts, datetime):
                ts_dt = ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)
            else:
                ts_dt = datetime.fromtimestamp(ts, tz=UTC)
            volume = getattr(row, "volume", getattr(row, "vol", 0))
            ticks.append(
                [
                    int(ts_dt.timestamp() * 1000),
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    volume,
                ]
            )
        return ticks

    def _store_cached_dataframe(self, pair: str, timeframe: str, df: DataFrame) -> None:
        if df.empty:
            return
        try:
            cleaned = df.drop_duplicates(subset="date", keep="last").reset_index(drop=True)
            self._datahandler.ohlcv_store(
                pair, timeframe, data=cleaned, candle_type=CandleType.SPOT
            )
        except Exception as exc:
            logger.warning("Failed to store OHLCV cache for %s: %s", pair, exc)

    def _get_latest_price(self, pair: str) -> float | None:
        timeframe = self._config.get("timeframe", "1d")
        df = self._get_cached_dataframe(pair, timeframe)
        if df.empty:
            return None
        latest_row = df.iloc[-1]
        try:
            return float(latest_row["close"])
        except (KeyError, TypeError, ValueError):
            return None

    def _apply_holiday_fill(self, df: DataFrame, timeframe: str) -> DataFrame:
        if (
            not self._copy_holiday_candles
            or timeframe.lower() != "1d"
            or df.empty
            or "date" not in df.columns
        ):
            return df

        filled = df.copy()
        filled["date"] = to_datetime(filled["date"], utc=True)
        last_date = filled.iloc[-1]["date"]
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        while last_date.date() < today.date():
            next_date = last_date + timedelta(days=1)
            if self._is_trading_day(next_date.date()):
                break
            new_row = filled.iloc[-1].copy()
            new_row["date"] = next_date
            filled = concat([filled, DataFrame([new_row])], ignore_index=True)
            last_date = next_date

        return filled

    def _is_trading_day(self, day) -> bool:
        if day.weekday() >= 5:
            return False
        if holidays is None:
            return True
        cal = self._holiday_cache.get(day.year)
        if cal is None:
            try:
                cal = holidays.country_holidays(self._holiday_country, years=day.year)
            except Exception as exc:  # pragma: no cover - network/holiday error
                logger.debug(
                    "Could not load holidays for %s/%s: %s",
                    self._holiday_country,
                    day.year,
                    exc,
                )
                cal = {}
            self._holiday_cache[day.year] = cal
        try:
            return day not in cal
        except TypeError:
            return day not in set(cal)

    def _call_daily_with_rate_limit(self, params: dict[str, Any]):
        """
        Enforce Tushare documented rate limit of 50 calls per minute on the daily endpoint.
        """
        while True:
            now = time.time()
            while self._daily_call_times and now - self._daily_call_times[0] >= self._daily_period:
                self._daily_call_times.popleft()

            if len(self._daily_call_times) < self._daily_rate_limit:
                break

            wait = self._daily_period - (now - self._daily_call_times[0])
            wait = max(wait, 0.0)
            logger.info(
                "Tushare daily rate limit reached (%s calls/%ss). Sleeping for %.2f seconds.",
                self._daily_rate_limit,
                self._daily_period,
                wait,
            )
            time.sleep(wait if wait > 0 else 0.1)

        self._daily_call_times.append(time.time())
        return self._pro.daily(**params)

    def _calculate_next_fetch_time(self) -> datetime:
        """
        Determine when to retry fetching data after a no-data response.
        """
        now_market = datetime.now(self._market_timezone)
        next_market = datetime.combine(
            now_market.date(), self._market_close_time, tzinfo=self._market_timezone
        )
        if now_market >= next_market:
            next_market += timedelta(days=1)
        while not self._is_trading_day(next_market.date()):
            next_market += timedelta(days=1)
        return next_market.astimezone(UTC)
