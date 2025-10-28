"""
Percent Change PairList provider

Provides dynamic pair list based on trade change
sorted based on percentage change in price over a
defined period or as coming from ticker
"""

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import TypedDict

from cachetools import TTLCache
from pandas import DataFrame, to_datetime

from freqtrade.constants import ListPairsWithTimeframes, PairWithTimeframe
from freqtrade.exceptions import OperationalException, PricingError
from freqtrade.exchange import timeframe_to_minutes, timeframe_to_prev_date
from freqtrade.exchange.exchange_types import Ticker, Tickers
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter, SupportsBacktesting
from freqtrade.util import dt_now, format_ms_time


logger = logging.getLogger(__name__)


class SymbolWithPercentage(TypedDict):
    symbol: str
    percentage: float | None


class PercentChangePairList(IPairList):
    is_pairlist_generator = True
    supports_backtesting = SupportsBacktesting.NO

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        logger.info("DEBUG: starting PercentChangePairList with config: %s", self._pairlistconfig)

        if "number_assets" not in self._pairlistconfig:
            raise OperationalException(
                "`number_assets` not specified. Please check your configuration "
                'for "pairlist.config.number_assets"'
            )

        self._stake_currency = self._config["stake_currency"]
        self._number_pairs = self._pairlistconfig["number_assets"]
        self._min_value = self._pairlistconfig.get("min_value", None)
        self._max_value = self._pairlistconfig.get("max_value", None)
        self._refresh_period = self._pairlistconfig.get("refresh_period", 1800)
        self._pair_cache: TTLCache = TTLCache(maxsize=1, ttl=self._refresh_period)
        self._lookback_days = self._pairlistconfig.get("lookback_days", 0)
        self._lookback_timeframe = self._pairlistconfig.get("lookback_timeframe", "1d")
        self._lookback_period = self._pairlistconfig.get("lookback_period", 0)
        self._use_daily_baseline = self._pairlistconfig.get("use_daily_baseline", False)
        base_dir_cfg = self._pairlistconfig.get(
            "base_cache_dir", "/freqtrade/user_data/percent_baselines"
        )
        default_base_dir = Path(self._config.get("datadir", "user_data/data")) / "percent_baselines"
        self._baseline_dir = Path(base_dir_cfg) if base_dir_cfg else default_base_dir
        self._baseline_dir.mkdir(parents=True, exist_ok=True)
        self._baseline_prices: dict[str, float] = {}
        self._baseline_file: Path | None = None
        self._baseline_date: date | None = None
        self._sort_direction: str | None = self._pairlistconfig.get("sort_direction", "desc")
        self._def_candletype = self._config["candle_type_def"]

        if (self._lookback_days > 0) & (self._lookback_period > 0):
            raise OperationalException(
                "Ambiguous configuration: lookback_days and lookback_period both set in pairlist "
                "config. Please set lookback_days only or lookback_period and lookback_timeframe "
                "and restart the bot."
            )

        # overwrite lookback timeframe and days when lookback_days is set
        if self._lookback_days > 0:
            self._lookback_timeframe = "1d"
            self._lookback_period = self._lookback_days

        # get timeframe in minutes and seconds
        self._tf_in_min = timeframe_to_minutes(self._lookback_timeframe)
        _tf_in_sec = self._tf_in_min * 60

        # whether to use range lookback or not
        self._use_range = (self._tf_in_min > 0) & (
            self._lookback_period > 0
        ) and not self._use_daily_baseline

        if self._use_range & (self._refresh_period < _tf_in_sec):
            raise OperationalException(
                f"Refresh period of {self._refresh_period} seconds is smaller than one "
                f"timeframe of {self._lookback_timeframe}. Please adjust refresh_period "
                f"to at least {_tf_in_sec} and restart the bot."
            )

        if (
            not self._use_range
            and not self._use_daily_baseline
            and not (
                self._exchange.exchange_has("fetchTickers")
                and self._exchange.get_option("tickers_have_percentage")
            )
        ):
            raise OperationalException(
                "Exchange does not support dynamic whitelist in this configuration. "
                "Please edit your config and either remove PercentChangePairList, "
                "or switch to using candles. and restart the bot."
            )

        if self._use_range:
            candle_limit = self._exchange.ohlcv_candle_limit(
                self._lookback_timeframe, self._def_candletype
            )
            if self._lookback_period > candle_limit:
                raise OperationalException(
                    "ChangeFilter requires lookback_period to not "
                    f"exceed exchange max request size ({candle_limit})"
                )

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty Dict is passed
        as tickers argument to filter_pairlist
        """
        return not self._use_range and not self._use_daily_baseline

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return f"{self.name} - top {self._pairlistconfig['number_assets']} percent change pairs."

    @staticmethod
    def description() -> str:
        return "Provides dynamic pair list based on percentage change."

    @staticmethod
    def available_parameters() -> dict[str, PairlistParameter]:
        return {
            "number_assets": {
                "type": "number",
                "default": 30,
                "description": "Number of assets",
                "help": "Number of assets to use from the pairlist",
            },
            "min_value": {
                "type": "number",
                "default": None,
                "description": "Minimum value",
                "help": "Minimum value to use for filtering the pairlist.",
            },
            "max_value": {
                "type": "number",
                "default": None,
                "description": "Maximum value",
                "help": "Maximum value to use for filtering the pairlist.",
            },
            "sort_direction": {
                "type": "option",
                "default": "desc",
                "options": ["", "asc", "desc"],
                "description": "Sort pairlist",
                "help": "Sort Pairlist ascending or descending by rate of change.",
            },
            **IPairList.refresh_period_parameter(),
            "lookback_days": {
                "type": "number",
                "default": 0,
                "description": "Lookback Days",
                "help": "Number of days to look back at.",
            },
            "lookback_timeframe": {
                "type": "string",
                "default": "1d",
                "description": "Lookback Timeframe",
                "help": "Timeframe to use for lookback.",
            },
            "lookback_period": {
                "type": "number",
                "default": 0,
                "description": "Lookback Period",
                "help": "Number of periods to look back at.",
            },
            "use_daily_baseline": {
                "type": "boolean",
                "default": False,
                "description": "Use today's 00:00 UTC price as baseline.",
                "help": "Ignore lookback configuration and base the change on cached daily baselines.",
            },
            "base_cache_dir": {
                "type": "string",
                "default": "",
                "description": "Directory to persist baseline prices.",
                "help": "Optional directory to store baseline cache files.",
            },
        }

    def gen_pairlist(self, tickers: Tickers) -> list[str]:
        """
        Generate the pairlist
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: List of pairs
        """
        pairlist = self._pair_cache.get("pairlist")
        if pairlist:
            # Item found - no refresh necessary
            return pairlist.copy()
        else:
            # Use fresh pairlist
            # Check if pair quote currency equals to the stake currency.
            _pairlist = [
                k
                for k in self._exchange.get_markets(
                    quote_currencies=[self._stake_currency], tradable_only=True, active_only=True
                ).keys()
            ]
            logger.info(
                "PercentChangePairList fetched %d tradable markets for stake %s.",
                len(_pairlist),
                self._stake_currency,
            )

            # No point in testing for blacklisted pairs...
            _pairlist = self.verify_blacklist(_pairlist, logger.info)
            logger.info(
                "PercentChangePairList after blacklist has %d markets (sample: %s)",
                len(_pairlist),
                _pairlist[:5],
            )
            if not self._use_range and not self._use_daily_baseline:
                filtered_tickers = [
                    v
                    for k, v in tickers.items()
                    if (
                        self._exchange.get_pair_quote_currency(k) == self._stake_currency
                        and v["symbol"] in _pairlist
                    )
                ]
                pairlist = [s["symbol"] for s in filtered_tickers]
            else:
                pairlist = _pairlist

            logger.info(
                "PercentChangePairList passing %d markets into filter phase.",
                len(pairlist),
            )

            pairlist = self.filter_pairlist(pairlist, tickers)
            self._pair_cache["pairlist"] = pairlist.copy()

        return pairlist

    def filter_pairlist(self, pairlist: list[str], tickers: dict) -> list[str]:
        """
        Filters and sorts pairlist and returns the whitelist again.
        Called on each bot iteration - please use internal caching if necessary
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: new whitelist
        """
        filtered_tickers: list[SymbolWithPercentage] = [
            {"symbol": k, "percentage": None} for k in pairlist
        ]
        logger.info(
            "Evaluating %d candidate pairs with mode=%s (min=%s, max=%s)",
            len(filtered_tickers),
            (
                "daily_baseline"
                if self._use_daily_baseline
                else "range" if self._use_range else "tickers"
            ),
            self._min_value,
            self._max_value,
        )
        if self._use_daily_baseline:
            filtered_tickers = self.fetch_percent_change_from_baseline(filtered_tickers)
        elif self._use_range:
            filtered_tickers = self.fetch_percent_change_from_lookback_period(filtered_tickers)
        else:
            filtered_tickers = self.fetch_percent_change_from_tickers(filtered_tickers, tickers)

        filtered_with_reason: list[SymbolWithPercentage] = []
        for entry in filtered_tickers:
            pct = entry["percentage"]
            if pct is None:
                self.log_once(
                    f"Removed {entry['symbol']} from whitelist, because percentage could not be calculated.",
                    logger.info,
                )
                continue

            include = True
            if (
                self._min_value is not None
                and self._max_value is not None
                and self._min_value > self._max_value
            ):
                include = pct >= self._min_value or pct <= self._max_value
                if not include:
                    self.log_once(
                        f"Removed {entry['symbol']} from whitelist, because change {pct:.3f}% is not >= {self._min_value} or <= {self._max_value}.",
                        logger.info,
                    )
            else:
                if self._min_value is not None and pct <= self._min_value:
                    self.log_once(
                        f"Removed {entry['symbol']} from whitelist, because change {pct:.3f}% <= min_value {self._min_value}.",
                        logger.info,
                    )
                    include = False
                if include and self._max_value is not None and pct >= self._max_value:
                    self.log_once(
                        f"Removed {entry['symbol']} from whitelist, because change {pct:.3f}% >= max_value {self._max_value}.",
                        logger.info,
                    )
                    include = False

            if include:
                filtered_with_reason.append(entry)

        filtered_tickers = filtered_with_reason
        if pairlist and not filtered_tickers:
            logger.info(
                "PercentChangePairList removed all %d candidate pairs after filtering.",
                len(pairlist),
            )

        sorted_tickers = sorted(
            filtered_tickers,
            reverse=self._sort_direction == "desc",
            key=lambda t: t["percentage"],  # type: ignore
        )

        # Validate whitelist to only have active market pairs
        pairs = self._whitelist_for_active_markets([s["symbol"] for s in sorted_tickers])
        pairs = self.verify_blacklist(pairs, logmethod=logger.info)
        # Limit pairlist to the requested number of pairs
        pairs = pairs[: self._number_pairs]

        return pairs

    def fetch_candles_for_lookback_period(
        self, filtered_tickers: list[SymbolWithPercentage]
    ) -> dict[PairWithTimeframe, DataFrame]:
        since_ms = (
            int(
                timeframe_to_prev_date(
                    self._lookback_timeframe,
                    dt_now()
                    + timedelta(
                        minutes=-(self._lookback_period * self._tf_in_min) - self._tf_in_min
                    ),
                ).timestamp()
            )
            * 1000
        )
        to_ms = (
            int(
                timeframe_to_prev_date(
                    self._lookback_timeframe, dt_now() - timedelta(minutes=self._tf_in_min)
                ).timestamp()
            )
            * 1000
        )
        self.log_once(
            f"Using change range of {self._lookback_period} candles, timeframe: "
            f"{self._lookback_timeframe}, starting from {format_ms_time(since_ms)} "
            f"till {format_ms_time(to_ms)}",
            logger.info,
        )
        needed_pairs: ListPairsWithTimeframes = [
            (p, self._lookback_timeframe, self._def_candletype)
            for p in [s["symbol"] for s in filtered_tickers]
            if p not in self._pair_cache
        ]
        candles = self._exchange.refresh_ohlcv_with_cache(needed_pairs, since_ms)
        return candles

    def fetch_percent_change_from_lookback_period(
        self, filtered_tickers: list[SymbolWithPercentage]
    ) -> list[SymbolWithPercentage]:
        # get lookback period in ms, for exchange ohlcv fetch
        candles = self.fetch_candles_for_lookback_period(filtered_tickers)

        for i, p in enumerate(filtered_tickers):
            pair_candles = (
                candles[(p["symbol"], self._lookback_timeframe, self._def_candletype)]
                if (p["symbol"], self._lookback_timeframe, self._def_candletype) in candles
                else None
            )

            # in case of candle data calculate typical price and change for candle
            if pair_candles is not None and not pair_candles.empty:
                current_close = pair_candles["close"].iloc[-1]
                previous_close = pair_candles["close"].shift(self._lookback_period).iloc[-1]
                pct_change = (
                    ((current_close - previous_close) / previous_close) * 100
                    if previous_close > 0
                    else 0
                )

                # replace change with a range change sum calculated above
                filtered_tickers[i]["percentage"] = pct_change
            else:
                filtered_tickers[i]["percentage"] = 0
        return filtered_tickers

    def fetch_percent_change_from_tickers(
        self, filtered_tickers: list[SymbolWithPercentage], tickers
    ) -> list[SymbolWithPercentage]:
        valid_tickers: list[SymbolWithPercentage] = []
        for p in filtered_tickers:
            # Filter out assets
            if (
                self._validate_pair(
                    p["symbol"], tickers[p["symbol"]] if p["symbol"] in tickers else None
                )
                and p["symbol"] != "UNI/USDT"
            ):
                p["percentage"] = tickers[p["symbol"]]["percentage"]
                valid_tickers.append(p)
        return valid_tickers

    def fetch_percent_change_from_baseline(
        self, filtered_tickers: list[SymbolWithPercentage]
    ) -> list[SymbolWithPercentage]:
        logger.info(
            "PercentChangePairList loading baselines for %d symbols (cache=%s).",
            len(filtered_tickers),
            self._baseline_dir,
        )
        self._load_baseline_prices()
        logger.info(
            "PercentChangePairList current baseline cache contains %d symbols.",
            len(self._baseline_prices),
        )
        today = dt_now().date()
        needed_pairs: ListPairsWithTimeframes = [
            (p["symbol"], "1d", self._def_candletype)
            for p in filtered_tickers
            if p["symbol"] not in self._baseline_prices
        ]

        if needed_pairs:
            candles = self._exchange.refresh_latest_ohlcv(needed_pairs, cache=True)
            updated = False
            for pair, timeframe, candle_type in needed_pairs:
                df = candles.get((pair, timeframe, candle_type))
                if df is None or df.empty:
                    logger.info(
                        "Skipped baseline creation for %s due to missing candle data.", pair
                    )
                    continue
                row = df.iloc[-1]
                row_date = row["date"]
                if not isinstance(row_date, date):
                    row_date = to_datetime(row_date, utc=True).date()
                if row_date == today:
                    baseline_price = float(row.get("open", row.get("close", 0)))
                else:
                    baseline_price = float(row.get("close", 0))
                if baseline_price > 0:
                    self._baseline_prices[pair] = baseline_price
                    updated = True
                    logger.debug(
                        "Baseline price for %s set to %.8f (date %s).",
                        pair,
                        baseline_price,
                        row_date,
                    )
                else:
                    logger.info(
                        "Baseline price for %s invalid (%.8f).",
                        pair,
                        baseline_price,
                    )
                    self.log_once(
                        f"No valid baseline price for {pair}. "
                        "Percentage change cannot be calculated.",
                        logger.info,
                    )
            if updated:
                self._save_baseline_prices()

        for entry in filtered_tickers:
            pair = entry["symbol"]
            base_price = self._baseline_prices.get(pair)
            pct = None
            if base_price and base_price > 0:
                try:
                    current_price = float(
                        self._exchange.get_rate(
                            pair,
                            side="entry",
                            is_short=False,
                            refresh=False,
                        )
                    )
                except PricingError:
                    logger.info(
                        "Removed %s from whitelist, because current price is unavailable.", pair
                    )
                    current_price = None
                if current_price is not None:
                    pct = ((current_price - base_price) / base_price) * 100
                    logger.debug(
                        "Percent change for %s using baseline %.8f and current %.8f is %.3f%%.",
                        pair,
                        base_price,
                        current_price,
                        pct,
                    )
            else:
                logger.info(
                    "Removed %s from whitelist, because baseline price is missing or invalid.",
                    pair,
                )
                self.log_once(
                    f"No valid baseline price for {pair}. "
                    "Percentage change cannot be calculated.",
                    logger.info,
                )
            entry["percentage"] = pct
        logger.info(
            "PercentChangePairList baseline results: %d valid, %d missing",
            len([x for x in filtered_tickers if x["percentage"] is not None]),
            len([x for x in filtered_tickers if x["percentage"] is None]),
        )
        return filtered_tickers

    def _load_baseline_prices(self) -> None:
        today = dt_now().date()
        if self._baseline_date == today:
            return
        file_path = self._baseline_dir / f"baseline_{today.isoformat()}.json"
        prices: dict[str, float] = {}
        if file_path.exists():
            try:
                with file_path.open("r", encoding="utf-8") as fp:
                    data = json.load(fp)
                prices = {k: float(v) for k, v in data.items() if float(v) > 0}
            except Exception:
                logger.warning("Could not load baseline cache from %s", file_path)
                prices = {}
        self._baseline_prices = prices
        self._baseline_file = file_path
        self._baseline_date = today

    def _save_baseline_prices(self) -> None:
        if not self._baseline_file:
            return
        try:
            with self._baseline_file.open("w", encoding="utf-8") as fp:
                json.dump(self._baseline_prices, fp, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning("Failed to persist baseline cache to %s: %s", self._baseline_file, exc)

    def _validate_pair(self, pair: str, ticker: Ticker | None) -> bool:
        """
        Check if one price-step (pip) is > than a certain barrier.
        :param pair: Pair that's currently validated
        :param ticker: ticker dict as returned from ccxt.fetch_ticker
        :return: True if the pair can stay, false if it should be removed
        """
        if not ticker or "percentage" not in ticker or ticker["percentage"] is None:
            self.log_once(
                f"Removed {pair} from whitelist, because "
                "ticker['percentage'] is empty (Usually no trade in the last 24h).",
                logger.info,
            )
            return False

        return True
