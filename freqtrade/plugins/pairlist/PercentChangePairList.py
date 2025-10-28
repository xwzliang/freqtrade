"""
Percent Change PairList provider

Provides dynamic pair list based on trade change
sorted based on percentage change in price over a
defined period or as coming from ticker
"""

import json
import logging
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TypedDict, Any

from cachetools import TTLCache
from pandas import DataFrame, Timestamp, to_datetime

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
        self._lookback_cache_suffix = str(
            getattr(self._def_candletype, "value", self._def_candletype)
        )
        self._use_candle_any_match: bool = self._pairlistconfig.get("use_candle_any_match", False)
        lookback_cache_dir_cfg = self._pairlistconfig.get(
            "lookback_cache_dir", "/freqtrade/user_data/percent_lookback"
        )
        default_lookback_dir = (
            Path(self._config.get("datadir", "user_data/data")) / "percent_lookback"
        )
        self._lookback_cache_dir = (
            Path(lookback_cache_dir_cfg) if lookback_cache_dir_cfg else default_lookback_dir
        )
        self._lookback_cache_dir.mkdir(parents=True, exist_ok=True)
        base_period = self._lookback_period or 0
        default_cache_cap = max(365, base_period * 3) if base_period else 365
        cache_cap_cfg = self._pairlistconfig.get("lookback_cache_max_candles")
        if cache_cap_cfg is None:
            cache_cap = default_cache_cap
        else:
            try:
                cache_cap = int(cache_cap_cfg)
            except (TypeError, ValueError):
                cache_cap = default_cache_cap
        if cache_cap < 0:
            cache_cap = 0
        if cache_cap != 0 and base_period and cache_cap < base_period:
            cache_cap = base_period
        self._lookback_cache_max_candles = cache_cap
        self._lookback_cache_file: Path | None = None
        self._lookback_cache_date: date | None = None
        self._lookback_cache_data: dict[str, dict[str, Any]] = {}
        self._percentage_cache: dict[str, dict[str, Any]] = {}
        save_path = self._pairlistconfig.get("save_to_file")
        append_path = self._pairlistconfig.get("append_to_file")
        self._save_to_file: Path | None = Path(save_path) if save_path else None
        self._append_to_file: Path | None = Path(append_path) if append_path else None
        if self._save_to_file:
            self._save_to_file.parent.mkdir(parents=True, exist_ok=True)
        if self._append_to_file:
            self._append_to_file.parent.mkdir(parents=True, exist_ok=True)

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
            "use_candle_any_match": {
                "type": "boolean",
                "default": False,
                "description": "Select pairs if any candle in the lookback window matches thresholds.",
                "help": "When enabled, a single candle exceeding the configured limits is enough to include the pair.",
            },
            "lookback_cache_dir": {
                "type": "string",
                "default": "",
                "description": "Directory to persist lookback evaluation cache.",
                "help": "Optional directory to store lookback candle cache files.",
            },
            "save_to_file": {
                "type": "string",
                "default": "",
                "description": "File path to store resulting pairlist JSON.",
                "help": "If provided, the resulting pair list is written to the given path.",
            },
            "append_to_file": {
                "type": "string",
                "default": "",
                "description": "File path to append resulting pairs into existing JSON.",
                "help": "Pairs will be merged into the existing JSON file without duplicates.",
            },
            "lookback_cache_max_candles": {
                "type": "number",
                "default": 0,
                "description": "Maximum candles to retain in lookback cache.",
                "help": "Set to 0 to auto-scale (default). Positive values cap retained candles per pair.",
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
                # self.log_once(
                #     f"Removed {entry['symbol']} from whitelist, because percentage could not be calculated.",
                #     logger.info,
                # )
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

        self._persist_result(pairs)
        return pairs

    def _persist_result(self, pairs: list[str]) -> None:
        payload = {"pairs": pairs, "refresh_period": self._refresh_period}
        if self._save_to_file:
            try:
                with self._save_to_file.open("w", encoding="utf-8") as fp:
                    json.dump(payload, fp, ensure_ascii=False, indent=2)
                logger.info(
                    "PercentChangePairList saved %d pairs to %s.",
                    len(pairs),
                    self._save_to_file,
                )
            except Exception as exc:
                logger.warning(
                    "PercentChangePairList failed to save pairlist to %s: %s",
                    self._save_to_file,
                    exc,
                )

        if self._append_to_file:
            existing = {"pairs": []}
            if self._append_to_file.exists():
                try:
                    with self._append_to_file.open("r", encoding="utf-8") as fp:
                        existing = json.load(fp) or {}
                except Exception:
                    logger.warning(
                        "PercentChangePairList could not read existing file %s. Overwriting.",
                        self._append_to_file,
                    )
                    existing = {"pairs": []}
            existing_pairs = set(existing.get("pairs", []))
            initial_len = len(existing_pairs)
            for pair in pairs:
                existing_pairs.add(pair)
            merged_payload = {
                "pairs": sorted(existing_pairs),
                "refresh_period": self._refresh_period,
            }
            try:
                with self._append_to_file.open("w", encoding="utf-8") as fp:
                    json.dump(merged_payload, fp, ensure_ascii=False, indent=2)
                logger.info(
                    "PercentChangePairList appended %d new pairs to %s.",
                    len(existing_pairs) - initial_len,
                    self._append_to_file,
                )
            except Exception as exc:
                logger.warning(
                    "PercentChangePairList failed to append pairlist to %s: %s",
                    self._append_to_file,
                    exc,
                )

    def fetch_candles_for_lookback_period(
        self, filtered_tickers: list[SymbolWithPercentage], symbols_to_fetch: set[str]
    ) -> dict[PairWithTimeframe, DataFrame]:
        if not symbols_to_fetch:
            return {}
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
            if p in symbols_to_fetch
        ]
        candles = self._exchange.refresh_ohlcv_with_cache(needed_pairs, since_ms)
        return candles

    def fetch_percent_change_from_lookback_period(
        self, filtered_tickers: list[SymbolWithPercentage]
    ) -> list[SymbolWithPercentage]:
        today = dt_now().date()
        cache_updated = False
        cached_symbols: set[str] = set()
        symbols_to_fetch: set[str] = set()

        self._load_lookback_cache(today)

        if self._use_candle_any_match:
            now_dt = dt_now()
            refresh_delta = timedelta(seconds=self._refresh_period)

            for entry in filtered_tickers:
                symbol = entry["symbol"]
                cache_entry = self._lookback_cache_data.get(symbol)
                if self._is_cache_entry_fresh(cache_entry, refresh_delta, now_dt):
                    selected_pct, ordered_values = self._evaluate_cached_percentages(cache_entry)
                    if self._lookback_period > 0 and len(ordered_values) < self._lookback_period:
                        symbols_to_fetch.add(symbol)
                        continue
                    cache_entry["percentage"] = selected_pct
                    entry["percentage"] = selected_pct
                    self._percentage_cache[symbol] = cache_entry
                    cached_symbols.add(symbol)
                    if selected_pct is not None:
                        logger.info(
                            "PercentChangePairList: using cached percentage %.3f for %s.",
                            selected_pct,
                            symbol,
                        )
                    else:
                        self._log_no_candle_match(symbol, ordered_values)
                else:
                    symbols_to_fetch.add(symbol)

            if cached_symbols:
                logger.info(
                    "PercentChangePairList reused cached percentages for %d symbols; fetching %d symbols.",
                    len(cached_symbols),
                    len(filtered_tickers) - len(cached_symbols),
                )
        else:
            symbols_to_fetch = {entry["symbol"] for entry in filtered_tickers}

        candles = self.fetch_candles_for_lookback_period(filtered_tickers, symbols_to_fetch)

        for i, entry in enumerate(filtered_tickers):
            symbol = entry["symbol"]
            if self._use_candle_any_match and symbol in cached_symbols:
                continue

            pair_candles = candles.get(
                (symbol, self._lookback_timeframe, self._def_candletype)
            )

            if pair_candles is None or pair_candles.empty:
                filtered_tickers[i]["percentage"] = None
                self.log_once(
                    f"Removed {symbol} from whitelist, because no candles were available.",
                    logger.info,
                )
                continue

            pair_candles = pair_candles.sort_values("date").reset_index(drop=True)
            latest_date = pair_candles.iloc[-1]["date"]
            latest_date_key = self._normalize_candle_timestamp(latest_date) or str(latest_date)

            if self._use_candle_any_match:
                dates, percentage_map = self._build_percentage_payload(pair_candles)
                existing_entry = self._lookback_cache_data.get(symbol, {})
                merged_percentages = dict(existing_entry.get("percentages", {}))
                merged_percentages.update(percentage_map)
                merged_dates = sorted(
                    set(existing_entry.get("dates", [])) | set(merged_percentages.keys())
                )
                merged_dates, merged_percentages = self._apply_cache_limit(
                    merged_percentages, merged_dates
                )
                merged_entries = [
                    (d, merged_percentages[d]) for d in merged_dates if d in merged_percentages
                ]
                selected_pct, all_values = self._select_percentage_from_entries(merged_entries)

                updated_ts = dt_now().isoformat()
                payload = {
                    "percentage": selected_pct,
                    "last_candle": merged_dates[-1] if merged_dates else latest_date_key,
                    "percentages": {d: merged_percentages[d] for d in merged_dates},
                    "dates": merged_dates,
                    "cached_at": updated_ts,
                }
                entry["percentage"] = selected_pct
                self._percentage_cache[symbol] = payload
                self._lookback_cache_data[symbol] = payload
                cache_updated = True
                if selected_pct is not None:
                    logger.info(
                        "PercentChangePairList: %s matched candle change %.3f%% (range %.3f%% to %.3f%%).",
                        symbol,
                        selected_pct,
                        min(all_values) if all_values else 0.0,
                        max(all_values) if all_values else 0.0,
                    )
                else:
                    self._log_no_candle_match(symbol, all_values)
            else:
                cache_entry = self._percentage_cache.get(symbol)
                if cache_entry and cache_entry.get("last_candle") == latest_date_key:
                    filtered_tickers[i]["percentage"] = cache_entry.get("percentage")
                    continue

                current_close = pair_candles["close"].iloc[-1]
                previous_close = pair_candles["close"].shift(self._lookback_period).iloc[-1]
                pct_change = (
                    ((current_close - previous_close) / previous_close) * 100
                    if previous_close > 0
                    else 0
                )

                filtered_tickers[i]["percentage"] = pct_change
                self._percentage_cache[symbol] = {
                    "percentage": pct_change,
                    "last_candle": latest_date_key,
                }

        if self._use_candle_any_match and cache_updated:
            self._save_lookback_cache(today)
        return filtered_tickers

    def _load_lookback_cache(self, today: date) -> None:
        if self._lookback_cache_date == today and self._lookback_cache_data:
            return
        filename = self._get_cache_filename()
        aggregate: dict[str, dict[str, Any]] = {}
        loaded_files: list[Path] = []

        pattern = f"lookback_{self._lookback_timeframe}_*.json"
        for path in sorted(self._lookback_cache_dir.glob(pattern)):
            if not path.is_file():
                continue
            try:
                with path.open("r", encoding="utf-8") as fp:
                    raw = json.load(fp)
                if isinstance(raw, dict):
                    normalized = self._normalize_cache_payload(raw)
                    aggregate = self._merge_cache_data(aggregate, normalized)
                    loaded_files.append(path)
            except Exception as exc:
                logger.warning(
                    "PercentChangePairList failed to load lookback cache %s: %s",
                    path,
                    exc,
                )

        self._lookback_cache_data = aggregate
        self._lookback_cache_file = filename
        self._lookback_cache_date = today

        if filename not in loaded_files and aggregate:
            logger.info(
                "PercentChangePairList migrating lookback cache to %s (sources=%d).",
                filename,
                len(loaded_files),
            )
            self._save_lookback_cache(today)

    def _save_lookback_cache(self, today: date) -> None:
        if not self._lookback_cache_file:
            return
        try:
            with self._lookback_cache_file.open("w", encoding="utf-8") as fp:
                json.dump(self._lookback_cache_data, fp, ensure_ascii=False, indent=2)
            logger.info(
                "PercentChangePairList persisted lookback cache with %d entries to %s.",
                len(self._lookback_cache_data),
                self._lookback_cache_file,
            )
        except Exception as exc:
            logger.warning(
                "PercentChangePairList failed to save lookback cache %s: %s",
                self._lookback_cache_file,
                exc,
            )

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
                    logger.info(
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
                    logger.info(
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

    def _calc_candle_percentage(self, candles: DataFrame, idx: int) -> float:
        current_close = float(candles.iloc[idx]["close"])
        pct = None
        if idx > 0:
            prev_close = float(candles.iloc[idx - 1]["close"])
            if prev_close > 0:
                pct = ((current_close - prev_close) / prev_close) * 100
        if pct is None or not math.isfinite(pct):
            open_price = float(candles.iloc[idx]["open"])
            if open_price > 0:
                pct = ((current_close - open_price) / open_price) * 100
            else:
                pct = 0.0
        return pct

    def _normalize_candle_timestamp(self, candle_ts: Any) -> str | None:
        if isinstance(candle_ts, Timestamp):
            ts = candle_ts
        elif isinstance(candle_ts, datetime):
            ts = Timestamp(candle_ts)
        else:
            try:
                ts = to_datetime(candle_ts, utc=True)
            except Exception:
                return None
            if not isinstance(ts, Timestamp):
                return str(candle_ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.isoformat()

    def _percentage_matches(self, pct: float) -> bool:
        if not math.isfinite(pct):
            return False
        if (
            self._min_value is not None
            and self._max_value is not None
            and self._min_value > self._max_value
        ):
            return pct >= self._min_value or pct <= self._max_value
        if self._min_value is not None and pct <= self._min_value:
            return False
        if self._max_value is not None and pct >= self._max_value:
            return False
        return True

    def _is_cache_entry_fresh(
        self, entry: dict[str, Any] | None, refresh_delta: timedelta, now_dt: datetime
    ) -> bool:
        if not entry:
            return False
        cached_at = entry.get("cached_at")
        if not cached_at:
            return False
        try:
            cached_dt = datetime.fromisoformat(cached_at)
        except ValueError:
            return False
        return now_dt - cached_dt <= refresh_delta

    def _get_cache_filename(self) -> Path:
        return (
            self._lookback_cache_dir
            / f"lookback_{self._lookback_timeframe}_{self._lookback_cache_suffix}.json"
        )

    def _normalize_cache_payload(self, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
        data: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            percentages_raw = value.get("percentages") or {}
            percentages: dict[str, float] = {}
            if isinstance(percentages_raw, dict):
                for p_key, p_val in percentages_raw.items():
                    try:
                        percentages[str(p_key)] = float(p_val)
                    except (TypeError, ValueError):
                        continue

            dates_raw = value.get("dates", [])
            dates_list: list[str] = []
            if isinstance(dates_raw, list):
                for item in dates_raw:
                    if isinstance(item, str):
                        dates_list.append(item)
                    else:
                        dates_list.append(str(item))
            all_dates = sorted(set(dates_list) | set(percentages.keys()))
            ordered_percentages = {d: percentages[d] for d in all_dates if d in percentages}

            percentage_val = value.get("percentage")
            try:
                percentage_val = float(percentage_val) if percentage_val is not None else None
            except (TypeError, ValueError):
                percentage_val = None

            last_candle = value.get("last_candle")
            if not isinstance(last_candle, str) and all_dates:
                last_candle = all_dates[-1]

            cached_at = value.get("cached_at")
            if not isinstance(cached_at, str):
                cached_at = None

            normalized = {
                "percentage": percentage_val,
                "last_candle": last_candle,
                "percentages": ordered_percentages,
                "dates": all_dates,
                "cached_at": cached_at,
            }
            data[str(key)] = normalized
        return data

    def _merge_cache_data(
        self, base: dict[str, dict[str, Any]], incoming: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        for symbol, entry in incoming.items():
            existing = base.get(symbol)
            if existing:
                merged_percentages = dict(existing.get("percentages", {}))
                merged_percentages.update(entry.get("percentages", {}))
                merged_dates = sorted(merged_percentages.keys())
                merged_dates, merged_percentages = self._apply_cache_limit(
                    merged_percentages, merged_dates
                )
                existing["percentages"] = merged_percentages
                existing["dates"] = merged_dates
                existing["last_candle"] = merged_dates[-1] if merged_dates else existing.get("last_candle")
                existing["percentage"] = entry.get("percentage", existing.get("percentage"))
                existing["cached_at"] = self._max_cached_at(
                    existing.get("cached_at"), entry.get("cached_at")
                )
            else:
                dates = entry.get("dates", [])
                percentages = entry.get("percentages", {})
                dates, percentages = self._apply_cache_limit(percentages, dates)
                entry["dates"] = dates
                entry["percentages"] = percentages
                if dates and not entry.get("last_candle"):
                    entry["last_candle"] = dates[-1]
                base[symbol] = entry
        return base

    def _apply_cache_limit(
        self, percentages: dict[str, float], dates: list[str]
    ) -> tuple[list[str], dict[str, float]]:
        if not dates:
            dates = list(percentages.keys())
        unique_dates = sorted(dict.fromkeys(dates))
        if self._lookback_cache_max_candles and len(unique_dates) > self._lookback_cache_max_candles:
            unique_dates = unique_dates[-self._lookback_cache_max_candles :]
        limited_percentages = {d: percentages.get(d) for d in unique_dates if d in percentages}
        return unique_dates, limited_percentages

    def _parse_iso_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _max_cached_at(self, first: str | None, second: str | None) -> str | None:
        first_dt = self._parse_iso_datetime(first)
        second_dt = self._parse_iso_datetime(second)
        if first_dt and second_dt:
            return first if first_dt >= second_dt else second
        return first or second

    def _build_percentage_payload(self, candles: DataFrame) -> tuple[list[str], dict[str, float]]:
        total = len(candles.index)
        if total == 0:
            return [], {}
        cache_limit = self._lookback_cache_max_candles or self._lookback_period
        if cache_limit:
            start_idx = max(0, total - cache_limit)
        else:
            start_idx = 0

        dates: list[str] = []
        percentages: dict[str, float] = {}
        for idx in range(start_idx, total):
            iso_ts = self._normalize_candle_timestamp(candles.iloc[idx]["date"])
            if iso_ts is None:
                continue
            pct = self._calc_candle_percentage(candles, idx)
            percentages[iso_ts] = pct
            dates.append(iso_ts)
        return dates, percentages

    def _select_percentage_from_entries(
        self, entries: list[tuple[str, float]]
    ) -> tuple[float | None, list[float]]:
        values = [float(pct) for _, pct in entries if math.isfinite(pct)]
        if not values:
            return None, []
        matches = [pct for pct in values if self._percentage_matches(pct)]
        selected = matches[-1] if matches else None
        return selected, values

    def _evaluate_cached_percentages(
        self, cache_entry: dict[str, Any]
    ) -> tuple[float | None, list[float]]:
        dates = cache_entry.get("dates") or []
        percentages_map = cache_entry.get("percentages") or {}
        if not isinstance(percentages_map, dict):
            percentages_map = {}

        ordered: list[tuple[str, float]] = []
        if dates and isinstance(dates, list):
            for key in dates:
                try:
                    val = float(percentages_map.get(key))
                except (TypeError, ValueError):
                    continue
                ordered.append((key, val))
        else:
            for key, val in sorted(percentages_map.items()):
                try:
                    ordered.append((key, float(val)))
                except (TypeError, ValueError):
                    continue

        if self._lookback_period > 0 and len(ordered) > self._lookback_period:
            ordered = ordered[-self._lookback_period :]

        selected, values = self._select_percentage_from_entries(ordered)
        return selected, values

    def _log_no_candle_match(self, symbol: str, values: list[float]) -> None:
        if values:
            self.log_once(
                f"Removed {symbol} from whitelist, because no candle in the last "
                f"{len(values)} {self._lookback_timeframe} periods matched the configured range. "
                f"Observed range {min(values):.3f}% to {max(values):.3f}%.",
                logger.info,
            )
        else:
            self.log_once(
                f"Removed {symbol} from whitelist, because candle data could not provide a valid percentage.",
                logger.info,
            )

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
