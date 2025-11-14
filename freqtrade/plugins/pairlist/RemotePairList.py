"""
Remote PairList provider

Provides pair list fetched from a remote source
"""

import logging
import glob
from pathlib import Path
from typing import Any

import rapidjson
import requests
from cachetools import TTLCache

from freqtrade import __version__
from freqtrade.configuration.load_config import CONFIG_PARSE_MODE
from freqtrade.exceptions import OperationalException
from freqtrade.exchange.exchange_types import Tickers
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter, SupportsBacktesting
from freqtrade.plugins.pairlist.pairlist_helpers import expand_pairlist


logger = logging.getLogger(__name__)


class RemotePairList(IPairList):
    is_pairlist_generator = True
    # Potential winner bias
    supports_backtesting = SupportsBacktesting.BIASED

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if "number_assets" not in self._pairlistconfig:
            raise OperationalException(
                "`number_assets` not specified. Please check your configuration "
                'for "pairlist.config.number_assets"'
            )

        self._pairlist_urls = self._parse_pairlist_urls()
        self._pairlist_url = self._pairlist_urls[0]

        self._mode = self._pairlistconfig.get("mode", "whitelist")
        self._processing_mode = self._pairlistconfig.get("processing_mode", "filter")
        self._number_pairs = self._pairlistconfig["number_assets"]
        self._refresh_period: int = self._pairlistconfig.get("refresh_period", 1800)
        self._keep_pairlist_on_failure = self._pairlistconfig.get("keep_pairlist_on_failure", True)
        self._pair_cache: TTLCache = TTLCache(maxsize=1, ttl=self._refresh_period)
        self._read_timeout = self._pairlistconfig.get("read_timeout", 60)
        self._bearer_token = self._pairlistconfig.get("bearer_token", "")
        self._init_done = False
        self._save_to_file = self._pairlistconfig.get("save_to_file", None)
        self._last_pairlist: list[Any] = list()

        if self._mode not in ["whitelist", "blacklist"]:
            raise OperationalException(
                '`mode` not configured correctly. Supported Modes are "whitelist","blacklist"'
            )

        if self._processing_mode not in ["filter", "append"]:
            raise OperationalException(
                '`processing_mode` not configured correctly. Supported Modes are "filter","append"'
            )

        if self._pairlist_pos == 0 and self._mode == "blacklist":
            raise OperationalException(
                "A `blacklist` mode RemotePairList can not be on the first "
                "position of your pairlist."
            )

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty Dict is passed
        as tickers argument to filter_pairlist
        """
        return False

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return f"{self.name} - {self._pairlistconfig['number_assets']} pairs from RemotePairlist."

    @staticmethod
    def description() -> str:
        return "Retrieve pairs from a remote API or local file."

    @staticmethod
    def available_parameters() -> dict[str, PairlistParameter]:
        return {
            "pairlist_url": {
                "type": "string",
                "default": "",
                "description": "URL to fetch pairlist from",
                "help": "URL to fetch pairlist from",
            },
            "pairlist_urls": {
                "type": "list",
                "default": [],
                "description": "List of URLs to fetch pairlist data from",
                "help": "Each URL should return pairlist JSON. Remote and local file URLs supported.",
            },
            "number_assets": {
                "type": "number",
                "default": 30,
                "description": "Number of assets",
                "help": "Number of assets to use from the pairlist.",
            },
            "mode": {
                "type": "option",
                "default": "whitelist",
                "options": ["whitelist", "blacklist"],
                "description": "Pairlist mode",
                "help": "Should this pairlist operate as a whitelist or blacklist?",
            },
            "processing_mode": {
                "type": "option",
                "default": "filter",
                "options": ["filter", "append"],
                "description": "Processing mode",
                "help": "Append pairs to incoming pairlist or filter them?",
            },
            **IPairList.refresh_period_parameter(),
            "keep_pairlist_on_failure": {
                "type": "boolean",
                "default": True,
                "description": "Keep last pairlist on failure",
                "help": "Keep last pairlist on failure",
            },
            "read_timeout": {
                "type": "number",
                "default": 60,
                "description": "Read timeout",
                "help": "Request timeout for remote pairlist",
            },
            "bearer_token": {
                "type": "string",
                "default": "",
                "description": "Bearer token",
                "help": "Bearer token - used for auth against the upstream service.",
            },
            "save_to_file": {
                "type": "string",
                "default": "",
                "description": "Filename to save processed pairlist to.",
                "help": "Specify a filename to save the processed pairlist in JSON format.",
            },
        }

    def _parse_pairlist_urls(self) -> list[str]:
        single_url = self._pairlistconfig.get("pairlist_url", "")
        configured_urls = self._pairlistconfig.get("pairlist_urls", [])
        urls: list[str] = []

        if single_url:
            urls.append(single_url)

        if configured_urls:
            if isinstance(configured_urls, str):
                configured_urls = [configured_urls]
            elif not isinstance(configured_urls, list):
                raise OperationalException("`pairlist_urls` must be a list of strings.")

            for url in configured_urls:
                if not isinstance(url, str):
                    raise OperationalException("Each entry in `pairlist_urls` must be a string.")
            urls.extend(configured_urls)

        if not urls:
            raise OperationalException(
                "`pairlist_url` or `pairlist_urls` not specified. Please check your configuration "
                'for "pairlist.config.pairlist_url(s)"'
            )

        return urls

    def process_json(self, jsonparse) -> list[str]:
        pairlist = jsonparse.get("pairs", [])
        remote_refresh_period = int(jsonparse.get("refresh_period", self._refresh_period))

        if self._refresh_period < remote_refresh_period:
            self.log_once(
                f"Refresh Period has been increased from {self._refresh_period}"
                f" to minimum allowed: {remote_refresh_period} from Remote.",
                logger.info,
            )

            self._refresh_period = remote_refresh_period
            self._pair_cache = TTLCache(maxsize=1, ttl=remote_refresh_period)

        self._init_done = True

        return pairlist

    def return_last_pairlist(self) -> list[str]:
        if self._keep_pairlist_on_failure:
            pairlist = self._last_pairlist
            self.log_once("Keeping last fetched pairlist", logger.info)
        else:
            pairlist = []

        return pairlist

    def fetch_pairlist(self, pairlist_url: str | None = None) -> tuple[list[str], float]:
        url = pairlist_url or self._pairlist_urls[0]
        headers = {"User-Agent": "Freqtrade/" + __version__ + " Remotepairlist"}

        if self._bearer_token:
            headers["Authorization"] = f"Bearer {self._bearer_token}"

        try:
            response = requests.get(url, headers=headers, timeout=self._read_timeout)
            content_type = response.headers.get("content-type")
            time_elapsed = response.elapsed.total_seconds()

            if "application/json" in str(content_type):
                jsonparse = response.json()

                try:
                    pairlist = self.process_json(jsonparse)
                except Exception as e:
                    pairlist = self._handle_error(
                        f"Failed processing JSON data from {url}: {type(e)}"
                    )
            else:
                pairlist = self._handle_error(
                    f"RemotePairList is not of type JSON. {url}"
                )

        except requests.exceptions.RequestException:
            pairlist = self._handle_error(f"Was not able to fetch pairlist from: {url}")

            time_elapsed = 0

        return pairlist, time_elapsed

    def _handle_error(self, error: str) -> list[str]:
        if self._init_done:
            self.log_once("Error: " + error, logger.info)
            return self.return_last_pairlist()
        else:
            raise OperationalException(error)

    def gen_pairlist(self, tickers: Tickers) -> list[str]:
        """
        Generate the pairlist
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: List of pairs
        """

        if self._init_done:
            pairlist = self._pair_cache.get("pairlist")
            if pairlist == [None]:
                # Valid but empty pairlist.
                return []
        else:
            pairlist = []

        time_elapsed = 0.0

        if pairlist:
            # Item found - no refresh necessary
            return pairlist.copy()
        else:
            aggregated_pairs: list[str] = []
            for pairlist_url in self._pairlist_urls:
                if pairlist_url.startswith("file:///"):
                    aggregated_pairs.extend(self._load_pairlist_from_file_url(pairlist_url))
                else:
                    fetched_pairs, elapsed = self.fetch_pairlist(pairlist_url)
                    aggregated_pairs.extend(fetched_pairs)
                    time_elapsed += elapsed
            pairlist = aggregated_pairs

        self.log_once(f"Fetched pairs: {pairlist}", logger.debug)

        pairlist = self._exchange.normalize_pairs(pairlist)
        pairlist = self._deduplicate_pairs(pairlist)
        pairlist = expand_pairlist(pairlist, list(self._exchange.get_markets().keys()))
        pairlist = self._whitelist_for_active_markets(pairlist)
        pairlist = self._deduplicate_pairs(pairlist)
        pairlist = pairlist[: self._number_pairs]

        if pairlist:
            self._pair_cache["pairlist"] = pairlist.copy()
        else:
            # If pairlist is empty, set a dummy value to avoid fetching again
            self._pair_cache["pairlist"] = [None]

        if time_elapsed != 0.0:
            self.log_once(f"Pairlist Fetched in {time_elapsed} seconds.", logger.info)
        else:
            self.log_once("Fetched Pairlist.", logger.info)

        self._last_pairlist = list(pairlist)

        if self._save_to_file:
            self.save_pairlist(pairlist, self._save_to_file)

        return pairlist

    def _load_pairlist_from_file_url(self, pairlist_url: str) -> list[str]:
        filename = pairlist_url.split("file:///", 1)[1]
        file_paths = self._expand_file_pattern(filename)

        if not file_paths:
            return self._handle_error(f"{pairlist_url} does not exist.")

        aggregated_pairs: list[str] = []
        for file_path in file_paths:
            if not file_path.exists():
                continue
            with file_path.open() as json_file:
                try:
                    file_content = json_file.read().strip()
                    if not file_content:
                        self.log_once(f"Ignoring empty pairlist file {file_path}", logger.warning)
                        continue
                    jsonparse = rapidjson.loads(file_content, parse_mode=CONFIG_PARSE_MODE)
                    aggregated_pairs.extend(self.process_json(jsonparse))
                except Exception as e:
                    return self._handle_error(
                        f"processing JSON data from {file_path}: {type(e)}"
                    )
        return aggregated_pairs

    def _expand_file_pattern(self, pattern: str) -> list[Path]:
        expanded_pattern = str(Path(pattern).expanduser())
        matches = glob.glob(expanded_pattern)

        if not matches and ".*" in expanded_pattern:
            regex_like_pattern = expanded_pattern.replace(".*", "*")
            matches = glob.glob(regex_like_pattern)

        return [Path(match) for match in matches]

    @staticmethod
    def _deduplicate_pairs(pairs: list[str]) -> list[str]:
        seen = set()
        deduplicated: list[str] = []
        for pair in pairs:
            if pair not in seen:
                seen.add(pair)
                deduplicated.append(pair)
        return deduplicated

    def save_pairlist(self, pairlist: list[str], filename: str) -> None:
        pairlist_data = {"pairs": pairlist}
        try:
            file_path = Path(filename)
            with file_path.open("w") as json_file:
                rapidjson.dump(pairlist_data, json_file)
                logger.info(f"Processed pairlist saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving processed pairlist to {filename}: {e}")

    def filter_pairlist(self, pairlist: list[str], tickers: dict) -> list[str]:
        """
        Filters and sorts pairlist and returns the whitelist again.
        Called on each bot iteration - please use internal caching if necessary
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: new whitelist
        """
        rpl_pairlist = self.gen_pairlist(tickers)
        merged_list = []
        filtered = []

        if self._mode == "whitelist":
            if self._processing_mode == "filter":
                merged_list = [pair for pair in pairlist if pair in rpl_pairlist]
            elif self._processing_mode == "append":
                merged_list = pairlist + rpl_pairlist
            merged_list = sorted(set(merged_list), key=merged_list.index)
        else:
            for pair in pairlist:
                if pair not in rpl_pairlist:
                    merged_list.append(pair)
                else:
                    filtered.append(pair)
            if filtered:
                self.log_once(f"Blacklist - Filtered out pairs: {filtered}", logger.info)

        merged_list = merged_list[: self._number_pairs]
        return merged_list
