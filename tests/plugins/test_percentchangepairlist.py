from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from freqtrade.data.converter import ohlcv_to_dataframe
from freqtrade.enums import CandleType
from freqtrade.exceptions import OperationalException
from freqtrade.plugins.pairlist.PercentChangePairList import PercentChangePairList
from freqtrade.plugins.pairlistmanager import PairListManager
from tests.conftest import (
    EXMS,
    generate_test_data_raw,
    get_patched_exchange,
    get_patched_freqtradebot,
)


@pytest.fixture(scope="function")
def rpl_config(default_conf):
    default_conf["stake_currency"] = "USDT"

    default_conf["exchange"]["pair_whitelist"] = [
        "ETH/USDT",
        "XRP/USDT",
    ]
    default_conf["exchange"]["pair_blacklist"] = ["BLK/USDT"]

    return default_conf


def test_volume_change_pair_list_init_exchange_support(mocker, rpl_config):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
        }
    ]

    with pytest.raises(
        OperationalException,
        match=r"Exchange does not support dynamic whitelist in this configuration. "
        r"Please edit your config and either remove PercentChangePairList, "
        r"or switch to using candles. and restart the bot.",
    ):
        get_patched_freqtradebot(mocker, rpl_config)


def test_volume_change_pair_list_init_wrong_refresh_period(mocker, rpl_config):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 1800,
            "lookback_days": 4,
        }
    ]

    with pytest.raises(
        OperationalException,
        match=r"Refresh period of 1800 seconds is smaller than one "
        r"timeframe of 1d. Please adjust refresh_period "
        r"to at least 86400 and restart the bot.",
    ):
        get_patched_freqtradebot(mocker, rpl_config)


def test_volume_change_pair_list_init_wrong_lookback_period(mocker, rpl_config):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
            "lookback_days": 3,
            "lookback_period": 3,
        }
    ]

    with pytest.raises(
        OperationalException,
        match=r"Ambiguous configuration: lookback_days "
        r"and lookback_period both set in pairlist config. "
        r"Please set lookback_days only or lookback_period "
        r"and lookback_timeframe and restart the bot.",
    ):
        get_patched_freqtradebot(mocker, rpl_config)

    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
            "lookback_days": 1001,
        }
    ]

    with pytest.raises(
        OperationalException,
        match=r"ChangeFilter requires lookback_period to not exceed"
        r" exchange max request size \(\d+\)",
    ):
        get_patched_freqtradebot(mocker, rpl_config)


def test_volume_change_pair_list_init_wrong_config(mocker, rpl_config):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
        }
    ]

    with pytest.raises(
        OperationalException,
        match=r"`number_assets` not specified. Please check your configuration "
        r'for "pairlist.config.number_assets"',
    ):
        get_patched_freqtradebot(mocker, rpl_config)


def test_gen_pairlist_with_valid_change_pair_list_config(mocker, rpl_config, tickers, time_machine):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
            "lookback_days": 4,
        }
    ]
    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            ohlcv_to_dataframe(
                generate_test_data_raw("1d", 100, start.strftime("%Y-%m-%d"), random_seed=12),
                "1d",
                pair="ETH/USDT",
                fill_missing=True,
            )
        ),
        ("BTC/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            ohlcv_to_dataframe(
                generate_test_data_raw("1d", 100, start.strftime("%Y-%m-%d"), random_seed=13),
                "1d",
                pair="BTC/USDT",
                fill_missing=True,
            )
        ),
        ("XRP/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            ohlcv_to_dataframe(
                generate_test_data_raw("1d", 100, start.strftime("%Y-%m-%d"), random_seed=14),
                "1d",
                pair="XRP/USDT",
                fill_missing=True,
            )
        ),
        ("NEO/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            ohlcv_to_dataframe(
                generate_test_data_raw("1d", 100, start.strftime("%Y-%m-%d"), random_seed=15),
                "1d",
                pair="NEO/USDT",
                fill_missing=True,
            )
        ),
        ("TKN/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            # Make sure always have highest percentage
            {
                "timestamp": [
                    "2024-07-01 00:00:00",
                    "2024-07-01 01:00:00",
                    "2024-07-01 02:00:00",
                    "2024-07-01 03:00:00",
                    "2024-07-01 04:00:00",
                    "2024-07-01 05:00:00",
                ],
                "open": [100, 102, 101, 103, 104, 105],
                "high": [102, 103, 102, 104, 105, 106],
                "low": [99, 101, 100, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 106],
                "volume": [1000, 1500, 2000, 2500, 3000, 3500],
            }
        ),
    }

    mocker.patch(f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data))

    exchange = get_patched_exchange(mocker, rpl_config, exchange="binance")
    pairlistmanager = PairListManager(exchange, rpl_config)

    remote_pairlist = PercentChangePairList(
        exchange, pairlistmanager, rpl_config, rpl_config["pairlists"][0], 0
    )

    result = remote_pairlist.gen_pairlist(tickers)

    assert len(result) == 2
    assert result == ["NEO/USDT", "TKN/USDT"]


def test_filter_pairlist_with_empty_ticker(mocker, rpl_config, tickers, time_machine):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "refresh_period": 86400,
            "sort_direction": "asc",
            "lookback_days": 4,
        }
    ]
    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "timestamp": [
                    "2024-07-01 00:00:00",
                    "2024-07-01 01:00:00",
                    "2024-07-01 02:00:00",
                    "2024-07-01 03:00:00",
                    "2024-07-01 04:00:00",
                    "2024-07-01 05:00:00",
                ],
                "open": [100, 102, 101, 103, 104, 105],
                "high": [102, 103, 102, 104, 105, 106],
                "low": [99, 101, 100, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 105],
                "volume": [1000, 1500, 2000, 2500, 3000, 3500],
            }
        ),
        ("XRP/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "timestamp": [
                    "2024-07-01 00:00:00",
                    "2024-07-01 01:00:00",
                    "2024-07-01 02:00:00",
                    "2024-07-01 03:00:00",
                    "2024-07-01 04:00:00",
                    "2024-07-01 05:00:00",
                ],
                "open": [100, 102, 101, 103, 104, 105],
                "high": [102, 103, 102, 104, 105, 106],
                "low": [99, 101, 100, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 104],
                "volume": [1000, 1500, 2000, 2500, 3000, 3400],
            }
        ),
    }

    mocker.patch(f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data))
    exchange = get_patched_exchange(mocker, rpl_config, exchange="binance")
    pairlistmanager = PairListManager(exchange, rpl_config)

    remote_pairlist = PercentChangePairList(
        exchange, pairlistmanager, rpl_config, rpl_config["pairlists"][0], 0
    )

    result = remote_pairlist.filter_pairlist(rpl_config["exchange"]["pair_whitelist"], {})

    assert len(result) == 2
    assert result == ["XRP/USDT", "ETH/USDT"]


def test_filter_pairlist_with_max_value_set(mocker, rpl_config, tickers, time_machine):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
            "max_value": 15,
            "refresh_period": 86400,
            "lookback_days": 4,
        }
    ]

    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "timestamp": [
                    "2024-07-01 00:00:00",
                    "2024-07-01 01:00:00",
                    "2024-07-01 02:00:00",
                    "2024-07-01 03:00:00",
                    "2024-07-01 04:00:00",
                    "2024-07-01 05:00:00",
                ],
                "open": [100, 102, 101, 103, 104, 105],
                "high": [102, 103, 102, 104, 105, 106],
                "low": [99, 101, 100, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 106],
                "volume": [1000, 1500, 2000, 1800, 2400, 2500],
            }
        ),
        ("XRP/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "timestamp": [
                    "2024-07-01 00:00:00",
                    "2024-07-01 01:00:00",
                    "2024-07-01 02:00:00",
                    "2024-07-01 03:00:00",
                    "2024-07-01 04:00:00",
                    "2024-07-01 05:00:00",
                ],
                "open": [100, 102, 101, 103, 104, 105],
                "high": [102, 103, 102, 104, 105, 106],
                "low": [99, 101, 100, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 101],
                "volume": [1000, 1500, 2000, 2500, 3000, 3500],
            }
        ),
    }

    mocker.patch(f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data))
    exchange = get_patched_exchange(mocker, rpl_config, exchange="binance")
    pairlistmanager = PairListManager(exchange, rpl_config)

    remote_pairlist = PercentChangePairList(
        exchange, pairlistmanager, rpl_config, rpl_config["pairlists"][0], 0
    )

    result = remote_pairlist.filter_pairlist(rpl_config["exchange"]["pair_whitelist"], {})

    assert len(result) == 1
    assert result == ["ETH/USDT"]


def test_use_candle_any_match_high_low_option(mocker, rpl_config, tickers, time_machine):
    base_pairlist = {
        "method": "PercentChangePairList",
        "number_assets": 5,
        "sort_key": "percentage",
        "min_value": 30,
        "max_value": -30,
        "refresh_period": 86400,
        "use_candle_any_match": True,
        "lookback_timeframe": "1d",
        "lookback_period": 2,
    }
    config_plain = deepcopy(rpl_config)
    config_plain["pairlists"] = [deepcopy(base_pairlist)]
    config_wick = deepcopy(config_plain)
    config_wick["pairlists"][0]["use_candle_any_match_high_low"] = True

    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    dates = pd.date_range("2024-07-01", periods=2, freq="1D", tz="UTC")
    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "date": dates,
                "open": [100, 100],
                "high": [100, 150],
                "low": [100, 70],
                "close": [100, 101],
                "volume": [1000, 1200],
            }
        ),
        ("XRP/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "date": dates,
                "open": [100, 100],
                "high": [101, 105],
                "low": [99, 98],
                "close": [100, 100],
                "volume": [900, 950],
            }
        ),
    }

    mocker.patch(f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data))

    exchange_plain = get_patched_exchange(mocker, config_plain, exchange="binance")
    pairlistmanager_plain = PairListManager(exchange_plain, config_plain)
    pairlist_plain = PercentChangePairList(
        exchange_plain, pairlistmanager_plain, config_plain, config_plain["pairlists"][0], 0
    )
    result_plain = pairlist_plain.filter_pairlist(
        config_plain["exchange"]["pair_whitelist"], {}
    )
    assert result_plain == []

    exchange_wick = get_patched_exchange(mocker, config_wick, exchange="binance")
    pairlistmanager_wick = PairListManager(exchange_wick, config_wick)
    pairlist_wick = PercentChangePairList(
        exchange_wick, pairlistmanager_wick, config_wick, config_wick["pairlists"][0], 0
    )
    result_wick = pairlist_wick.filter_pairlist(
        config_wick["exchange"]["pair_whitelist"], {}
    )
    assert result_wick == ["ETH/USDT"]


def test_high_low_cache_normalization_drops_plain_percentages(
    mocker, rpl_config, tickers, time_machine
):
    config = deepcopy(rpl_config)
    config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 5,
            "sort_key": "percentage",
            "min_value": 30,
            "max_value": -30,
            "refresh_period": 86400,
            "use_candle_any_match": True,
            "use_candle_any_match_high_low": True,
            "lookback_timeframe": "1d",
            "lookback_period": 2,
        }
    ]
    exchange = get_patched_exchange(mocker, config, exchange="binance")
    pairlistmanager = PairListManager(exchange, config)
    pairlist = PercentChangePairList(
        exchange, pairlistmanager, config, config["pairlists"][0], 0
    )

    raw = {
        "ETH/USDT": {
            "percentage": None,
            "percentages": {
                "2024-07-01T00:00:00+00:00": 5.0,
                "2024-07-02T00:00:00+00:00": {"high": 25.0, "low": -10.0},
            },
            "dates": [
                "2024-07-01T00:00:00+00:00",
                "2024-07-02T00:00:00+00:00",
            ],
            "last_candle": "2024-07-02T00:00:00+00:00",
            "cached_at": "2024-07-03T00:00:00+00:00",
        }
    }

    normalized = pairlist._normalize_cache_payload(raw)
    entry = normalized["ETH/USDT"]
    assert "2024-07-01T00:00:00+00:00" not in entry["percentages"]
    assert "2024-07-02T00:00:00+00:00" in entry["percentages"]


def test_use_candle_any_match_high_low_refreshes_legacy_cache(
    mocker, rpl_config, tickers, time_machine
):
    rpl_config = deepcopy(rpl_config)
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 5,
            "sort_key": "percentage",
            "min_value": 30,
            "max_value": -30,
            "refresh_period": 86400,
            "use_candle_any_match": True,
            "use_candle_any_match_high_low": True,
            "lookback_timeframe": "1d",
            "lookback_period": 2,
        }
    ]

    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    dates = pd.date_range("2024-07-01", periods=2, freq="1D", tz="UTC")
    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "date": dates,
                "open": [100, 100],
                "high": [100, 150],
                "low": [100, 70],
                "close": [100, 101],
                "volume": [1000, 1200],
            }
        )
    }

    refresh_mock = mocker.patch(
        f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data)
    )

    exchange = get_patched_exchange(mocker, rpl_config, exchange="binance")
    pairlistmanager = PairListManager(exchange, rpl_config)
    pairlist = PercentChangePairList(
        exchange, pairlistmanager, rpl_config, rpl_config["pairlists"][0], 0
    )

    pairlist._load_lookback_cache = MagicMock()
    pairlist._save_lookback_cache = MagicMock()
    legacy_date = datetime(2024, 7, 31, tzinfo=UTC).isoformat()
    pairlist._lookback_cache_data = {
        "ETH/USDT": {
            "percentage": None,
            "percentages": {
                legacy_date: 5.0,
            },
            "dates": [legacy_date],
            "last_candle": legacy_date,
            "cached_at": start.isoformat(),
        }
    }

    result = pairlist.filter_pairlist(rpl_config["exchange"]["pair_whitelist"], {})

    assert refresh_mock.call_count > 0
    assert result == ["ETH/USDT"]
    payload = pairlist._lookback_cache_data["ETH/USDT"]
    assert payload["dates"]
    assert isinstance(payload["percentages"][payload["dates"][-1]], dict)


def test_use_candle_any_match_high_low_uses_intraday_reference(
    mocker, rpl_config, tickers, time_machine
):
    config = deepcopy(rpl_config)
    config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 5,
            "sort_key": "percentage",
            "min_value": 25,
            "max_value": -25,
            "refresh_period": 86400,
            "use_candle_any_match": True,
            "use_candle_any_match_high_low": True,
            "lookback_timeframe": "1d",
            "lookback_period": 2,
        }
    ]

    start = datetime(2024, 8, 1, 0, 0, 0, 0, tzinfo=UTC)
    time_machine.move_to(start, tick=False)

    dates = pd.date_range("2024-07-01", periods=2, freq="1D", tz="UTC")
    mock_ohlcv_data = {
        ("ETH/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "date": dates,
                "open": [130, 100],
                "high": [131, 101],
                "low": [129, 90],
                "close": [130, 100],
                "volume": [1000, 1200],
            }
        ),
        ("XRP/USDT", "1d", CandleType.SPOT): pd.DataFrame(
            {
                "date": dates,
                "open": [100, 100],
                "high": [101, 101],
                "low": [99, 99],
                "close": [100, 100],
                "volume": [800, 750],
            }
        ),
    }

    mocker.patch(f"{EXMS}.refresh_latest_ohlcv", MagicMock(return_value=mock_ohlcv_data))

    exchange = get_patched_exchange(mocker, config, exchange="binance")
    pairlistmanager = PairListManager(exchange, config)
    pairlist = PercentChangePairList(
        exchange, pairlistmanager, config, config["pairlists"][0], 0
    )

    result = pairlist.filter_pairlist(config["exchange"]["pair_whitelist"], {})

    assert result == []
def test_gen_pairlist_from_tickers(mocker, rpl_config, tickers):
    rpl_config["pairlists"] = [
        {
            "method": "PercentChangePairList",
            "number_assets": 2,
            "sort_key": "percentage",
            "min_value": 0,
        }
    ]

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    exchange = get_patched_exchange(mocker, rpl_config, exchange="binance")
    pairlistmanager = PairListManager(exchange, rpl_config)

    remote_pairlist = pairlistmanager._pairlist_handlers[0]

    # The generator returns BTC ETH and TKN - filtering the first ensures removing pairs
    # in this step ain't problematic.
    def _validate_pair(pair, ticker):
        if pair == "BTC/USDT":
            return False
        return True

    remote_pairlist._validate_pair = _validate_pair

    result = remote_pairlist.gen_pairlist(tickers.return_value)

    assert len(result) == 1
    assert result == ["ETH/USDT"]
