"""
MCP Tool: get_technical_analysis

Composite technical analysis for a single stock: trend direction,
momentum signals, volatility, and support/resistance levels.

Usage (from Claude):
    "Technical analysis for AAPL"
    "Is TSLA overbought?"
    "What's the trend for MSFT?"
    "Show me MACD and RSI for NVDA"

Architecture note:
- Standalone ticker tool (no portfolio loading required)
- Parallel-fetches multiple FMP technical indicator endpoints
- Computes derived indicators (MACD, Bollinger Bands) from raw data
- Interprets signals into actionable summary
"""

import logging
import re
import sys
import time
from datetime import datetime
from typing import Literal, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from ..client import FMPClient
from ._file_output import FILE_OUTPUT_DIR, write_csv


logger = logging.getLogger(__name__)

# Default period configurations
DEFAULT_PERIODS = {
    "sma_periods": [20, 50, 200],
    "ema_periods": [12, 26],
    "rsi_period": 14,
    "adx_period": 14,
    "williams_period": 14,
    "stddev_period": 20,
}

# Valid indicator names for the `indicators` parameter
VALID_INDICATORS = {"sma", "ema", "rsi", "adx", "williams", "macd", "bollinger"}


def _supports_cached_fetch(fmp: object) -> bool:
    return not type(fmp).__module__.startswith("unittest.mock") and callable(
        getattr(type(fmp), "fetch", None)
    )


def get_technical_analysis(
    symbol: str,
    timeframe: Literal["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"] = "1day",
    indicators: Optional[list[str]] = None,
    period_overrides: Optional[dict] = None,
    format: Literal["full", "summary"] = "summary",
    output: Literal["inline", "file"] = "inline",
    last_n: Optional[int] = None,
    use_cache: bool = True,
) -> dict:
    """
    Get composite technical analysis for a single stock.

    Args:
        symbol: Stock ticker (e.g., "AAPL", "MSFT").
        timeframe: Candle timeframe (default "1day").
        indicators: Subset of indicators to include. Options: "sma", "ema",
            "rsi", "adx", "williams", "macd", "bollinger". Default: all.
        period_overrides: Override default period lengths. Keys: sma_periods,
            ema_periods, rsi_period, adx_period, williams_period, stddev_period.
        format: "summary" (signals + current values) or "full" (+ time series).
        output: "inline" (default) or "file". Applies to format="full" only.
        last_n: Keep only the most recent N points per indicator in full mode.
        use_cache: Use cached FMP data (default True).

    Returns:
        dict with status field ("success" or "error")
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        # 1. Validate indicators
        requested = set(indicators) if indicators else set(VALID_INDICATORS)
        invalid = requested - VALID_INDICATORS
        if invalid:
            return {
                "status": "error",
                "error": f"Invalid indicators: {sorted(invalid)}. "
                         f"Valid options: {sorted(VALID_INDICATORS)}"
                }

        if last_n is not None:
            try:
                last_n = int(last_n)
            except (TypeError, ValueError):
                return {
                    "status": "error",
                    "error": "last_n must be a positive integer.",
                }
            if last_n <= 0:
                return {
                    "status": "error",
                    "error": "last_n must be a positive integer.",
                }

        # 2. Merge period config
        periods = {**DEFAULT_PERIODS}
        if period_overrides:
            periods.update(period_overrides)

        # 3. Build fetch list
        fetches = _build_fetch_list(requested, periods)

        # 4. Parallel fetch
        start_time = time.time()
        results, errors = _parallel_fetch(fetches, symbol, timeframe, use_cache)
        fetch_time_ms = int((time.time() - start_time) * 1000)

        # 5. Check for total failure
        if not results:
            return {
                "status": "error",
                "error": f"All indicator fetches failed for '{symbol}'. "
                         f"Errors: {errors}"
            }

        # 6. Compute derived indicators
        derived = _compute_derived(results, requested, periods)

        # 7. Build signal interpretation
        signals = _interpret_signals(results, derived, periods)

        # 8. Determine which high-level indicators we actually have
        included, failed = _categorize_results(results, errors, requested)

        # 9. Format response
        response = {
            "status": "success",
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            **signals,
            "indicators_included": sorted(included),
            "indicators_failed": sorted(failed),
        }

        if format == "full":
            time_series = _build_time_series(results, derived, periods, last_n=last_n)
            response["time_series"] = time_series
            response["fetch_time_ms"] = fetch_time_ms
            if output == "file":
                symbol_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", symbol.upper()).strip("_") or "symbol"
                timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
                file_path = FILE_OUTPUT_DIR / f"technical_{symbol_slug}_{timestamp}.csv"
                write_csv(_flatten_time_series_rows(time_series), file_path)
                response.pop("time_series", None)
                response["output"] = "file"
                response["file_path"] = str(file_path)
                response["hint"] = "Use Read tool with file_path, or Grep to search indicators."
            else:
                response["output"] = "inline"

        return response

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved


# === Internal helpers ===


def _build_fetch_list(requested: set[str], periods: dict) -> list[tuple[str, int]]:
    """Build list of (endpoint_name, periodLength) tuples for FMP fetches."""
    fetches = []

    sma_periods = periods.get("sma_periods", DEFAULT_PERIODS["sma_periods"])
    ema_periods = periods.get("ema_periods", DEFAULT_PERIODS["ema_periods"])
    rsi_period = periods.get("rsi_period", DEFAULT_PERIODS["rsi_period"])
    adx_period = periods.get("adx_period", DEFAULT_PERIODS["adx_period"])
    williams_period = periods.get("williams_period", DEFAULT_PERIODS["williams_period"])
    stddev_period = periods.get("stddev_period", DEFAULT_PERIODS["stddev_period"])

    # SMA fetches: needed for sma indicator and bollinger (needs SMA matching stddev_period)
    sma_periods_needed = set()
    if "sma" in requested:
        sma_periods_needed.update(sma_periods)
    if "bollinger" in requested:
        sma_periods_needed.add(stddev_period)  # Bollinger needs SMA(stddev_period)

    for period in sorted(sma_periods_needed):
        fetches.append(("ta_sma", period))

    # EMA fetches: needed for ema indicator and macd
    ema_periods_needed = set()
    if "ema" in requested:
        ema_periods_needed.update(ema_periods)
    if "macd" in requested:
        ema_periods_needed.update(ema_periods)  # MACD needs EMA(12) and EMA(26)

    for period in sorted(ema_periods_needed):
        fetches.append(("ta_ema", period))

    # RSI
    if "rsi" in requested:
        fetches.append(("ta_rsi", rsi_period))

    # ADX
    if "adx" in requested:
        fetches.append(("ta_adx", adx_period))

    # Williams %R
    if "williams" in requested:
        fetches.append(("ta_williams", williams_period))

    # StdDev: needed for bollinger
    if "bollinger" in requested:
        fetches.append(("ta_stddev", stddev_period))

    return fetches


def _parallel_fetch(
    fetches: list[tuple[str, int]],
    symbol: str,
    timeframe: str,
    use_cache: bool,
) -> tuple[dict[str, list], dict[str, str]]:
    """Fetch all indicators in parallel via ThreadPoolExecutor.

    Returns (results, errors) where results maps "endpoint_period" -> list of dicts
    and errors maps "endpoint_period" -> error message.
    """
    fmp = FMPClient()
    results = {}
    errors = {}

    def _fetch_one(endpoint_name: str, period: int) -> tuple[str, int, list]:
        """Fetch one indicator. Returns (endpoint_name, period, data_list)."""
        if _supports_cached_fetch(fmp):
            payload = fmp.fetch(
                endpoint_name,
                symbol=symbol,
                periodLength=period,
                timeframe=timeframe,
                use_cache=use_cache,
            )
            data = payload.to_dict("records") if hasattr(payload, "to_dict") else payload
        else:
            data = fmp.fetch_raw(
                endpoint_name,
                symbol=symbol,
                periodLength=period,
                timeframe=timeframe,
            )
        if isinstance(data, dict):
            data = [data]
        if not data:
            raise ValueError(f"Empty response for {endpoint_name} period={period}")
        return (endpoint_name, period, data)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_fetch_one, ep, period): (ep, period)
            for ep, period in fetches
        }
        for future in as_completed(futures):
            ep, period = futures[future]
            key = f"{ep}_{period}"
            try:
                _, _, data = future.result()
                results[key] = data
            except Exception as e:
                errors[key] = str(e)
                logger.warning("Failed to fetch %s: %s", key, e)

    return results, errors


def _compute_derived(
    results: dict[str, list],
    requested: set[str],
    periods: dict,
) -> dict:
    """Compute MACD and Bollinger Bands from raw indicator data."""
    derived = {}
    ema_periods = periods.get("ema_periods", DEFAULT_PERIODS["ema_periods"])
    stddev_period = periods.get("stddev_period", DEFAULT_PERIODS["stddev_period"])

    # MACD: derived from EMA(12) and EMA(26)
    if "macd" in requested:
        ema_short = ema_periods[0] if len(ema_periods) >= 1 else 12
        ema_long = ema_periods[1] if len(ema_periods) >= 2 else 26
        ema_short_key = f"ta_ema_{ema_short}"
        ema_long_key = f"ta_ema_{ema_long}"

        if ema_short_key in results and ema_long_key in results:
            try:
                df_short = pd.DataFrame(results[ema_short_key])
                df_long = pd.DataFrame(results[ema_long_key])

                # Align on date
                df_short = df_short.set_index("date").sort_index()
                df_long = df_long.set_index("date").sort_index()

                # MACD line = EMA(short) - EMA(long)
                macd_line = df_short["ema"] - df_long["ema"]
                macd_line = macd_line.dropna()

                # Signal line = 9-period EMA of MACD line
                signal_line = macd_line.ewm(span=9, adjust=False).mean()

                # Histogram = MACD - Signal
                histogram = macd_line - signal_line

                derived["macd"] = {
                    "macd_line": macd_line,
                    "signal_line": signal_line,
                    "histogram": histogram,
                }
            except Exception as e:
                logger.warning("Failed to compute MACD: %s", e)

    # Bollinger Bands: derived from SMA(stddev_period) and StdDev(stddev_period)
    if "bollinger" in requested:
        sma_key = f"ta_sma_{stddev_period}"
        stddev_key = f"ta_stddev_{stddev_period}"

        if sma_key in results and stddev_key in results:
            try:
                df_sma = pd.DataFrame(results[sma_key])
                df_std = pd.DataFrame(results[stddev_key])

                df_sma = df_sma.set_index("date").sort_index()
                df_std = df_std.set_index("date").sort_index()

                sma_series = df_sma["sma"]
                std_series = df_std["standardDeviation"]
                close_series = df_sma["close"]

                upper_band = sma_series + 2 * std_series
                lower_band = sma_series - 2 * std_series

                band_diff = upper_band - lower_band
                pct_b = (close_series - lower_band) / band_diff.replace(0, float("nan"))
                bandwidth = band_diff / sma_series.replace(0, float("nan"))

                derived["bollinger"] = {
                    "upper": upper_band,
                    "lower": lower_band,
                    "middle": sma_series,
                    "close": close_series,
                    "pct_b": pct_b,
                    "bandwidth": bandwidth,
                }
            except Exception as e:
                logger.warning("Failed to compute Bollinger Bands: %s", e)

    return derived


def _get_latest_value(results: dict[str, list], key: str, field: str) -> Optional[float]:
    """Extract the most recent value from a results list for a given key and field."""
    if key not in results:
        return None
    data = results[key]
    if not data:
        return None
    # Data is typically sorted newest-first from FMP
    latest = data[0]
    val = latest.get(field)
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    return None


def _get_latest_close(results: dict[str, list]) -> Optional[float]:
    """Get the latest close price from any available indicator result."""
    # Try SMA results first (most common to have)
    for key, data in results.items():
        if data and "close" in data[0]:
            try:
                return float(data[0]["close"])
            except (ValueError, TypeError, KeyError):
                continue
    return None


def _get_latest_date(results: dict[str, list]) -> Optional[str]:
    """Get the date of the most recent data point."""
    for key, data in results.items():
        if data and "date" in data[0]:
            return str(data[0]["date"])
    return None


def _interpret_signals(
    results: dict[str, list],
    derived: dict,
    periods: dict,
) -> dict:
    """Extract latest values and build signal interpretation dict."""
    sma_periods = periods.get("sma_periods", DEFAULT_PERIODS["sma_periods"])
    ema_periods = periods.get("ema_periods", DEFAULT_PERIODS["ema_periods"])
    rsi_period = periods.get("rsi_period", DEFAULT_PERIODS["rsi_period"])
    adx_period = periods.get("adx_period", DEFAULT_PERIODS["adx_period"])
    williams_period = periods.get("williams_period", DEFAULT_PERIODS["williams_period"])
    stddev_period = periods.get("stddev_period", DEFAULT_PERIODS["stddev_period"])

    latest_close = _get_latest_close(results)
    as_of = _get_latest_date(results)

    response = {}
    if as_of:
        response["as_of"] = as_of
    if latest_close is not None:
        response["price"] = round(latest_close, 2)

    # --- Moving Averages ---
    ma_values = {}
    for period in sma_periods:
        key = f"ta_sma_{period}"
        val = _get_latest_value(results, key, "sma")
        if val is not None:
            ma_values[f"sma_{period}"] = round(val, 2)

    for period in ema_periods:
        key = f"ta_ema_{period}"
        val = _get_latest_value(results, key, "ema")
        if val is not None:
            ma_values[f"ema_{period}"] = round(val, 2)

    if ma_values:
        response["moving_averages"] = ma_values

    # --- Trend ---
    trend = {}
    if latest_close is not None:
        # Use the longest SMA for primary trend
        longest_sma_period = max(sma_periods)
        longest_sma_val = ma_values.get(f"sma_{longest_sma_period}")
        if longest_sma_val is not None:
            trend["primary"] = "bullish" if latest_close > longest_sma_val else "bearish"

        # Trend alignment: check if SMAs are in order
        sma_vals = []
        for p in sorted(sma_periods):
            v = ma_values.get(f"sma_{p}")
            if v is not None:
                sma_vals.append(v)

        if len(sma_vals) >= 2:
            all_ascending = all(sma_vals[i] >= sma_vals[i + 1] for i in range(len(sma_vals) - 1))
            all_descending = all(sma_vals[i] <= sma_vals[i + 1] for i in range(len(sma_vals) - 1))

            if all_ascending:
                trend["alignment"] = "strongly_bullish"
            elif all_descending:
                trend["alignment"] = "strongly_bearish"
            else:
                trend["alignment"] = "mixed"

        # Golden/Death cross (50 vs 200 or two longest SMAs)
        if len(sma_periods) >= 2:
            sorted_periods = sorted(sma_periods)
            mid_sma = ma_values.get(f"sma_{sorted_periods[-2]}")
            long_sma = ma_values.get(f"sma_{sorted_periods[-1]}")
            if mid_sma is not None and long_sma is not None:
                trend["ma_cross"] = "golden_cross" if mid_sma > long_sma else "death_cross"

    # ADX trend strength
    adx_val = _get_latest_value(results, f"ta_adx_{adx_period}", "adx")
    if adx_val is not None:
        if adx_val >= 50:
            adx_strength = "very_strong"
        elif adx_val >= 25:
            adx_strength = "strong"
        elif adx_val >= 20:
            adx_strength = "developing"
        else:
            adx_strength = "weak"
        trend["adx_strength"] = adx_strength
        trend["adx_value"] = round(adx_val, 1)

    if trend:
        response["trend"] = trend

    # --- Momentum ---
    momentum = {}

    # RSI
    rsi_val = _get_latest_value(results, f"ta_rsi_{rsi_period}", "rsi")
    if rsi_val is not None:
        if rsi_val >= 70:
            rsi_signal = "overbought"
        elif rsi_val >= 60:
            rsi_signal = "bullish"
        elif rsi_val <= 30:
            rsi_signal = "oversold"
        elif rsi_val <= 40:
            rsi_signal = "bearish"
        else:
            rsi_signal = "neutral"
        momentum["rsi"] = {"value": round(rsi_val, 1), "signal": rsi_signal}

    # Williams %R
    williams_val = _get_latest_value(results, f"ta_williams_{williams_period}", "williams")
    if williams_val is not None:
        if williams_val >= -20:
            williams_signal = "overbought"
        elif williams_val <= -80:
            williams_signal = "oversold"
        else:
            williams_signal = "neutral"
        momentum["williams"] = {"value": round(williams_val, 1), "signal": williams_signal}

    # MACD
    if "macd" in derived:
        macd_data = derived["macd"]
        macd_line = macd_data["macd_line"]
        signal_line = macd_data["signal_line"]
        histogram = macd_data["histogram"]

        if len(macd_line) > 0:
            macd_val = float(macd_line.iloc[-1])
            signal_val = float(signal_line.iloc[-1])
            hist_val = float(histogram.iloc[-1])

            if macd_val > signal_val and hist_val > 0:
                macd_signal = "bullish"
            elif macd_val < signal_val and hist_val < 0:
                macd_signal = "bearish"
            else:
                macd_signal = "neutral"

            # Momentum direction from histogram
            macd_momentum = "increasing"
            if len(histogram) >= 2:
                prev_hist = float(histogram.iloc[-2])
                macd_momentum = "increasing" if hist_val > prev_hist else "decreasing"

            momentum["macd"] = {
                "signal": macd_signal,
                "momentum": macd_momentum,
                "macd_line": round(macd_val, 4),
                "signal_line": round(signal_val, 4),
                "histogram": round(hist_val, 4),
            }

    if momentum:
        response["momentum"] = momentum

    # --- Volatility (Bollinger Bands) ---
    volatility = {}
    if "bollinger" in derived:
        boll = derived["bollinger"]
        upper = boll["upper"]
        lower = boll["lower"]
        pct_b = boll["pct_b"]
        bw = boll["bandwidth"]

        if len(upper) > 0:
            upper_val = float(upper.iloc[-1])
            lower_val = float(lower.iloc[-1])
            pct_b_val = float(pct_b.iloc[-1]) if pd.notna(pct_b.iloc[-1]) else 0.5
            bw_val = float(bw.iloc[-1]) if pd.notna(bw.iloc[-1]) else 0.0

            if pct_b_val > 1.0:
                boll_signal = "above_upper_band"
            elif pct_b_val < 0.0:
                boll_signal = "below_lower_band"
            elif pct_b_val > 0.8:
                boll_signal = "near_upper_band"
            elif pct_b_val < 0.2:
                boll_signal = "near_lower_band"
            else:
                boll_signal = "mid_band"

            volatility["bollinger"] = {
                "signal": boll_signal,
                "pct_b": round(pct_b_val, 2),
                "bandwidth": round(bw_val, 4),
                "squeeze": bw_val < 0.04,
                "upper": round(upper_val, 2),
                "lower": round(lower_val, 2),
            }

    # StdDev raw value
    stddev_val = _get_latest_value(results, f"ta_stddev_{stddev_period}", "standardDeviation")
    if stddev_val is not None:
        volatility["std_dev"] = round(stddev_val, 2)

    if volatility:
        response["volatility"] = volatility

    # --- Support/Resistance ---
    if latest_close is not None:
        support = []
        resistance = []

        # Collect all MA levels + bollinger bands
        levels = []
        for name, val in ma_values.items():
            levels.append((name, val))

        if "bollinger" in derived and len(derived["bollinger"]["lower"]) > 0:
            levels.append(("bollinger_lower", round(float(derived["bollinger"]["lower"].iloc[-1]), 2)))
        if "bollinger" in derived and len(derived["bollinger"]["upper"]) > 0:
            levels.append(("bollinger_upper", round(float(derived["bollinger"]["upper"].iloc[-1]), 2)))

        for name, val in levels:
            if val < latest_close:
                support.append({"level": name, "price": val})
            elif val > latest_close:
                resistance.append({"level": name, "price": val})

        # Sort support descending (nearest first), resistance ascending
        support.sort(key=lambda x: x["price"], reverse=True)
        resistance.sort(key=lambda x: x["price"])

        if support or resistance:
            response["support_resistance"] = {
                "support": support,
                "resistance": resistance,
            }

    # --- Composite Signal ---
    score = 0
    votes = 0

    # Primary trend vote
    if "trend" in response and "primary" in response["trend"]:
        score += 1 if response["trend"]["primary"] == "bullish" else -1
        votes += 1

    # RSI vote
    if "momentum" in response and "rsi" in response["momentum"]:
        rsi_sig = response["momentum"]["rsi"]["signal"]
        if rsi_sig in ("bullish", "oversold"):
            score += 1
        elif rsi_sig in ("bearish", "overbought"):
            score -= 1
        votes += 1

    # MACD vote
    if "momentum" in response and "macd" in response["momentum"]:
        macd_sig = response["momentum"]["macd"]["signal"]
        if macd_sig == "bullish":
            score += 1
        elif macd_sig == "bearish":
            score -= 1
        votes += 1

    # Williams vote
    if "momentum" in response and "williams" in response["momentum"]:
        will_sig = response["momentum"]["williams"]["signal"]
        if will_sig == "oversold":
            score += 1
        elif will_sig == "overbought":
            score -= 1
        votes += 1

    if votes > 0:
        if score >= 3:
            composite = "strong_buy"
        elif score >= 1:
            composite = "buy"
        elif score <= -3:
            composite = "strong_sell"
        elif score <= -1:
            composite = "sell"
        else:
            composite = "neutral"

        response["composite_signal"] = composite
        response["signal_score"] = score

    return response


def _categorize_results(
    results: dict[str, list],
    errors: dict[str, str],
    requested: set[str],
) -> tuple[set[str], set[str]]:
    """Determine which high-level indicators succeeded/failed.

    Returns (included, failed) sets of indicator names.
    """
    included = set()
    failed = set()

    # Map endpoint prefixes to high-level indicator names
    for key in results:
        if key.startswith("ta_sma"):
            included.add("sma")
        elif key.startswith("ta_ema"):
            included.add("ema")
        elif key.startswith("ta_rsi"):
            included.add("rsi")
        elif key.startswith("ta_adx"):
            included.add("adx")
        elif key.startswith("ta_williams"):
            included.add("williams")
        elif key.startswith("ta_stddev"):
            included.add("bollinger")

    for key in errors:
        if key.startswith("ta_sma"):
            failed.add("sma")
        elif key.startswith("ta_ema"):
            failed.add("ema")
            failed.add("macd")  # MACD depends on EMA
        elif key.startswith("ta_rsi"):
            failed.add("rsi")
        elif key.startswith("ta_adx"):
            failed.add("adx")
        elif key.startswith("ta_williams"):
            failed.add("williams")
        elif key.startswith("ta_stddev"):
            failed.add("bollinger")

    # MACD/bollinger are derived -- check if they were actually computed
    if "ema" in included and "macd" in requested:
        # Check if we have both EMA results needed for MACD
        ema_periods = DEFAULT_PERIODS["ema_periods"]
        has_both = all(f"ta_ema_{p}" in results for p in ema_periods)
        if has_both:
            included.add("macd")
        else:
            failed.add("macd")

    if "sma" in included and "bollinger" in requested:
        stddev_period = DEFAULT_PERIODS["stddev_period"]
        if f"ta_sma_{stddev_period}" in results and f"ta_stddev_{stddev_period}" in results:
            included.add("bollinger")

    # Only report indicators that were requested
    included = included & requested
    failed = failed & requested
    # Don't double-report: if included, remove from failed
    failed = failed - included

    return included, failed


def _build_time_series(
    results: dict[str, list],
    derived: dict,
    periods: dict,
    last_n: Optional[int] = None,
) -> dict:
    """Build time_series dict for full format output."""
    time_series = {}

    sma_periods = periods.get("sma_periods", DEFAULT_PERIODS["sma_periods"])
    ema_periods = periods.get("ema_periods", DEFAULT_PERIODS["ema_periods"])
    rsi_period = periods.get("rsi_period", DEFAULT_PERIODS["rsi_period"])
    adx_period = periods.get("adx_period", DEFAULT_PERIODS["adx_period"])
    williams_period = periods.get("williams_period", DEFAULT_PERIODS["williams_period"])

    # Raw indicator time series
    for period in sma_periods:
        key = f"ta_sma_{period}"
        if key in results:
            time_series[f"sma_{period}"] = _slice_recent_records(results[key], last_n)

    for period in ema_periods:
        key = f"ta_ema_{period}"
        if key in results:
            time_series[f"ema_{period}"] = _slice_recent_records(results[key], last_n)

    rsi_key = f"ta_rsi_{rsi_period}"
    if rsi_key in results:
        time_series[f"rsi_{rsi_period}"] = _slice_recent_records(results[rsi_key], last_n)

    adx_key = f"ta_adx_{adx_period}"
    if adx_key in results:
        time_series[f"adx_{adx_period}"] = _slice_recent_records(results[adx_key], last_n)

    williams_key = f"ta_williams_{williams_period}"
    if williams_key in results:
        time_series[f"williams_{williams_period}"] = _slice_recent_records(results[williams_key], last_n)

    # Derived: MACD
    if "macd" in derived:
        macd_data = derived["macd"]
        macd_records = []
        macd_line = macd_data["macd_line"]
        signal_line = macd_data["signal_line"]
        histogram = macd_data["histogram"]
        for date in macd_line.index:
            if pd.notna(macd_line[date]):
                macd_records.append({
                    "date": str(date),
                    "macd_line": round(float(macd_line[date]), 4),
                    "signal_line": round(float(signal_line[date]), 4),
                    "histogram": round(float(histogram[date]), 4),
                })
        time_series["macd"] = _slice_recent_records(macd_records, last_n)

    # Derived: Bollinger
    if "bollinger" in derived:
        boll = derived["bollinger"]
        boll_records = []
        for date in boll["upper"].index:
            rec = {"date": str(date)}
            if pd.notna(boll["close"].get(date)):
                rec["close"] = round(float(boll["close"][date]), 2)
            if pd.notna(boll["upper"].get(date)):
                rec["upper"] = round(float(boll["upper"][date]), 2)
            if pd.notna(boll["middle"].get(date)):
                rec["sma"] = round(float(boll["middle"][date]), 2)
            if pd.notna(boll["lower"].get(date)):
                rec["lower"] = round(float(boll["lower"][date]), 2)
            if pd.notna(boll["pct_b"].get(date)):
                rec["pct_b"] = round(float(boll["pct_b"][date]), 2)
            if pd.notna(boll["bandwidth"].get(date)):
                rec["bandwidth"] = round(float(boll["bandwidth"][date]), 4)
            boll_records.append(rec)
        time_series["bollinger"] = _slice_recent_records(boll_records, last_n)

    return time_series


def _slice_recent_records(records: list[dict], last_n: Optional[int]) -> list[dict]:
    """Slice to the most recent N records while preserving existing order."""
    if last_n is None or len(records) <= last_n:
        return records

    first_date = records[0].get("date")
    last_date = records[-1].get("date")
    if isinstance(first_date, str) and isinstance(last_date, str):
        # Descending (newest-first): keep head, ascending: keep tail.
        return records[:last_n] if first_date >= last_date else records[-last_n:]

    return records[:last_n]


def _flatten_time_series_rows(time_series: dict[str, list[dict]]) -> list[dict]:
    """Flatten indicator->records mapping into CSV-friendly rows."""
    rows: list[dict] = []
    for indicator, series in time_series.items():
        for record in series:
            row = {"indicator": indicator}
            row.update(record)
            rows.append(row)
    return rows
