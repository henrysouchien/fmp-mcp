from __future__ import annotations

from fmp.tools import estimates


def _install_fake_summary(monkeypatch, rows: list[dict]) -> None:
    monkeypatch.setattr(estimates, "_ESTIMATE_API_URL", "https://example.test")
    monkeypatch.setattr(estimates, "_api_get", lambda _path, _params: rows)


def test_screen_filters_immaterial_revisions_by_default(monkeypatch) -> None:
    _install_fake_summary(
        monkeypatch,
        [
            {
                "ticker": "MSFT",
                "eps_delta": 0.00318,
                "baseline_eps_avg": 4.26528,
                "revenue_delta": -10_838_939.0,
                "baseline_revenue_avg": 87_570_806_507.0,
                "direction": "up",
            }
        ],
    )

    result = estimates.screen_estimate_revisions(tickers=["MSFT"], direction="all")

    assert result["status"] == "success"
    assert result["result_count"] == 0
    assert result["results"] == []


def test_screen_can_return_immaterial_rows_for_diagnostics(monkeypatch) -> None:
    _install_fake_summary(
        monkeypatch,
        [
            {
                "ticker": "MSFT",
                "eps_delta": 0.00318,
                "baseline_eps_avg": 4.26528,
                "revenue_delta": -10_838_939.0,
                "baseline_revenue_avg": 87_570_806_507.0,
                "direction": "up",
            }
        ],
    )

    result = estimates.screen_estimate_revisions(
        tickers=["MSFT"],
        direction="all",
        include_immaterial=True,
    )

    assert result["result_count"] == 1
    row = result["results"][0]
    assert row["is_material"] is False
    assert row["direction"] == "flat"
    assert row["raw_direction"] == "up"
    assert row["materiality_reason"] == "below_threshold"
    assert row["eps_delta_pct"] == 0.00318 / 4.26528
    assert row["revenue_delta_pct"] == -10_838_939.0 / 87_570_806_507.0


def test_screen_uses_material_revenue_direction_when_eps_is_noise(monkeypatch) -> None:
    _install_fake_summary(
        monkeypatch,
        [
            {
                "ticker": "REV",
                "eps_delta": 0.003,
                "baseline_eps_avg": 4.0,
                "revenue_delta": -1_000_000_000.0,
                "baseline_revenue_avg": 10_000_000_000.0,
                "direction": "up",
            }
        ],
    )

    result = estimates.screen_estimate_revisions(tickers=["REV"], direction="down")

    assert result["result_count"] == 1
    row = result["results"][0]
    assert row["ticker"] == "REV"
    assert row["is_material"] is True
    assert row["direction"] == "down"
    assert row["raw_direction"] == "up"
    assert row["materiality_basis"] == ["revenue_delta_pct"]
