"""predictor.FishingPredictor.predict_tomorrow のユニットテスト."""

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def predictor_module():
    import predictor  # noqa: PLC0415 — autouse fixture が sys.path を整えた後でロード
    return predictor


def _make_history(days: int = 60) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2025-01-01", periods=days, freq="D")
    return pd.DataFrame({
        "date": dates,
        "aji_count": rng.integers(10, 300, days),
        "visitors": rng.integers(80, 350, days),
        "water_temp": rng.uniform(12, 22, days),
        "weather": "晴",
    })


def _make_artifacts(predicted_raw: float, threshold: float = 1.0):
    model = MagicMock()
    model.predict = MagicMock(return_value=np.array([predicted_raw]))
    return {
        "model": model,
        "config": {
            # create_features が必ず作る列名のみ指定
            "selected_features": [
                "aji_pp_lag1",
                "aji_pp_lag7",
                "visitors_lag1",
                "month_sin",
            ],
            "threshold": threshold,
        },
    }


def test_returns_go_when_above_threshold(predictor_module):
    # log1p(3.0) → expm1 で戻すと 3.0、閾値 1.0 を超えるので Go
    artifacts = _make_artifacts(predicted_raw=np.log1p(3.0), threshold=1.0)
    p = predictor_module.FishingPredictor(artifacts=artifacts)

    result = p.predict_tomorrow(_make_history(), target_date=date(2025, 2, 28))

    assert result["go_decision"] is True
    assert result["predicted_catch"] == pytest.approx(3.0, abs=0.01)
    assert result["threshold"] == 1.0
    # target_date + 1 日が予測対象になる
    assert result["prediction_date"] == "2025-03-01"


def test_returns_nogo_when_below_threshold(predictor_module):
    artifacts = _make_artifacts(predicted_raw=np.log1p(0.3), threshold=1.0)
    p = predictor_module.FishingPredictor(artifacts=artifacts)

    result = p.predict_tomorrow(_make_history(), target_date=date(2025, 2, 28))

    assert result["go_decision"] is False
    assert result["predicted_catch"] == pytest.approx(0.3, abs=0.01)


def test_clamps_negative_predictions_to_zero(predictor_module):
    # expm1(-5) ≈ -0.99 → max(., 0) で 0 にクランプ
    artifacts = _make_artifacts(predicted_raw=-5.0, threshold=1.0)
    p = predictor_module.FishingPredictor(artifacts=artifacts)

    result = p.predict_tomorrow(_make_history(), target_date=date(2025, 2, 28))

    assert result["predicted_catch"] == 0.0
    assert result["go_decision"] is False


def test_raises_when_history_too_short(predictor_module):
    # ラグ28日分も取れないデータでは特徴量がほぼ全 NaN → ValueError
    artifacts = _make_artifacts(predicted_raw=0.0)
    p = predictor_module.FishingPredictor(artifacts=artifacts)

    with pytest.raises(ValueError, match="特徴量作成後のデータが空"):
        p.predict_tomorrow(_make_history(days=5), target_date=date(2025, 1, 5))
