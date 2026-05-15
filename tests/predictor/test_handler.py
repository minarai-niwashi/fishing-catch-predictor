"""handler._compute_go_accuracy のユニットテスト."""

import pandas as pd
import pytest

import handler


def _make_predictions(rows):
    df = pd.DataFrame(
        rows, columns=["prediction_date", "predicted_catch", "go_decision", "created_at"]
    )
    df["prediction_date"] = pd.to_datetime(df["prediction_date"])
    return df


def _make_historical(rows):
    df = pd.DataFrame(rows, columns=["date", "aji_count", "visitors"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def test_returns_zeros_for_empty_inputs():
    result = handler._compute_go_accuracy(pd.DataFrame(), pd.DataFrame())
    assert result == {
        "precision_hits": 0,
        "precision_total": 0,
        "recall_hits": 0,
        "recall_total": 0,
        "span_days": 0,
    }


def test_basic_precision_and_recall():
    preds = _make_predictions([
        ("2026-04-20", 2.0, True, "2026-04-19 21:30:00"),
        ("2026-04-21", 0.5, False, "2026-04-20 21:30:00"),
        ("2026-04-22", 1.5, True, "2026-04-21 21:30:00"),
    ])
    hist = _make_historical([
        ("2026-04-20", 200, 100),  # aji/visitors = 2.0 >= 1.0 → 実績 Go
        ("2026-04-21", 50, 100),   # 0.5 → 実績 No-Go
        ("2026-04-22", 50, 100),   # 0.5 → 実績 No-Go (予測 Go → FP)
    ])

    result = handler._compute_go_accuracy(preds, hist)

    # TP: 2026-04-20 のみ
    assert result["precision_hits"] == 1
    # 予測 Go: 2026-04-20, 2026-04-22 の 2件
    assert result["precision_total"] == 2
    assert result["recall_hits"] == 1
    # 実績 Go: 2026-04-20 の 1件
    assert result["recall_total"] == 1
    # 予測の最小日〜最大日 = 3日間
    assert result["span_days"] == 3


def test_deduplicates_predictions_keeping_latest_created_at():
    preds = _make_predictions([
        ("2026-04-20", 0.5, False, "2026-04-19 21:30:00"),
        ("2026-04-20", 2.0, True, "2026-04-19 22:00:00"),
    ])
    hist = _make_historical([("2026-04-20", 200, 100)])

    result = handler._compute_go_accuracy(preds, hist)

    # 新しい created_at の予測 (Go=True) が採用される
    assert result["precision_hits"] == 1
    assert result["precision_total"] == 1


def test_skips_zero_visitor_days():
    # visitors=0 の日は実績判定不能 (0除算) なので集計から除外
    preds = _make_predictions([
        ("2026-04-20", 2.0, True, "2026-04-19 21:30:00"),
    ])
    hist = _make_historical([("2026-04-20", 100, 0)])

    result = handler._compute_go_accuracy(preds, hist)
    assert result["precision_total"] == 0
    assert result["recall_total"] == 0


def test_span_days_uses_prediction_date_range():
    # 予測のあった最小日〜最大日のカレンダー日数を返す
    preds = _make_predictions([
        ("2026-04-01", 0.5, False, "2026-03-31 21:30:00"),
        ("2026-04-10", 0.5, False, "2026-04-09 21:30:00"),
    ])
    hist = _make_historical([("2026-04-01", 50, 100)])

    result = handler._compute_go_accuracy(preds, hist)
    assert result["span_days"] == 10
