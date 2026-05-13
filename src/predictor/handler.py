"""AWS Lambda ハンドラー.

両施設 (本牧・大黒) の翌日 釣行判定 予測を実行し、
1通の SNS メールで結果を配信する。
"""

import os

import boto3
import pandas as pd
from data_loader import S3DataLoader
from facility_config import FACILITIES

from predictor import FishingPredictor

ACTUAL_GO_THRESHOLD = 1.0


def lambda_handler(event, context):
    """Lambda エントリーポイント.

    環境変数:
        S3_BUCKET_NAME: S3バケット名 (default: fishing-catch-predictor)
        SNS_TOPIC_ARN: SNSトピックARN
        FACILITIES: カンマ区切りの施設名 (default: honmoku,daikoku)
    """
    try:
        bucket_name = os.environ.get("S3_BUCKET_NAME", "fishing-catch-predictor")
        sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
        facility_names = os.environ.get("FACILITIES", "honmoku,daikoku").split(",")

        loader = S3DataLoader(bucket_name=bucket_name)

        results = {}

        for facility in facility_names:
            facility = facility.strip()
            if facility not in FACILITIES:
                print(f"Warning: 未知の施設名 '{facility}' をスキップ")
                continue

            try:
                result = _predict_facility(loader, facility)
                results[facility] = result
                print(f"{FACILITIES[facility]['display_name']}: "
                      f"予測={result['predicted_catch']:.2f}匹/人, "
                      f"判定={'Go' if result['go_decision'] else 'No-Go'}")
            except Exception as e:
                print(f"Error: {FACILITIES[facility]['display_name']} の予測に失敗: {e}")
                results[facility] = {"error": str(e)}

        # SNS 通知
        if sns_topic_arn and results:
            _send_notification(sns_topic_arn, results)

        return {
            "statusCode": 200,
            "body": results,
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": {"error": str(e)},
        }


def _predict_facility(loader: S3DataLoader, facility: str) -> dict:
    """1施設の予測を実行する."""
    fac_config = FACILITIES[facility]

    # データ読み込み (外部データは data_updater で付与済み)
    historical_data = loader.load_historical_data(facility=facility, days=365)

    # アーティファクト読み込み
    artifacts = loader.load_artifacts(facility=facility)

    # 予測実行
    predictor = FishingPredictor(artifacts=artifacts)
    result = predictor.predict_tomorrow(historical_data=historical_data)

    # 直近の実績データ
    latest = historical_data.iloc[-1]
    latest_visitors = int(latest["visitors"]) if latest["visitors"] > 0 else 0
    latest_aji_count = int(latest["aji_count"])
    latest_catch_per_person = latest_aji_count / latest_visitors if latest_visitors > 0 else 0

    result["facility"] = facility
    result["display_name"] = fac_config["display_name"]
    result["latest_date"] = latest["date"].strftime("%Y-%m-%d")
    result["latest_visitors"] = latest_visitors
    result["latest_aji_count"] = latest_aji_count
    result["latest_catch_per_person"] = round(latest_catch_per_person, 2)

    # 全期間の Go 判定の適合率・再現率
    try:
        predictions_df = loader.load_predictions(facility=facility)
        metrics = _compute_go_accuracy(predictions_df, historical_data)
    except Exception as e:
        print(f"Warning: {fac_config['display_name']} の精度計算に失敗: {e}")
        metrics = {
            "precision_hits": 0,
            "precision_total": 0,
            "recall_hits": 0,
            "recall_total": 0,
            "span_days": 0,
        }
    result["precision_hits"] = metrics["precision_hits"]
    result["precision_total"] = metrics["precision_total"]
    result["recall_hits"] = metrics["recall_hits"]
    result["recall_total"] = metrics["recall_total"]
    result["accuracy_span_days"] = metrics["span_days"]

    # 予測結果を S3 に保存
    config = artifacts["config"]
    loader.save_prediction(
        facility=facility,
        prediction_date=result["prediction_date"],
        predicted_catch=result["predicted_catch"],
        go_decision=result["go_decision"],
        model_version=config.get("model_type", "lgbm_regressor"),
        model_trained_at=config.get("trained_at", ""),
    )

    return result


def _compute_go_accuracy(
    predictions_df: pd.DataFrame,
    historical_df: pd.DataFrame,
) -> dict:
    """全期間の Go 判定の適合率・再現率と予測期間日数を返す.

    - TP (両指標の分子): 予測 Go かつ 実績 Go (aji_count / visitors >= 1.0)
    - 適合率の母数: 予測が Go だった日のうち、実績データ (visitors > 0) が存在する日
    - 再現率の母数: 実績が Go だった日のうち、予測が存在する日
    - 期間日数: 最初の予測日から最新予測日までのカレンダー日数
    """
    empty = {
        "precision_hits": 0,
        "precision_total": 0,
        "recall_hits": 0,
        "recall_total": 0,
        "span_days": 0,
    }
    if predictions_df.empty or historical_df.empty:
        return empty

    preds = (
        predictions_df.sort_values("created_at")
        .drop_duplicates("prediction_date", keep="last")
    )

    pred_dates = pd.to_datetime(preds["prediction_date"])
    span_days = int((pred_dates.max() - pred_dates.min()).days) + 1

    actual = historical_df[historical_df["visitors"] > 0].copy()
    if actual.empty:
        return {**empty, "span_days": span_days}
    actual["actual_go"] = (actual["aji_count"] / actual["visitors"]) >= ACTUAL_GO_THRESHOLD

    merged = actual.merge(
        preds[["prediction_date", "go_decision"]],
        left_on="date",
        right_on="prediction_date",
        how="inner",
    )
    merged = merged.dropna(subset=["go_decision"])
    if merged.empty:
        return {**empty, "span_days": span_days}

    tp = int((merged["go_decision"] & merged["actual_go"]).sum())
    predicted_go = int(merged["go_decision"].sum())
    actual_go = int(merged["actual_go"].sum())

    return {
        "precision_hits": tp,
        "precision_total": predicted_go,
        "recall_hits": tp,
        "recall_total": actual_go,
        "span_days": span_days,
    }


def _format_accuracy(hits: int, total: int) -> str:
    if total == 0:
        return "0/0"
    pct = round(hits / total * 100)
    return f"{hits}/{total} ({pct}%)"


def _send_notification(topic_arn: str, results: dict):
    """両施設の予測結果を1通の SNS メールで配信する."""
    sns = boto3.client("sns")

    # 予測日を取得
    prediction_date = None
    for r in results.values():
        if "prediction_date" in r:
            prediction_date = r["prediction_date"]
            break

    if prediction_date is None:
        return

    subject = f"【釣果予測】{prediction_date} の予測結果"

    # 施設ごとのセクションを組み立て
    sections = []
    for facility_name, r in results.items():
        if "error" in r:
            display_name = FACILITIES.get(facility_name, {}).get("display_name", facility_name)
            sections.append(
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏠 {display_name}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠ 予測エラー: {r['error']}\n"
            )
            continue

        display_name = r["display_name"]
        predicted = r["predicted_catch"]
        go_decision = r["go_decision"]

        if go_decision:
            decision_text = "✅ おすすめ: 釣りに行きましょう！"
        else:
            decision_text = "❌ 見送り: 今回は見送りが無難です。"

        precision_text = _format_accuracy(r["precision_hits"], r["precision_total"])
        recall_text = _format_accuracy(r["recall_hits"], r["recall_total"])
        section = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏠 {display_name}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎣 予測釣果: {predicted:.2f} 匹/人\n"
            f"{decision_text}\n"
            f"\n"
            f"📊 直近の実績 ({r['latest_date']})\n"
            f"   来場者数: {r['latest_visitors']:,} 人\n"
            f"   アジ釣果数: {r['latest_aji_count']:,} 匹\n"
            f"   1人あたり: {r['latest_catch_per_person']:.2f} 匹/人\n"
            f"\n"
            f"🎯 予測精度 (全{r['accuracy_span_days']}日)\n"
            f"   適合率: {precision_text}\n"
            f"   再現率: {recall_text}\n"
        )
        sections.append(section)

    message = f"""
明日の釣果予測をお届けします。

📅 予測日: {prediction_date}

{"".join(sections)}
━━━━━━━━━━━━━━━━━━━━━━━━

※ この予測は過去データに基づく参考値です。
※ 天候や海況により実際の釣果は変動します。
"""

    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message,
    )
