"""AWS Lambda ハンドラー.

両施設 (本牧・大黒) の翌日 釣行判定 予測を実行し、
1通の SNS メールで結果を配信する。
"""

import os

import boto3
from data_loader import S3DataLoader
from facility_config import FACILITIES

from predictor import FishingPredictor


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

    # 予測結果を S3 に保存
    loader.save_prediction(
        facility=facility,
        prediction_date=result["prediction_date"],
        predicted_catch=result["predicted_catch"],
        go_decision=result["go_decision"],
    )

    return result


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
