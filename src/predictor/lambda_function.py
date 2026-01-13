#!/usr/bin/env python3
"""
AWS Lambda ハンドラー

実行日の翌日の釣果予測とリスクレベルを返す
SNS経由でメール通知を送信する
"""

import os

import boto3
from data_loader import S3DataLoader
from inference import FishingPredictor


def lambda_handler(event, context):
    """
    Lambda エントリーポイント

    Returns:
        dict: 予測結果
            - date: 予測対象日 (YYYY-MM-DD)
            - predicted_catch: 予測釣果（匹/人）
            - risk_level: リスクレベル (0-3)
    """
    try:
        # 環境変数から設定を取得
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'fishing-catch-predictor')
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

        # S3からデータ・モデル・設定を読み込む
        loader = S3DataLoader(bucket_name=bucket_name)
        historical_data, model, config = loader.load_all()

        # 予測実行
        predictor = FishingPredictor(model=model, config=config)
        result = predictor.predict_tomorrow(historical_data=historical_data)

        prediction = {
            'date': result['prediction_date'],
            'predicted_catch': round(result['conservative_prediction'], 2),
            'risk_level': result['risk_level']
        }

        # SNS でメール送信
        if sns_topic_arn:
            _send_notification(sns_topic_arn, prediction)

        return {
            'statusCode': 200,
            'body': prediction
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }


def _send_notification(topic_arn: str, prediction: dict):
    """
    SNS経由でメール通知を送信

    Args:
        topic_arn: SNSトピックのARN
        prediction: 予測結果
    """
    sns = boto3.client('sns')

    # リスクレベルを表示用に変換
    risk_display = _get_risk_display(prediction['risk_level'])

    # メール件名
    subject = f"【釣果予測】{prediction['date']} の予測結果"

    # メール本文
    message = f"""
明日の釣果予測をお届けします。

━━━━━━━━━━━━━━━━━━━━━━━━
📅 予測日: {prediction['date']}
━━━━━━━━━━━━━━━━━━━━━━━━

🎣 予測釣果: {prediction['predicted_catch']:.1f} 匹/人

{risk_display['emoji']} リスクレベル: {risk_display['level']}
   {risk_display['description']}

━━━━━━━━━━━━━━━━━━━━━━━━
{_get_recommendation(prediction)}
━━━━━━━━━━━━━━━━━━━━━━━━

※ この予測は過去データに基づく参考値です。
※ 天候や海況により実際の釣果は変動します。
"""

    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message
    )


def _get_risk_display(risk_level: int) -> dict:
    """リスクレベルを表示用に変換"""
    displays = {
        0: {'level': '低 ⭐⭐⭐', 'emoji': '🟢', 'description': '予測の信頼度が高いです'},
        1: {'level': '低 ⭐⭐⭐', 'emoji': '🟢', 'description': '予測の信頼度が高いです'},
        2: {'level': '中 ⭐⭐', 'emoji': '🟡', 'description': '外れる可能性がやや高いです'},
        3: {'level': '高 ⭐', 'emoji': '🔴', 'description': '外れる可能性が高いです'},
    }
    return displays.get(risk_level, displays[3])


def _get_recommendation(prediction: dict) -> str:
    """予測結果に基づくおすすめメッセージ"""
    catch = prediction['predicted_catch']
    risk = prediction['risk_level']

    if catch >= 1.0 and risk <= 1:
        return "✅ おすすめ: 釣りに行きましょう！好釣果が期待できます。"
    elif catch >= 1.0 and risk >= 2:
        return "⚠️ 判断注意: 釣果は期待できますが、予測の信頼度がやや低めです。"
    elif catch < 1.0 and risk <= 1:
        return "❌ おすすめしない: 今回は見送りが無難です。"
    else:
        return "❌ おすすめしない: 釣果・信頼度ともに低いため、見送りが無難です。"
