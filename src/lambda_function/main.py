#!/usr/bin/env python3
"""
AWS Lambda関数 - 釣果予測API

明日釣りに行くべきかを判断するLambda関数
"""

import json
import os
from datetime import datetime

import sys
sys.path.append('/var/task/src')

from predictor.data_loader import S3DataLoader
from predictor.inference import FishingPredictor


# 環境変数
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'inference-choka')
S3_DATA_KEY = os.environ.get('S3_DATA_KEY', 'data/fishing_data.csv')
S3_MODEL_KEY = os.environ.get('S3_MODEL_KEY', 'models/model_cv.pkl')
S3_CONFIG_KEY = os.environ.get('S3_CONFIG_KEY', 'models/config.json')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント

    Args:
        event: Lambda イベント
            - target_date (optional): 予測基準日（YYYY-MM-DD形式）
                指定しない場合は実行日の翌日を予測
        context: Lambda コンテキスト

    Returns:
        dict: API Gateway形式のレスポンス
            - statusCode: HTTPステータスコード
            - body: JSON文字列
                - prediction_date: 予測対象日
                - raw_prediction: 生予測値（匹/人）
                - conservative_prediction: 保守的予測値（匹/人）
                - should_go: 釣りに行くべきか
                - confidence_level: 信頼度（高/中/低）
                - confidence_stars: 信頼度（星）
                - risk_reasons: リスク要因リスト
                - threshold: 判定しきい値
                - message: 判断メッセージ

    Example:
        # APIリクエスト例
        {
            "target_date": "2025-11-13"  # オプション
        }

        # レスポンス例
        {
            "statusCode": 200,
            "body": {
                "prediction_date": "2025-11-14",
                "raw_prediction": 2.17,
                "conservative_prediction": 1.52,
                "should_go": true,
                "confidence_level": "高",
                "confidence_stars": "⭐⭐⭐",
                "risk_reasons": ["前日好調", "安定期"],
                "threshold": 1.0,
                "message": "釣りに行く！期待釣果: 1.52匹/人以上"
            }
        }
    """
    try:
        # イベントからパラメータ取得
        target_date = None
        if event and 'target_date' in event:
            target_date = datetime.strptime(event['target_date'], '%Y-%m-%d').date()

        # S3からデータとモデルをロード
        loader = S3DataLoader(bucket_name=S3_BUCKET_NAME, region_name=AWS_REGION)
        historical_data, model, config = loader.load_all(
            data_key=S3_DATA_KEY,
            model_key=S3_MODEL_KEY,
            config_key=S3_CONFIG_KEY,
            days=365  # 過去365日分のデータを使用
        )

        # 予測実行
        predictor = FishingPredictor(model=model, config=config)
        result = predictor.predict_tomorrow(
            historical_data=historical_data,
            target_date=target_date
        )

        # 判断メッセージの生成
        if result['should_go']:
            message = f"釣りに行く！期待釣果: {result['conservative_prediction']:.2f}匹/人以上"
            if result['confidence_level'] in ['中', '低']:
                message += f" ⚠️ 注意: 予測信頼度は{result['confidence_level']}です。外れリスクがやや高い条件です。"
        else:
            message = f"釣りに行かない。期待釣果: {result['conservative_prediction']:.2f}匹/人（{result['threshold']:.2f}未満）"

        result['message'] = message

        # 成功レスポンス
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps(result, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        # エラーレスポンス
        error_message = f"予測処理でエラーが発生しました: {str(e)}"
        print(f"ERROR: {error_message}")

        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'error': error_message
            }, ensure_ascii=False, indent=2)
        }


# ローカルテスト用
if __name__ == '__main__':
    # ローカル実行時のテスト
    test_event = {
        # 'target_date': '2025-11-13'  # オプション
    }
    test_context = {}

    response = lambda_handler(test_event, test_context)
    print(json.dumps(json.loads(response['body']), ensure_ascii=False, indent=2))
