#!/usr/bin/env python3
"""
AWS Lambda ハンドラー

実行日の翌日の釣果予測とリスクレベルを返す
"""

import os

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

        # S3からデータ・モデル・設定を読み込む
        loader = S3DataLoader(bucket_name=bucket_name)
        historical_data, model, config = loader.load_all()

        # 予測実行
        predictor = FishingPredictor(model=model, config=config)
        result = predictor.predict_tomorrow(historical_data=historical_data)

        return {
            'statusCode': 200,
            'body': {
                'date': result['prediction_date'],
                'predicted_catch': round(result['conservative_prediction'], 2),
                'risk_level': result['risk_level']
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }
