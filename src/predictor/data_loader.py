#!/usr/bin/env python3
"""
S3データローダー

S3から釣果データと学習済みモデルを読み込む
"""

import io
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, Tuple

import boto3
import pandas as pd


class S3DataLoader:
    """S3からデータとモデルを読み込むクラス"""

    def __init__(self, bucket_name: str, region_name: str = 'ap-northeast-1'):
        """
        Args:
            bucket_name: S3バケット名
            region_name: AWSリージョン（デフォルト: 東京）
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)

    def load_historical_data(
        self,
        key: str = 'data/fishing_data.csv',
        days: int = 365
    ) -> pd.DataFrame:
        """
        S3から過去の釣果データを読み込む

        Args:
            key: S3オブジェクトキー
            days: 読み込む過去の日数（デフォルト: 365日）

        Returns:
            DataFrame: 過去データ（date, aji_count, visitors, water_temp, weather）

        Raises:
            Exception: S3読み込みエラー
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )

            # CSVを読み込み
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            df['date'] = pd.to_datetime(df['date'])

            # 日付でソート
            df = df.sort_values('date').reset_index(drop=True)

            # 直近N日分のみフィルタ
            if days is not None:
                today = datetime.now().date()
                start_date = today - timedelta(days=days)
                df = df[df['date'].dt.date >= start_date].copy()

            return df

        except Exception as e:
            raise Exception(f"Failed to load data from S3: {str(e)}")

    def load_model(
        self,
        model_key: str = 'models/model_cv.pkl'
    ) -> object:
        """
        S3から学習済みモデルを読み込む

        Args:
            model_key: モデルファイルのS3キー

        Returns:
            学習済みモデル（sklearn RandomForestRegressor）

        Raises:
            Exception: S3読み込みエラー
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=model_key
            )

            model = pickle.loads(response['Body'].read())
            return model

        except Exception as e:
            raise Exception(f"Failed to load model from S3: {str(e)}")

    def load_config(
        self,
        config_key: str = 'models/config.json'
    ) -> Dict:
        """
        S3からモデル設定を読み込む

        Args:
            config_key: 設定ファイルのS3キー

        Returns:
            dict: モデル設定
                - selected_features: 使用する特徴量リスト
                - bias_factor: 保守係数（0.7）
                - threshold: 判定しきい値（1.0匹/人）

        Raises:
            Exception: S3読み込みエラー
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=config_key
            )

            config = json.loads(response['Body'].read())
            return config

        except Exception as e:
            raise Exception(f"Failed to load config from S3: {str(e)}")

    def load_all(
        self,
        data_key: str = 'data/fishing_data.csv',
        model_key: str = 'models/model_cv.pkl',
        config_key: str = 'models/config.json',
        days: int = 365
    ) -> Tuple[pd.DataFrame, object, Dict]:
        """
        データ、モデル、設定をまとめて読み込む

        Args:
            data_key: データファイルのS3キー
            model_key: モデルファイルのS3キー
            config_key: 設定ファイルのS3キー
            days: 読み込む過去の日数

        Returns:
            tuple: (データフレーム, モデル, 設定dict)
        """
        df = self.load_historical_data(key=data_key, days=days)
        model = self.load_model(model_key=model_key)
        config = self.load_config(config_key=config_key)

        return df, model, config


def upload_data_to_s3(
    df: pd.DataFrame,
    bucket_name: str,
    key: str = 'data/fishing_data.csv',
    region_name: str = 'ap-northeast-1'
) -> None:
    """
    データフレームをS3にアップロード（データ更新用）

    Args:
        df: アップロードするDataFrame
        bucket_name: S3バケット名
        key: S3オブジェクトキー
        region_name: AWSリージョン

    Raises:
        Exception: S3アップロードエラー
    """
    try:
        s3_client = boto3.client('s3', region_name=region_name)

        # DataFrameをCSVに変換
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # S3にアップロード
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    except Exception as e:
        raise Exception(f"Failed to upload data to S3: {str(e)}")
