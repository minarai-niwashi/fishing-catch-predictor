"""S3データローダー.

施設ごとのデータ・モデルアーティファクトを S3 から読み込む。
"""

import io
import json
import pickle
from datetime import datetime, timedelta
from typing import Any

import boto3
import pandas as pd


class S3DataLoader:
    """S3からデータとモデルを読み込むクラス."""

    def __init__(self, bucket_name: str, region_name: str = "ap-northeast-1"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3", region_name=region_name)

    def load_historical_data(
        self,
        facility: str,
        days: int = 365,
    ) -> pd.DataFrame:
        """S3から施設の過去データを読み込む."""
        key = f"data/{facility}/fishing_data.csv"
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        if days is not None:
            start_date = datetime.now().date() - timedelta(days=days)
            df = df[df["date"].dt.date >= start_date].copy()

        return df

    def load_artifacts(self, facility: str) -> dict[str, Any]:
        """施設のモデルアーティファクトを読み込む.

        Returns:
            dict: {model, config}
        """
        prefix = f"models/{facility}"

        # model.pkl
        resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=f"{prefix}/model.pkl")
        model = pickle.loads(resp["Body"].read())

        # config.json
        resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=f"{prefix}/config.json")
        config = json.loads(resp["Body"].read())

        return {"model": model, "config": config}

    def load_predictions(self, facility: str) -> pd.DataFrame:
        """施設の予測履歴を S3 から読み込む (未作成なら空 DataFrame)."""
        key = f"predictions/{facility}/predictions.csv"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        except self.s3_client.exceptions.NoSuchKey:
            return pd.DataFrame(
                columns=["prediction_date", "predicted_catch", "go_decision", "created_at"]
            )

        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        df["prediction_date"] = pd.to_datetime(df["prediction_date"])
        df["go_decision"] = (
            df["go_decision"].astype(str).str.lower().map({"true": True, "false": False})
        )
        return df

    def save_prediction(
        self,
        facility: str,
        prediction_date: str,
        predicted_catch: float,
        go_decision: bool,
    ) -> None:
        """予測結果を S3 に CSV 形式で保存する."""
        key = f"predictions/{facility}/predictions.csv"
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        new_row = pd.DataFrame([{
            "prediction_date": prediction_date,
            "predicted_catch": predicted_catch,
            "go_decision": go_decision,
            "created_at": created_at,
        }])

        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            existing_df = pd.read_csv(io.BytesIO(response["Body"].read()))
            df = pd.concat([existing_df, new_row], ignore_index=True)
        except self.s3_client.exceptions.NoSuchKey:
            df = new_row

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
        )
