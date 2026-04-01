"""釣果予測推論ロジック.

学習済みの LightGBM 回帰モデルを使って翌日の 釣行判定 を予測する。
"""

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from features import create_features


class FishingPredictor:
    """釣果予測クラス."""

    def __init__(self, artifacts: dict[str, Any]):
        """
        Args:
            artifacts: S3DataLoader.load_artifacts() の戻り値
                - model: LGBMRegressor
                - config: {selected_features, threshold, ...}
        """
        self.model = artifacts["model"]
        self.config = artifacts["config"]
        self.selected_features = self.config["selected_features"]
        self.threshold = self.config["threshold"]

    def predict_tomorrow(
        self,
        historical_data: pd.DataFrame,
        target_date: datetime.date = None,
    ) -> dict:
        """翌日の 釣行判定 を予測する.

        Args:
            historical_data: 過去データ (外部データ列を含む)
            target_date: 予測対象日 (None なら翌日)

        Returns:
            dict: {prediction_date, predicted_catch, go_decision, threshold}
        """
        if target_date is None:
            target_date = datetime.now().date() + timedelta(days=1)
        else:
            target_date = target_date + timedelta(days=1)

        # 特徴量作成
        df = create_features(historical_data)
        df_clean = df.replace([np.inf, -np.inf], np.nan).dropna(
            subset=[c for c in self.selected_features if c in df.columns]
        )

        if len(df_clean) == 0:
            raise ValueError("特徴量作成後のデータが空です。過去データが不足している可能性があります。")

        # 最新行の特徴量を取得
        latest = df_clean.iloc[[-1]]
        X = latest[self.selected_features]

        # LightGBM 回帰で予測 → expm1 → 閾値比較
        raw_pred = self.model.predict(X)[0]
        predicted_catch = float(max(np.expm1(raw_pred), 0))
        go_decision = predicted_catch >= self.threshold

        return {
            "prediction_date": target_date.strftime("%Y-%m-%d"),
            "predicted_catch": round(predicted_catch, 2),
            "go_decision": go_decision,
            "threshold": self.threshold,
        }
