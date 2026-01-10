#!/usr/bin/env python3
"""
釣果予測推論ロジック

学習済みモデルを使って翌日の釣果を予測
"""

from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
from features import create_features


class FishingPredictor:
    """釣果予測クラス"""

    def __init__(self, model, config: Dict):
        """
        Args:
            model: 学習済みモデル（sklearn RandomForestRegressor）
            config: モデル設定
                - selected_features: 使用する特徴量リスト
                - bias_factor: 保守係数（0.7）
        """
        self.model = model
        self.selected_features = config['selected_features']
        self.bias_factor = config['bias_factor']

    def predict_tomorrow(
        self,
        historical_data: pd.DataFrame,
        target_date: datetime.date = None
    ) -> Dict:
        """
        翌日の釣果を予測

        Args:
            historical_data: 過去の釣果データ
                必須カラム: ['date', 'aji_count', 'visitors', 'water_temp', 'weather']
            target_date: 予測対象日（指定しない場合は実行日の翌日）

        Returns:
            dict: 予測結果
                - prediction_date: 予測対象日
                - conservative_prediction: 保守的予測値（匹/人）
                - risk_level: リスクレベル (0-3)

        Raises:
            ValueError: データが不足している場合
        """
        # 予測対象日の決定
        if target_date is None:
            target_date = datetime.now().date() + timedelta(days=1)
        else:
            target_date = target_date + timedelta(days=1)

        # 特徴量作成
        df = create_features(historical_data)

        # NaN/Infを除去
        df_clean = df.replace([np.inf, -np.inf], np.nan).dropna()

        if len(df_clean) == 0:
            raise ValueError("特徴量作成後のデータが空です。過去データが不足している可能性があります。")

        # 最新のデータ（実行日）の特徴量を使って翌日を予測
        X = df_clean[self.selected_features].iloc[[-1]]
        X_full = df_clean.iloc[[-1]]  # リスク判定用

        # 予測
        raw_prediction = self.model.predict(X)[0]
        conservative_prediction = raw_prediction * self.bias_factor

        # リスクレベル判定
        risk_level = self._assess_risk(X_full)

        return {
            'prediction_date': target_date.strftime('%Y-%m-%d'),
            'conservative_prediction': float(conservative_prediction),
            'risk_level': risk_level,
        }

    def _assess_risk(self, X_full: pd.DataFrame) -> int:
        """
        リスクレベルを判定

        Args:
            X_full: 特徴量データフレーム（1行）

        Returns:
            int: リスクレベル (0-3)
        """
        risk_level = 0

        # 前日釣果
        aji_pp_lag1 = X_full['aji_pp_lag1'].values[0] if 'aji_pp_lag1' in X_full.columns else np.nan
        if not np.isnan(aji_pp_lag1) and aji_pp_lag1 < 2.0:
            risk_level += 1

        # 最近の変動
        aji_std7 = X_full['aji_std7'].values[0] if 'aji_std7' in X_full.columns else np.nan
        if not np.isnan(aji_std7) and aji_std7 > 400:
            risk_level += 1

        # 月（リスク月）
        month = X_full['month'].values[0] if 'month' in X_full.columns else np.nan
        if not np.isnan(month) and month in [5, 6, 7, 8, 9]:
            risk_level += 1

        return risk_level
