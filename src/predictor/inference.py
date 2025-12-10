#!/usr/bin/env python3
"""
釣果予測推論ロジック

学習済みモデルを使って翌日の釣果を予測
"""

from datetime import datetime, timedelta
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from .features import create_features


class FishingPredictor:
    """釣果予測クラス"""

    def __init__(self, model, config: Dict):
        """
        Args:
            model: 学習済みモデル（sklearn RandomForestRegressor）
            config: モデル設定
                - selected_features: 使用する特徴量リスト
                - bias_factor: 保守係数（0.7）
                - threshold: 判定しきい値（1.0匹/人）
        """
        self.model = model
        self.selected_features = config['selected_features']
        self.bias_factor = config['bias_factor']
        self.threshold = config['threshold']

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
                - raw_prediction: 生予測値（匹/人）
                - conservative_prediction: 保守的予測値（匹/人）
                - should_go: 釣りに行くべきか（bool）
                - confidence_level: 信頼度（'高', '中', '低'）
                - confidence_stars: 信頼度（星）
                - risk_reasons: リスク要因リスト
                - threshold: 判定しきい値

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
        risk_level, risk_reasons = self._assess_risk(X_full)

        # 信頼度表示
        confidence_level, confidence_stars = self._get_confidence_display(risk_level)

        # 行くべきか判定
        should_go = conservative_prediction >= self.threshold

        return {
            'prediction_date': target_date.strftime('%Y-%m-%d'),
            'raw_prediction': float(raw_prediction),
            'conservative_prediction': float(conservative_prediction),
            'should_go': bool(should_go),
            'confidence_level': confidence_level,
            'confidence_stars': confidence_stars,
            'risk_reasons': risk_reasons,
            'threshold': float(self.threshold),
            'bias_factor': float(self.bias_factor)
        }

    def _assess_risk(self, X_full: pd.DataFrame) -> Tuple[int, list]:
        """
        リスクレベルを判定

        Args:
            X_full: 特徴量データフレーム（1行）

        Returns:
            tuple: (リスクレベル, リスク要因リスト)
        """
        risk_level = 0
        risk_reasons = []

        # 前日釣果
        aji_pp_lag1 = X_full['aji_pp_lag1'].values[0] if 'aji_pp_lag1' in X_full.columns else np.nan
        if not np.isnan(aji_pp_lag1):
            if aji_pp_lag1 < 2.0:
                risk_level += 1
                risk_reasons.append("前日低調")
            else:
                risk_reasons.append("前日好調")

        # 最近の変動
        aji_std7 = X_full['aji_std7'].values[0] if 'aji_std7' in X_full.columns else np.nan
        if not np.isnan(aji_std7):
            if aji_std7 > 400:
                risk_level += 1
                risk_reasons.append("変動大")
            else:
                risk_reasons.append("安定期")

        # 月（リスク月）
        month = X_full['month'].values[0] if 'month' in X_full.columns else np.nan
        if not np.isnan(month):
            if month in [5, 6, 7, 8, 9]:
                risk_level += 1
                risk_reasons.append("リスク月")

        return risk_level, risk_reasons

    def _get_confidence_display(self, risk_level: int) -> Tuple[str, str]:
        """
        リスクレベルから信頼度表示を取得

        Args:
            risk_level: リスクレベル（0-3）

        Returns:
            tuple: (信頼度レベル, 星表示)
        """
        if risk_level <= 1:
            return "高", "⭐⭐⭐"
        elif risk_level == 2:
            return "中", "⭐⭐"
        else:
            return "低", "⭐"

    def get_recent_performance(
        self,
        historical_data: pd.DataFrame,
        days: int = 7
    ) -> pd.DataFrame:
        """
        最近N日間の実績を取得

        Args:
            historical_data: 過去の釣果データ
            days: 取得する日数（デフォルト: 7日）

        Returns:
            DataFrame: 最近の実績（date, aji_per_person, water_temp, weather）
        """
        df = historical_data.copy()
        df['aji_per_person'] = df['aji_count'] / (df['visitors'] + 1)

        recent = df.tail(days)[['date', 'aji_per_person', 'water_temp', 'weather']].copy()
        recent.columns = ['日付', '釣果(匹/人)', '水温', '天気']
        recent['日付'] = pd.to_datetime(recent['日付']).dt.strftime('%m/%d')

        return recent
