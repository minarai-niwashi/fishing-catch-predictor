#!/usr/bin/env python3
"""
特徴量作成モジュール

fish_model/features.py から本番運用に必要な機能のみを抽出・リファクタリング
"""

import numpy as np
import pandas as pd


def get_moon_phase(date):
    """
    月齢を計算

    Args:
        date: pd.Timestamp

    Returns:
        float: 月齢（0.0-1.0の範囲で、0.0が新月、0.5が満月）
    """
    ref_date = pd.Timestamp('2000-01-06')
    days_since_ref = (date - ref_date).days
    moon_cycle = 29.53058867
    moon_phase = (days_since_ref % moon_cycle) / moon_cycle
    return moon_phase


def create_features(df):
    """
    予測に必要な特徴量を作成

    Args:
        df: DataFrame with columns: ['date', 'aji_count', 'visitors', 'water_temp', 'weather']

    Returns:
        DataFrame: 特徴量が追加されたDataFrame

    Note:
        - 全ての特徴量は shift(1) 以上を使用（データリーク防止）
        - 最低15日分、理想的には365日分のデータが必要
    """
    df = df.sort_values('date').reset_index(drop=True)
    df['aji_per_person'] = df['aji_count'] / (df['visitors'] + 1)

    # 月齢
    df['moon_phase'] = df['date'].apply(get_moon_phase)
    df['moon_phase_sin'] = np.sin(2 * np.pi * df['moon_phase'])
    df['moon_phase_cos'] = np.cos(2 * np.pi * df['moon_phase'])
    df['is_full_moon'] = (
        (df['moon_phase'] > 0.4) & (df['moon_phase'] < 0.6)
    ).astype(int)
    df['is_new_moon'] = (
        (df['moon_phase'] < 0.1) | (df['moon_phase'] > 0.9)
    ).astype(int)

    # ラグ特徴量
    for lag in [1, 2, 3, 7, 14, 21, 28]:
        df[f'aji_pp_lag{lag}'] = df['aji_per_person'].shift(lag)
        df[f'aji_lag{lag}'] = df['aji_count'].shift(lag)
        df[f'visitors_lag{lag}'] = df['visitors'].shift(lag)
        df[f'temp_lag{lag}'] = df['water_temp'].shift(lag)

    # 季節パターン
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['dayofweek'] = df['date'].dt.dayofweek
    df['week_of_year'] = df['date'].dt.isocalendar().week

    df['aji_pp_same_week_last_year'] = df.groupby(
        'week_of_year'
    )['aji_per_person'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    df['aji_pp_month_avg'] = df.groupby('month')['aji_per_person'].transform(
        lambda x: x.shift(1).expanding().mean()
    )
    df['aji_pp_dayofweek_avg'] = df.groupby(
        'dayofweek'
    )['aji_per_person'].transform(
        lambda x: x.shift(1).expanding().mean()
    )

    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)

    # 水温勾配
    for days in [1, 3, 7, 14]:
        df[f'temp_gradient_{days}d'] = (
            df['water_temp'].shift(1) - df['water_temp'].shift(1+days)
        ) / days

    df['temp_acceleration'] = (
        df['temp_gradient_1d'] - df['temp_gradient_1d'].shift(1)
    )
    df['temp_rising'] = (df['temp_gradient_1d'] > 0).astype(int)
    df['temp_falling'] = (df['temp_gradient_1d'] < 0).astype(int)
    df['temp_optimal'] = (
        (df['water_temp'] >= 15) & (df['water_temp'] <= 23)
    ).astype(int)
    df['temp_optimal_lag1'] = df['temp_optimal'].shift(1)

    # 移動統計
    for window in [3, 7, 14, 30]:
        df[f'aji_pp_ma{window}'] = (
            df['aji_per_person'].shift(1).rolling(
                window=window, min_periods=1
            ).mean()
        )
        df[f'aji_pp_std{window}'] = (
            df['aji_per_person'].shift(1).rolling(
                window=window, min_periods=2
            ).std()
        )
        df[f'aji_pp_max{window}'] = (
            df['aji_per_person'].shift(1).rolling(
                window=window, min_periods=1
            ).max()
        )
        df[f'aji_pp_min{window}'] = (
            df['aji_per_person'].shift(1).rolling(
                window=window, min_periods=1
            ).min()
        )
        df[f'aji_ma{window}'] = (
            df['aji_count'].shift(1).rolling(window=window, min_periods=1).mean()
        )
        df[f'aji_std{window}'] = (
            df['aji_count'].shift(1).rolling(window=window, min_periods=2).std()
        )
        df[f'visitors_ma{window}'] = (
            df['visitors'].shift(1).rolling(window=window, min_periods=1).mean()
        )
        df[f'visitors_std{window}'] = (
            df['visitors'].shift(1).rolling(window=window, min_periods=2).std()
        )
        df[f'temp_ma{window}'] = (
            df['water_temp'].shift(1).rolling(
                window=window, min_periods=1
            ).mean()
        )
        df[f'temp_std{window}'] = (
            df['water_temp'].shift(1).rolling(
                window=window, min_periods=2
            ).std()
        )

    # 変化率
    df['aji_pp_change'] = (
        df['aji_per_person'].shift(1) - df['aji_per_person'].shift(2)
    )
    df['aji_pp_pct_change'] = df['aji_per_person'].pct_change(1).shift(1)
    df['aji_pp_is_increasing'] = (
        df['aji_pp_lag1'] > df['aji_pp_lag2']
    ).astype(int)
    df['visitors_change'] = df['visitors'].shift(1) - df['visitors'].shift(2)
    df['visitors_pct_change'] = df['visitors'].pct_change(1).shift(1)

    # DataFrameの断片化を軽減
    df = df.copy()

    # 交互作用
    df['aji_pp_lag1_x_temp_lag1'] = df['aji_pp_lag1'] * df['temp_lag1']
    df['aji_pp_lag1_x_visitors_lag1'] = df['aji_pp_lag1'] * df['visitors_lag1']
    df['aji_lag1_x_temp_lag1'] = df['aji_lag1'] * df['temp_lag1']
    df['temp_lag1_x_visitors_lag1'] = df['temp_lag1'] * df['visitors_lag1']
    df['aji_pp_lag1_x_moon'] = df['aji_pp_lag1'] * df['moon_phase_sin']
    df['temp_lag1_x_moon'] = df['temp_lag1'] * df['moon_phase_sin']
    df['aji_pp_lag1_x_month_sin'] = df['aji_pp_lag1'] * df['month_sin']
    df['temp_lag1_x_month_sin'] = df['temp_lag1'] * df['month_sin']
    df['aji_pp_lag1_x_temp_gradient'] = (
        df['aji_pp_lag1'] * df['temp_gradient_7d']
    )
    df['aji_pp_ma7_x_temp_gradient'] = (
        df['aji_pp_ma7'] * df['temp_gradient_7d']
    )

    # 比率
    df['aji_pp_to_ma3_ratio'] = df['aji_pp_lag1'] / (df['aji_pp_ma3'] + 0.1)
    df['aji_pp_to_ma7_ratio'] = df['aji_pp_lag1'] / (df['aji_pp_ma7'] + 0.1)
    df['aji_pp_to_ma14_ratio'] = df['aji_pp_lag1'] / (df['aji_pp_ma14'] + 0.1)
    df['visitors_to_ma7_ratio'] = df['visitors_lag1'] / (df['visitors_ma7'] + 1)
    df['temp_to_ma7_ratio'] = df['temp_lag1'] / (df['temp_ma7'] + 0.1)

    # 天気ダミー
    df['weather_lag1'] = df['weather'].shift(1)
    weather_dummies = pd.get_dummies(df['weather_lag1'], prefix='weather_lag1')
    df = pd.concat([df, weather_dummies], axis=1)

    # DataFrameの断片化を解消
    return df.copy()
