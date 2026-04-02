"""外部データ取得モジュール.

Open-Meteo API から気象・海洋データ、astral から月齢、
holidays-jp API から祝日データを取得し、欠損行を補完する。
"""

import time

import numpy as np
import pandas as pd


def fetch_weather(
    start_date: str,
    end_date: str,
    lat: float,
    lon: float,
) -> pd.DataFrame:
    """Open-Meteo Archive API から気象データを取得する."""
    import requests

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "wind_speed_10m_max,wind_direction_10m_dominant,precipitation_sum,pressure_msl_mean",
        "timezone": "Asia/Tokyo",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    daily = resp.json()["daily"]
    return pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "wind_speed_max": daily["wind_speed_10m_max"],
        "wind_direction": daily["wind_direction_10m_dominant"],
        "precipitation": daily["precipitation_sum"],
        "pressure_msl": daily["pressure_msl_mean"],
    })


def fetch_marine(
    start_date: str,
    end_date: str,
    lat: float,
    lon: float,
) -> pd.DataFrame:
    """Open-Meteo Marine API から海洋データを取得する."""
    import requests

    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "wave_height_max,wave_direction_dominant,wave_period_max,swell_wave_height_max",
        "timezone": "Asia/Tokyo",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    daily = resp.json()["daily"]
    return pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "wave_height_max": daily["wave_height_max"],
        "wave_direction": daily["wave_direction_dominant"],
        "wave_period_max": daily["wave_period_max"],
        "swell_height_max": daily.get("swell_wave_height_max"),
    })


def fetch_holidays() -> set[str]:
    """holidays-jp API から日本の祝日リストを取得する."""
    import requests

    url = "https://holidays-jp.github.io/api/v1/date.json"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return set(resp.json().keys())


def compute_moon_phase(dates: pd.Series) -> pd.Series:
    """astral ライブラリで月齢を計算する."""
    from astral import moon

    def _phase(d):
        dt = d.date() if hasattr(d, "date") else d
        return moon.phase(dt)

    return dates.apply(_phase)


def enrich_missing_external_data(
    df: pd.DataFrame,
    lat: float,
    lon: float,
) -> pd.DataFrame:
    """外部データ列が欠損している行のみ補完する.

    既に外部データが付与済みの行は上書きしない。

    Args:
        df: date 列を持つ DataFrame
        lat: 施設の緯度
        lon: 施設の経度

    Returns:
        外部データ列が補完された DataFrame
    """
    df = df.copy()

    ext_cols = ["wind_speed_max", "wind_direction", "precipitation", "pressure_msl",
                "wave_height_max", "wave_direction", "wave_period_max", "swell_height_max",
                "moon_phase", "is_holiday"]

    for col in ext_cols:
        if col not in df.columns:
            df[col] = np.nan

    missing_mask = df["wind_speed_max"].isna()
    if not missing_mask.any():
        print("  外部データ: 補完不要")
        return df

    missing_dates = df.loc[missing_mask, "date"]
    start = missing_dates.min().strftime("%Y-%m-%d")
    end = missing_dates.max().strftime("%Y-%m-%d")
    print(f"  外部データ補完: {start} ~ {end} ({missing_mask.sum()}日)")

    # 気象データ
    try:
        weather_df = fetch_weather(start, end, lat, lon)
        df = df.set_index("date")
        weather_df = weather_df.set_index("date")
        df.update(weather_df, overwrite=False)
        df = df.reset_index()
        print(f"    気象データ: {len(weather_df)}日分取得")
    except Exception as e:
        print(f"    気象データ取得失敗: {e}")

    time.sleep(1)

    # 海洋データ
    try:
        marine_df = fetch_marine(start, end, lat, lon)
        df = df.set_index("date")
        marine_df = marine_df.set_index("date")
        df.update(marine_df, overwrite=False)
        df = df.reset_index()
        print(f"    海洋データ: {len(marine_df)}日分取得")
    except Exception as e:
        print(f"    海洋データ取得失敗: {e}")

    # 月齢
    try:
        moon_missing = df["moon_phase"].isna()
        if moon_missing.any():
            df.loc[moon_missing, "moon_phase"] = compute_moon_phase(df.loc[moon_missing, "date"])
            print("    月齢: 計算完了")
    except Exception as e:
        print(f"    月齢計算失敗: {e}")

    # 祝日
    try:
        holiday_missing = df["is_holiday"].isna()
        if holiday_missing.any():
            holidays = fetch_holidays()
            df.loc[holiday_missing, "is_holiday"] = (
                df.loc[holiday_missing, "date"].dt.strftime("%Y-%m-%d").isin(holidays).astype(int)
            )
            print(f"    祝日: {len(holidays)}日分取得")
    except Exception as e:
        print(f"    祝日取得失敗: {e}")

    return df
