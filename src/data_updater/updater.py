#!/usr/bin/env python3
"""
fishing_data.csv更新Lambda関数

data-daily-scraiping-chokaバケットのS3イベント（ObjectCreated:Put）をトリガーに、
該当施設のfishing_data.csvを増分更新する。
S3イベントのキーから施設名を自動判別し、1施設ずつ処理する。
"""

import io
import json
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

import boto3
import pandas as pd

from external_data import enrich_missing_external_data
from facility_config import FACILITIES as FACILITY_CONFIGS

# 環境変数
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'data-daily-scraiping-choka')
DEST_BUCKET = os.environ.get('DEST_BUCKET', 'fishing-catch-predictor')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')


def load_existing_fishing_data(s3_client, bucket: str, facility: str) -> pd.DataFrame:
    """
    既存のfishing_data.csvを読み込む

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        facility: 施設名

    Returns:
        DataFrame: 既存データ（存在しない場合は空のDataFrame）
    """
    key = f'data/{facility}/fishing_data.csv'
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        df['date'] = pd.to_datetime(df['date'])
        print(f"  ✓ 既存データ読み込み: {len(df)}行 (最終日: {df['date'].max().date()})")
        return df
    except s3_client.exceptions.NoSuchKey:
        print("  ⚠ fishing_data.csvが存在しません。新規作成します。")
        return pd.DataFrame(columns=['date', 'aji_count', 'visitors', 'water_temp', 'weather'])
    except Exception as e:
        print(f"  ⚠ 既存データ読み込みエラー: {e}。新規作成します。")
        return pd.DataFrame(columns=['date', 'aji_count', 'visitors', 'water_temp', 'weather'])


def parse_daily_data(
    s3_client,
    bucket: str,
    facility: str,
    date_obj: datetime
) -> Optional[dict]:
    """
    指定日のデータを読み込んでパース

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        facility: 施設名（honmoku/daikoku）
        date_obj: 日付

    Returns:
        dict: パース済みデータ（date, aji_count, visitors, water_temp, weather）
              データが存在しない場合はNone
    """
    date_str = date_obj.strftime('%Y-%m-%d')
    base_prefix = f"data/{facility}/{date_str}"

    try:
        # head.csvを読み込み
        head_key = f"{base_prefix}/head.csv"
        head_response = s3_client.get_object(Bucket=bucket, Key=head_key)
        head_df = pd.read_csv(io.BytesIO(head_response['Body'].read()))

        if len(head_df) == 0:
            print(f"    ⚠ {date_str}: head.csvにデータがありません")
            return None

        # 天気・水温・来場者数を取得
        row = head_df.iloc[0]
        weather = row.get('天気', None)
        water_temp_str = row.get('水温', None)

        # 来場者数と入場者数の両方に対応
        visitors_str = row.get('来場者数') if '来場者数' in row.index else row.get('入場者数', None)

        # 水温をパース
        water_temp = None
        if pd.notna(water_temp_str):
            match = re.search(r'([\d.]+)', str(water_temp_str))
            if match:
                water_temp = float(match.group(1))

        # 来場者数をパース
        visitors = None
        if pd.notna(visitors_str):
            match = re.search(r'(\d+)', str(visitors_str))
            if match:
                visitors = int(match.group(1))

        # body.csvを読み込み
        body_key = f"{base_prefix}/body.csv"
        body_response = s3_client.get_object(Bucket=bucket, Key=body_key)
        body_df = pd.read_csv(io.BytesIO(body_response['Body'].read()))

        # アジの合計を取得
        aji_count = 0
        for _, fish_row in body_df.iterrows():
            fish_name = fish_row.get('魚', None)
            if fish_name == 'アジ':
                count_str = fish_row.get('合計', None)
                if pd.notna(count_str):
                    try:
                        aji_count = int(count_str)
                    except ValueError:
                        match = re.search(r'(\d+)', str(count_str))
                        if match:
                            aji_count = int(match.group(1))
                break

        return {
            'date': date_obj,
            'aji_count': aji_count,
            'visitors': visitors,
            'water_temp': water_temp,
            'weather': weather
        }

    except s3_client.exceptions.NoSuchKey:
        print(f"    ⚠ {date_str}: データファイルが存在しません")
        return None
    except Exception as e:
        print(f"    ⚠ {date_str}: パースエラー: {e}")
        return None


def update_fishing_data(
    s3_client,
    source_bucket: str,
    dest_bucket: str,
    facility: str,
    target_date: datetime = None
) -> Tuple[pd.DataFrame, int]:
    """
    fishing_data.csvを増分更新

    Args:
        s3_client: boto3 S3クライアント
        source_bucket: ソースバケット（data-daily-scraiping-choka）
        dest_bucket: 保存先バケット（fishing-catch-predictor）
        facility: 施設名
        target_date: 更新対象日（指定しない場合は前日）

    Returns:
        tuple: (更新後のDataFrame, 追加された行数)
    """
    # 既存データを読み込み
    df_existing = load_existing_fishing_data(s3_client, dest_bucket, facility)

    # 更新対象日を決定
    if target_date is None:
        target_date = datetime.now().date() - timedelta(days=1)
    else:
        target_date = target_date.date()

    # 既存データの最終日を確認
    if len(df_existing) > 0:
        last_date = df_existing['date'].max().date()

        # 既にデータが存在する場合
        if target_date <= last_date:
            print(f"  ⚠ {target_date}のデータは既に存在します（最終日: {last_date}）")
            return df_existing, 0

        print(f"  📅 更新対象: {last_date + timedelta(days=1)} 〜 {target_date}")

        # 最終日の翌日から対象日までを更新
        current_date = last_date + timedelta(days=1)
    else:
        print(f"  📅 新規作成: {target_date}のデータから開始")
        current_date = target_date

    # 新しいデータを収集
    new_data = []
    while current_date <= target_date:
        date_obj = datetime.combine(current_date, datetime.min.time())
        print(f"    処理中: {current_date}...")

        data_entry = parse_daily_data(s3_client, source_bucket, facility, date_obj)
        if data_entry is not None:
            new_data.append(data_entry)
            print(f"      ✓ アジ: {data_entry['aji_count']}匹, 来場者: {data_entry['visitors']}人")

        current_date += timedelta(days=1)

    # 新しいデータを追加
    if len(new_data) > 0:
        df_new = pd.DataFrame(new_data)
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)
        df_updated = df_updated.sort_values('date').reset_index(drop=True)
        print(f"  ✓ {len(new_data)}行を追加しました")
    else:
        df_updated = df_existing
        print("  ⚠ 追加するデータがありません")

    # 外部データが未付与の行を補完
    if len(df_updated) > 0 and facility in FACILITY_CONFIGS:
        fac = FACILITY_CONFIGS[facility]
        df_updated = enrich_missing_external_data(df_updated, fac["lat"], fac["lon"])

    return df_updated, len(new_data)


def save_fishing_data(s3_client, bucket: str, df: pd.DataFrame, facility: str) -> None:
    """
    fishing_data.csvをS3に保存

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        df: 保存するDataFrame
        facility: 施設名
    """
    key = f'data/{facility}/fishing_data.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    print(f"  ✓ S3に保存: s3://{bucket}/{key}")


def _extract_facility_from_s3_event(event: dict) -> Tuple[str, Optional[datetime]]:
    """S3イベントからfacility名と日付を抽出する.

    S3キーの形式: data/{facility}/{YYYY-MM-DD}/body.csv

    Args:
        event: S3イベント

    Returns:
        tuple: (施設名, 日付 or None)

    Raises:
        ValueError: S3キーから施設名を抽出できない場合
    """
    record = event['Records'][0]
    s3_key = record['s3']['object']['key']
    print(f"  S3キー: {s3_key}")

    # data/{facility}/{YYYY-MM-DD}/body.csv からパース
    match = re.match(r'^data/([^/]+)/(\d{4}-\d{2}-\d{2})/', s3_key)
    if not match:
        raise ValueError(f"S3キーから施設名を抽出できません: {s3_key}")

    facility = match.group(1)
    date_str = match.group(2)
    target_date = datetime.strptime(date_str, '%Y-%m-%d')

    return facility, target_date


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント

    S3イベント（ObjectCreated:Put）をトリガーに、
    該当施設のfishing_data.csvを増分更新する。

    環境変数:
        SOURCE_BUCKET: スクレイピングデータのバケット (default: data-daily-scraiping-choka)
        DEST_BUCKET: 保存先バケット (default: fishing-catch-predictor)

    Args:
        event: S3イベント（Records[0].s3.object.key から施設名と日付を取得）
        context: Lambda コンテキスト

    Returns:
        dict: レスポンス (施設の更新結果)
    """
    try:
        print("=" * 80)
        print("fishing_data.csv 増分更新")
        print("=" * 80)

        # S3イベントから施設名と日付を抽出
        facility, target_date = _extract_facility_from_s3_event(event)

        if facility not in FACILITY_CONFIGS:
            raise ValueError(f"未知の施設名: {facility}")

        print(f"  施設: {facility}, 対象日: {target_date.date()}")

        # S3クライアント
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        print(f"\n--- {facility} ---")

        df_updated, rows_added = update_fishing_data(
            s3_client=s3_client,
            source_bucket=SOURCE_BUCKET,
            dest_bucket=DEST_BUCKET,
            facility=facility,
            target_date=target_date
        )

        # S3に保存
        if rows_added > 0:
            save_fishing_data(s3_client, DEST_BUCKET, df_updated, facility)

        result = {
            'rows_added': rows_added,
            'total_rows': len(df_updated),
            'last_date': df_updated['date'].max().strftime('%Y-%m-%d') if len(df_updated) > 0 else None
        }

        # 結果サマリー
        print("\n" + "=" * 80)
        print("✅ 更新完了")
        print(f"  {facility}: +{result['rows_added']}行 (合計{result['total_rows']}行, 最終日: {result['last_date']})")
        print("=" * 80)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'{facility} データ更新完了',
                'facility': facility,
                'result': result
            }, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        error_message = f"データ更新でエラーが発生しました: {str(e)}"
        print(f"ERROR: {error_message}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message
            }, ensure_ascii=False, indent=2)
        }
