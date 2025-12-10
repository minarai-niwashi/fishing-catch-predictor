#!/usr/bin/env python3
"""
fishing_data.csv更新Lambda関数

data-daily-scraiping-chokaバケットから新しいデータを読み込み、
fishing-catch-predictorバケットのfishing_data.csvを増分更新する
"""

import io
import json
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

import boto3
import pandas as pd


# 環境変数
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'data-daily-scraiping-choka')
DEST_BUCKET = os.environ.get('DEST_BUCKET', 'fishing-catch-predictor')
FACILITY = os.environ.get('FACILITY', 'honmoku')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')


def load_existing_fishing_data(s3_client, bucket: str, key: str = 'data/fishing_data.csv') -> pd.DataFrame:
    """
    既存のfishing_data.csvを読み込む

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        key: S3キー

    Returns:
        DataFrame: 既存データ（存在しない場合は空のDataFrame）
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        df['date'] = pd.to_datetime(df['date'])
        print(f"✓ 既存データ読み込み: {len(df)}行 (最終日: {df['date'].max().date()})")
        return df
    except s3_client.exceptions.NoSuchKey:
        print("⚠ fishing_data.csvが存在しません。新規作成します。")
        return pd.DataFrame(columns=['date', 'aji_count', 'visitors', 'water_temp', 'weather'])
    except Exception as e:
        print(f"⚠ 既存データ読み込みエラー: {e}。新規作成します。")
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
            print(f"  ⚠ {date_str}: head.csvにデータがありません")
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
        print(f"  ⚠ {date_str}: データファイルが存在しません")
        return None
    except Exception as e:
        print(f"  ⚠ {date_str}: パースエラー: {e}")
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
    df_existing = load_existing_fishing_data(s3_client, dest_bucket)

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
            print(f"⚠ {target_date}のデータは既に存在します（最終日: {last_date}）")
            return df_existing, 0

        print(f"📅 更新対象: {last_date + timedelta(days=1)} 〜 {target_date}")

        # 最終日の翌日から対象日までを更新
        current_date = last_date + timedelta(days=1)
    else:
        print(f"📅 新規作成: {target_date}のデータから開始")
        current_date = target_date

    # 新しいデータを収集
    new_data = []
    while current_date <= target_date:
        date_obj = datetime.combine(current_date, datetime.min.time())
        print(f"  処理中: {current_date}...")

        data_entry = parse_daily_data(s3_client, source_bucket, facility, date_obj)
        if data_entry is not None:
            new_data.append(data_entry)
            print(f"    ✓ アジ: {data_entry['aji_count']}匹, 来場者: {data_entry['visitors']}人")

        current_date += timedelta(days=1)

    # 新しいデータを追加
    if len(new_data) > 0:
        df_new = pd.DataFrame(new_data)
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)
        df_updated = df_updated.sort_values('date').reset_index(drop=True)
        print(f"\n✓ {len(new_data)}行を追加しました")
    else:
        df_updated = df_existing
        print("\n⚠ 追加するデータがありません")

    return df_updated, len(new_data)


def save_fishing_data(s3_client, bucket: str, df: pd.DataFrame, key: str = 'data/fishing_data.csv') -> None:
    """
    fishing_data.csvをS3に保存

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        df: 保存するDataFrame
        key: S3キー
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    print(f"✓ S3に保存: s3://{bucket}/{key}")


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント

    Args:
        event: Lambda イベント
            - target_date (optional): 更新対象日（YYYY-MM-DD形式）
              指定しない場合は前日を更新
        context: Lambda コンテキスト

    Returns:
        dict: レスポンス
            - statusCode: HTTPステータスコード
            - body: JSON文字列
                - rows_added: 追加された行数
                - total_rows: 合計行数
                - last_date: 最終日

    Example:
        # 前日のデータを更新
        {}

        # 特定の日付を更新
        {"target_date": "2025-11-13"}
    """
    try:
        print("=" * 80)
        print("fishing_data.csv 増分更新")
        print("=" * 80)

        # イベントからパラメータ取得
        target_date = None
        if event and 'target_date' in event:
            target_date = datetime.strptime(event['target_date'], '%Y-%m-%d')

        # S3クライアント
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        # データ更新
        df_updated, rows_added = update_fishing_data(
            s3_client=s3_client,
            source_bucket=SOURCE_BUCKET,
            dest_bucket=DEST_BUCKET,
            facility=FACILITY,
            target_date=target_date
        )

        # S3に保存
        if rows_added > 0:
            save_fishing_data(s3_client, DEST_BUCKET, df_updated)

        # 結果サマリー
        print("\n" + "=" * 80)
        print("✅ 更新完了")
        print(f"  追加行数: {rows_added}")
        print(f"  合計行数: {len(df_updated)}")
        if len(df_updated) > 0:
            print(f"  最終日: {df_updated['date'].max().date()}")
        print("=" * 80)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'データ更新完了',
                'rows_added': rows_added,
                'total_rows': len(df_updated),
                'last_date': df_updated['date'].max().strftime('%Y-%m-%d') if len(df_updated) > 0 else None
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


# ローカルテスト用
if __name__ == '__main__':
    test_event = {
        # 'target_date': '2025-11-13'  # オプション
    }
    test_context = {}

    response = lambda_handler(test_event, test_context)
    print(json.dumps(json.loads(response['body']), ensure_ascii=False, indent=2))
