#!/usr/bin/env python3
"""
fishing_data.csv初期セットアップスクリプト

data-daily-scraiping-chokaバケットから全履歴データを読み込み、
fishing-catch-predictorバケットに初期データを作成する

注意: 初回のみ実行。全日付フォルダを読み込むため、コストがかかります。
"""

import io
import json
import os
import re
from datetime import datetime
from typing import List, Optional

import boto3
import pandas as pd

# 環境変数
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'data-daily-scraiping-choka')
DEST_BUCKET = os.environ.get('DEST_BUCKET', 'fishing-catch-predictor')
FACILITY = os.environ.get('FACILITY', 'honmoku')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')


def list_all_date_folders(s3_client, bucket: str, facility: str) -> List[str]:
    """
    S3から全ての日付フォルダをリスト

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        facility: 施設名

    Returns:
        List[str]: 日付文字列のリスト（YYYY-MM-DD）
    """
    prefix = f"data/{facility}/"
    paginator = s3_client.get_paginator('list_objects_v2')

    date_folders = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for obj in page['CommonPrefixes']:
                folder_path = obj['Prefix']
                # data/honmoku/YYYY-MM-DD/ から日付を抽出
                date_str = folder_path.rstrip('/').split('/')[-1]
                # YYYY-MM-DD形式かチェック
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                    date_folders.add(date_str)

    return sorted(list(date_folders))


def parse_daily_data(
    s3_client,
    bucket: str,
    facility: str,
    date_str: str
) -> Optional[dict]:
    """
    指定日のデータを読み込んでパース

    Args:
        s3_client: boto3 S3クライアント
        bucket: S3バケット名
        facility: 施設名
        date_str: 日付文字列（YYYY-MM-DD）

    Returns:
        dict: パース済みデータ（date, aji_count, visitors, water_temp, weather）
              データが存在しない場合はNone
    """
    base_prefix = f"data/{facility}/{date_str}"

    try:
        # head.csvを読み込み
        head_key = f"{base_prefix}/head.csv"
        head_response = s3_client.get_object(Bucket=bucket, Key=head_key)
        head_df = pd.read_csv(io.BytesIO(head_response['Body'].read()))

        if len(head_df) == 0:
            return None

        # 天気・水温・来場者数を取得
        row = head_df.iloc[0]
        weather = row.get('天気', None)
        water_temp_str = row.get('水温', None)
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

        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        return {
            'date': date_obj,
            'aji_count': aji_count,
            'visitors': visitors,
            'water_temp': water_temp,
            'weather': weather
        }

    except Exception as e:
        print(f"  ⚠ {date_str}: エラー: {e}")
        return None


def create_initial_fishing_data(
    s3_client,
    source_bucket: str,
    dest_bucket: str,
    facility: str
) -> pd.DataFrame:
    """
    全履歴データからfishing_data.csvを初期生成

    Args:
        s3_client: boto3 S3クライアント
        source_bucket: ソースバケット
        dest_bucket: 保存先バケット
        facility: 施設名

    Returns:
        DataFrame: 生成されたデータ
    """
    print("📂 日付フォルダを検索中...")
    date_folders = list_all_date_folders(s3_client, source_bucket, facility)
    print(f"✓ {len(date_folders)}個の日付フォルダを発見")

    if len(date_folders) == 0:
        raise RuntimeError("日付フォルダが見つかりませんでした")

    print(f"📅 データ範囲: {date_folders[0]} 〜 {date_folders[-1]}")
    print(f"\n⚠ 警告: {len(date_folders) * 3}個のファイルを読み込みます（コストに注意）")
    print("処理を開始しますか？ (Ctrl+Cでキャンセル)")

    # データ収集
    all_data = []
    success_count = 0
    error_count = 0

    for i, date_str in enumerate(date_folders, 1):
        if i % 10 == 0 or i == len(date_folders):
            print(f"  進捗: {i}/{len(date_folders)} ({i/len(date_folders)*100:.1f}%)")

        data_entry = parse_daily_data(s3_client, source_bucket, facility, date_str)

        if data_entry is not None:
            all_data.append(data_entry)
            success_count += 1
        else:
            error_count += 1

    # DataFrameを作成
    df = pd.DataFrame(all_data)
    df = df.sort_values('date').reset_index(drop=True)

    print(f"\n✓ データ収集完了")
    print(f"  成功: {success_count}日")
    print(f"  エラー: {error_count}日")
    print(f"  合計: {len(df)}行")

    return df


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント（初回セットアップ用）

    注意: このLambda関数は初回のみ実行してください。
          全履歴を読み込むため、コストがかかります。

    Args:
        event: Lambda イベント
        context: Lambda コンテキスト

    Returns:
        dict: レスポンス
    """
    try:
        print("=" * 80)
        print("fishing_data.csv 初期セットアップ")
        print("=" * 80)
        print("\n⚠ 警告: 全履歴データを読み込みます（初回のみ実行）\n")

        # S3クライアント
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        # 初期データ生成
        df = create_initial_fishing_data(
            s3_client=s3_client,
            source_bucket=SOURCE_BUCKET,
            dest_bucket=DEST_BUCKET,
            facility=FACILITY
        )

        # データ情報
        print("\n📊 データサマリー:")
        print(f"  データ範囲: {df['date'].min().date()} 〜 {df['date'].max().date()}")
        print(f"  総日数: {len(df)}日")
        print(f"\n  欠損値:")
        print(df.isnull().sum().to_string())

        # S3に保存
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        dest_key = 'data/fishing_data.csv'
        s3_client.put_object(
            Bucket=DEST_BUCKET,
            Key=dest_key,
            Body=csv_buffer.getvalue()
        )

        print(f"\n✓ S3に保存: s3://{DEST_BUCKET}/{dest_key}")
        print("\n" + "=" * 80)
        print("✅ 初期セットアップ完了")
        print("=" * 80)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '初期セットアップ完了',
                'total_rows': len(df),
                'date_range': {
                    'start': df['date'].min().strftime('%Y-%m-%d'),
                    'end': df['date'].max().strftime('%Y-%m-%d')
                }
            }, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        error_message = f"初期セットアップでエラーが発生しました: {str(e)}"
        print(f"ERROR: {error_message}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message
            }, ensure_ascii=False, indent=2)
        }


# ローカルテスト用
if __name__ == '__main__':
    response = lambda_handler({}, {})
    print(json.dumps(json.loads(response['body']), ensure_ascii=False, indent=2))
