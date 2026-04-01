#!/usr/bin/env python3
"""
fishing_data.csv初期セットアップスクリプト

data-daily-scraiping-chokaバケットから全履歴データを読み込み、
fishing-catch-predictorバケットに初期データを作成する。
FACILITIES環境変数で指定された全施設を1回のLambda実行で処理する。

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
FACILITIES = os.environ.get('FACILITIES', 'honmoku,daikoku')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')


def list_all_date_folders(s3_client, bucket: str, facility: str) -> List[str]:
    """S3から全ての日付フォルダをリスト."""
    prefix = f"data/{facility}/"
    paginator = s3_client.get_paginator('list_objects_v2')

    date_folders = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for obj in page['CommonPrefixes']:
                folder_path = obj['Prefix']
                date_str = folder_path.rstrip('/').split('/')[-1]
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                    date_folders.add(date_str)

    return sorted(list(date_folders))


def parse_daily_data(
    s3_client,
    bucket: str,
    facility: str,
    date_str: str
) -> Optional[dict]:
    """指定日のデータを読み込んでパース."""
    base_prefix = f"data/{facility}/{date_str}"

    try:
        head_key = f"{base_prefix}/head.csv"
        head_response = s3_client.get_object(Bucket=bucket, Key=head_key)
        head_df = pd.read_csv(io.BytesIO(head_response['Body'].read()))

        if len(head_df) == 0:
            return None

        row = head_df.iloc[0]
        weather = row.get('天気', None)
        water_temp_str = row.get('水温', None)
        visitors_str = row.get('来場者数') if '来場者数' in row.index else row.get('入場者数', None)

        water_temp = None
        if pd.notna(water_temp_str):
            match = re.search(r'([\d.]+)', str(water_temp_str))
            if match:
                water_temp = float(match.group(1))

        visitors = None
        if pd.notna(visitors_str):
            match = re.search(r'(\d+)', str(visitors_str))
            if match:
                visitors = int(match.group(1))

        body_key = f"{base_prefix}/body.csv"
        body_response = s3_client.get_object(Bucket=bucket, Key=body_key)
        body_df = pd.read_csv(io.BytesIO(body_response['Body'].read()))

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
        print(f"    ⚠ {date_str}: エラー: {e}")
        return None


def create_initial_fishing_data(
    s3_client,
    source_bucket: str,
    facility: str
) -> pd.DataFrame:
    """全履歴データからfishing_data.csvを初期生成."""
    print("  📂 日付フォルダを検索中...")
    date_folders = list_all_date_folders(s3_client, source_bucket, facility)
    print(f"  ✓ {len(date_folders)}個の日付フォルダを発見")

    if len(date_folders) == 0:
        raise RuntimeError(f"{facility}: 日付フォルダが見つかりませんでした")

    print(f"  📅 データ範囲: {date_folders[0]} 〜 {date_folders[-1]}")

    all_data = []
    success_count = 0
    error_count = 0

    for i, date_str in enumerate(date_folders, 1):
        if i % 50 == 0 or i == len(date_folders):
            print(f"    進捗: {i}/{len(date_folders)} ({i/len(date_folders)*100:.1f}%)")

        data_entry = parse_daily_data(s3_client, source_bucket, facility, date_str)
        if data_entry is not None:
            all_data.append(data_entry)
            success_count += 1
        else:
            error_count += 1

    df = pd.DataFrame(all_data)
    df = df.sort_values('date').reset_index(drop=True)

    print(f"  ✓ データ収集完了: 成功={success_count}日, エラー={error_count}日")
    return df


def lambda_handler(event, context):
    """
    Lambda関数のエントリーポイント（初回セットアップ用）

    環境変数:
        SOURCE_BUCKET: スクレイピングデータのバケット (default: data-daily-scraiping-choka)
        DEST_BUCKET: 保存先バケット (default: fishing-catch-predictor)
        FACILITIES: カンマ区切りの施設名 (default: honmoku,daikoku)

    注意: 全履歴を読み込むため、コストがかかります。初回のみ実行してください。
    """
    try:
        print("=" * 80)
        print("fishing_data.csv 初期セットアップ")
        print("⚠ 警告: 全履歴データを読み込みます（初回のみ実行）")
        print("=" * 80)

        facility_names = [f.strip() for f in FACILITIES.split(',')]
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        results = {}

        for facility in facility_names:
            print(f"\n--- {facility} ---")

            try:
                df = create_initial_fishing_data(
                    s3_client=s3_client,
                    source_bucket=SOURCE_BUCKET,
                    facility=facility
                )

                # 外部データ補完
                from external_data import enrich_missing_external_data
                from facility_config import FACILITIES as FAC_CONFIGS
                fac = FAC_CONFIGS[facility]
                df = enrich_missing_external_data(df, fac["lat"], fac["lon"])

                # S3に保存
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)

                dest_key = f'data/{facility}/fishing_data.csv'
                s3_client.put_object(
                    Bucket=DEST_BUCKET,
                    Key=dest_key,
                    Body=csv_buffer.getvalue()
                )
                print(f"  ✓ S3に保存: s3://{DEST_BUCKET}/{dest_key}")

                results[facility] = {
                    'total_rows': len(df),
                    'date_range': {
                        'start': df['date'].min().strftime('%Y-%m-%d'),
                        'end': df['date'].max().strftime('%Y-%m-%d')
                    }
                }
            except Exception as e:
                print(f"  ERROR: {facility} のセットアップに失敗: {e}")
                results[facility] = {'error': str(e)}

        print("\n" + "=" * 80)
        print("✅ 初期セットアップ完了")
        for facility, r in results.items():
            if 'error' in r:
                print(f"  {facility}: エラー - {r['error']}")
            else:
                print(f"  {facility}: {r['total_rows']}行 ({r['date_range']['start']} ~ {r['date_range']['end']})")
        print("=" * 80)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '初期セットアップ完了',
                'results': results
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
