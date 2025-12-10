# 釣果予測システム (Fishing Catch Predictor)

機械学習を使って「明日釣りに行くべきか」を判断するAWS Lambda関数

> **このリポジトリについて**
> AWS Lambdaデプロイ用のコードを管理します。モデル学習コードは含まれません（ローカル環境で実行しS3にアップロード）。

## 📁 プロジェクト構成

```
fishing-catch-predictor/
├── src/
│   ├── predictor/              # 予測コアモジュール
│   │   ├── __init__.py
│   │   ├── features.py         # 特徴量生成
│   │   ├── data_loader.py      # S3データ読み込み
│   │   └── inference.py        # 予測ロジック
│   ├── lambda_function/        # 予測Lambda関数
│   │   ├── __init__.py
│   │   └── main.py             # Lambda handler（予測）
│   └── data_updater/           # データ更新Lambda関数
│       ├── __init__.py
│       ├── main.py             # 日次更新ハンドラ
│       └── initial_setup.py    # 初回セットアップ
├── requirements.txt            # Python依存パッケージ
├── Dockerfile                  # Lambda用Dockerイメージ
├── .gitignore
└── README.md
```

## 🎯 Lambda関数

| 関数名 | 目的 | ハンドラー |
|--------|------|-----------|
| **fishing-catch-predictor** | 翌日釣果の予測 | `src.lambda_function.main.lambda_handler` |
| **fishing-data-updater** | 前日分データの追加 | `src.data_updater.main.lambda_handler` |
| **fishing-data-initial-setup** | 全履歴データの初期取り込み | `src.data_updater.initial_setup.lambda_handler` |
