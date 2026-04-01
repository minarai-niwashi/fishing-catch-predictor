# 釣果予測システム (Fishing Catch Predictor)

機械学習を使って「明日釣りに行くべきか」を判断するシステム

対象施設: **本牧海釣り施設** / **大黒海釣り施設**

## プロジェクト構成

```
fishing-catch-predictor/
├── template.yaml                 # SAM テンプレート
├── samconfig.toml                # SAM デプロイ設定
├── src/
│   ├── predictor/                # Lambda (釣果予測)
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── handler.py            # Lambda ハンドラー (両施設の予測 + SNS通知)
│   │   ├── predictor.py          # FishingPredictor (推論)
│   │   ├── data_loader.py        # S3データ・モデル読み込み
│   │   ├── features.py           # 特徴量エンジニアリング
│   │   └── facility_config.py    # 施設設定 (座標等)
│   │
│   └── data_updater/             # Lambda (データ更新 + 外部データ取得)
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── updater.py            # 日次更新ハンドラー (S3イベント駆動、1施設ずつ)
│       ├── initial_setup.py      # 初回セットアップハンドラー
│       ├── external_data.py      # 外部データ取得 (気象/海洋/月齢/祝日)
│       └── facility_config.py    # 施設設定 (座標等)
│
├── pyproject.toml
└── README.md
```

## デプロイ

SAM + Docker イメージ方式。WSL2 (Docker Engine) 上でビルド・デプロイする。

```bash
sam build && sam deploy
```

## Lambda関数

SAM スタック `fishing-catch-predictor` で管理。

| 関数 | 目的 | トリガー |
|------|------|----------|
| **PredictorFunction** | 翌日の釣行判定予測 (両施設) + SNS通知 | EventBridge (毎日 JST 21:30) |
| **DataUpdaterFunction** | 前日分データ + 外部データの追加 (1施設ずつ) | S3イベント (body.csv の Put) |
| **InitialSetupFunction** | 全履歴データの初期取り込み (両施設) | 手動実行 |
