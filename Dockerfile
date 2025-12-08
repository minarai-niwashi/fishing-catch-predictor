# AWS Lambda用Dockerfile
# Python 3.12ランタイムを使用
#
# このDockerイメージは3つのLambda関数で共有されます:
# 1. fishing-catch-predictor: 予測Lambda（デフォルト）
# 2. fishing-data-updater: 日次データ更新Lambda
# 3. fishing-data-initial-setup: 初回セットアップLambda

FROM public.ecr.aws/lambda/python:3.12

# 作業ディレクトリを設定
WORKDIR ${LAMBDA_TASK_ROOT}

# requirements.txtをコピーして依存パッケージをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY src/ ${LAMBDA_TASK_ROOT}/src/

# デフォルトのLambda関数ハンドラ（予測Lambda）
# 他のLambda関数では --handler オプションで上書きされます
CMD ["src.lambda_function.main.lambda_handler"]
