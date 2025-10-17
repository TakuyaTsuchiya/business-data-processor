# Business Data Processor v2.1.0 - Docker Image
# マルチステージビルドで最適化されたイメージを作成

# ステージ1: ビルド環境
FROM python:3.11-slim as builder

# 作業ディレクトリ設定
WORKDIR /app

# streamlitユーザーを作成（ビルド段階）
RUN useradd -m -u 1000 streamlit

# streamlitユーザーに切り替え
USER streamlit

# 依存関係ファイルをコピー
COPY requirements.txt .

# Python依存関係を事前ビルド（streamlitユーザーでインストール）
RUN pip install --no-cache-dir --upgrade pip --user \
    && pip install --no-cache-dir --user -r requirements.txt

# ステージ2: 実行環境
FROM python:3.11-slim

# 作者情報
LABEL maintainer="Takuya Tsuchiya"
LABEL version="2.1.0-docker"
LABEL description="統合データ処理システム - 15種類プロセッサー対応（Docker最適化版）"

# 作業ディレクトリ設定
WORKDIR /app

# 必要最小限のシステムパッケージインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    locales \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i '/ja_JP.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen

# 環境変数設定（文字化け対策・最適化）
ENV LANG=ja_JP.UTF-8 \
    LC_ALL=ja_JP.UTF-8 \
    LC_CTYPE=ja_JP.UTF-8 \
    PYTHONIOENCODING=utf-8 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# streamlitユーザーを作成（実行環境）
RUN useradd -m -u 1000 streamlit

# ビルドステージから依存関係をコピー（streamlitユーザーのホームディレクトリへ）
COPY --from=builder /home/streamlit/.local /home/streamlit/.local

# streamlitユーザーのPATHを設定
ENV PATH=/home/streamlit/.local/bin:$PATH

# アプリケーションファイルをコピー
COPY app.py .
COPY processors/ ./processors/
COPY components/ ./components/
COPY screens/ ./screens/
COPY services/ ./services/
COPY config/ ./config/

# 必要なディレクトリを作成
RUN mkdir -p /app/data /app/downloads /app/logs

# アプリケーションディレクトリの権限設定
RUN chown -R streamlit:streamlit /app \
    && chmod -R 755 /app \
    && chmod -R 777 /app/data /app/downloads /app/logs \
    && chown -R streamlit:streamlit /home/streamlit/.local

# ポート公開
EXPOSE 8501

# ヘルスチェック（改善版）
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# ユーザー切り替え
USER streamlit

# アプリケーション起動
CMD ["streamlit", "run", "app.py", "--server.maxUploadSize", "100"]