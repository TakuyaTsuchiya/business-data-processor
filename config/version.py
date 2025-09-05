"""
Business Data Processor バージョン管理

このファイルはアプリケーション全体で使用されるバージョン情報を一元管理します。
"""

# アプリケーションバージョン
APP_VERSION = "2.3.0"

# Dockerイメージバージョン
DOCKER_VERSION = "2.3.0-docker"

# バージョン履歴
VERSION_HISTORY = {
    "2.3.0": {
        "date": "2025-01-09",
        "changes": [
            "Service層の充実（logger.py作成、autocall/smsサービスにログ機能追加）",
            "インフラ層の拡張（EncodingHandler、FileWriter）",
            "エラーハンドリングの統一（domain/exceptions.py）",
            "設定管理の改善（バージョン一元管理）"
        ]
    },
    "2.2.0": {
        "date": "2024-12-01",
        "changes": [
            "プラザ残債更新機能の実装",
            "プラザ交渉履歴で入金額0円のデータを除外"
        ]
    },
    "2.1.0": {
        "date": "2024-11-01",
        "changes": [
            "Docker対応版リリース"
        ]
    }
}

def get_version():
    """現在のアプリケーションバージョンを返す"""
    return APP_VERSION

def get_docker_version():
    """現在のDockerイメージバージョンを返す"""
    return DOCKER_VERSION

def get_version_info():
    """バージョン情報の詳細を返す"""
    return {
        "app_version": APP_VERSION,
        "docker_version": DOCKER_VERSION,
        "latest_changes": VERSION_HISTORY.get(APP_VERSION, {}).get("changes", [])
    }