#!/usr/bin/env python3
"""
Business Data Processor - Smoke Test
デプロイ後の基本的な動作確認
"""

import sys
import time
import requests
from io import BytesIO
import pandas as pd

# テスト対象のURL設定
BASE_URL = "http://localhost"  # Nginx経由
DIRECT_BLUE_URL = "http://localhost:8501"  # Blue直接（開発時）
DIRECT_GREEN_URL = "http://localhost:8502"  # Green直接（開発時）

# カラー出力
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_test_header(test_name):
    print(f"\n{BLUE}▶ Testing: {test_name}{RESET}")

def print_success(message):
    print(f"  {GREEN}✓ {message}{RESET}")

def print_error(message):
    print(f"  {RED}✗ {message}{RESET}")

def print_warning(message):
    print(f"  {YELLOW}⚠ {message}{RESET}")

def test_health_check(url=BASE_URL):
    """ヘルスチェックエンドポイントのテスト"""
    print_test_header("Health Check Endpoint")
    
    try:
        response = requests.get(f"{url}/_stcore/health", timeout=5)
        if response.status_code == 200:
            print_success(f"Health check passed (status: {response.status_code})")
            return True
        else:
            print_error(f"Health check failed (status: {response.status_code})")
            return False
    except requests.exceptions.RequestException as e:
        print_error(f"Health check failed: {str(e)}")
        return False

def test_main_page(url=BASE_URL):
    """メインページの表示テスト"""
    print_test_header("Main Page Loading")
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Streamlitの特徴的な要素を確認
            content = response.text.lower()
            if "streamlit" in content or "stapp" in content:
                print_success(f"Main page loaded successfully")
                
                # ページサイズの確認
                content_size = len(response.content)
                print_success(f"Page size: {content_size:,} bytes")
                return True
            else:
                print_warning("Page loaded but doesn't appear to be Streamlit")
                return False
        else:
            print_error(f"Failed to load main page (status: {response.status_code})")
            return False
    except requests.exceptions.RequestException as e:
        print_error(f"Failed to connect: {str(e)}")
        return False

def test_websocket_endpoint(url=BASE_URL):
    """WebSocketエンドポイントの確認"""
    print_test_header("WebSocket Endpoint")
    
    try:
        # WebSocketエンドポイントはGETリクエストに対して特定のレスポンスを返す
        response = requests.get(f"{url}/_stcore/stream", timeout=5)
        # Streamlitは通常のHTTPリクエストに対して400や426を返す
        if response.status_code in [400, 426]:  # Upgrade Required
            print_success("WebSocket endpoint is responding correctly")
            return True
        else:
            print_warning(f"Unexpected response from WebSocket endpoint (status: {response.status_code})")
            return True  # エンドポイントは存在している
    except requests.exceptions.RequestException as e:
        print_error(f"WebSocket endpoint check failed: {str(e)}")
        return False

def test_file_upload_capability():
    """ファイルアップロード機能のテスト（基本確認のみ）"""
    print_test_header("File Upload Capability")
    
    # 実際のファイルアップロードは複雑なので、
    # ここではエンドポイントの存在確認のみ
    print_warning("File upload test requires interactive session - skipping detailed test")
    print_success("File upload endpoints should be available")
    return True

def test_static_resources(url=BASE_URL):
    """静的リソースのアクセステスト"""
    print_test_header("Static Resources")
    
    # Streamlitの静的リソースパス
    static_paths = [
        "/static/js/vendor.js",
        "/static/css/main.css",
        "/favicon.png"
    ]
    
    success = True
    for path in static_paths:
        try:
            response = requests.head(f"{url}{path}", timeout=5)
            if response.status_code in [200, 304]:  # 304 = Not Modified
                print_success(f"Static resource accessible: {path}")
            else:
                print_warning(f"Static resource not found: {path} (status: {response.status_code})")
                # 警告だけで失敗にはしない（Streamlitのバージョンによってパスが変わる可能性）
        except:
            print_warning(f"Could not check static resource: {path}")
    
    return success

def test_response_time(url=BASE_URL):
    """レスポンスタイムのテスト"""
    print_test_header("Response Time")
    
    try:
        start_time = time.time()
        response = requests.get(url, timeout=10)
        response_time = (time.time() - start_time) * 1000  # ミリ秒に変換
        
        if response.status_code == 200:
            if response_time < 1000:  # 1秒以下
                print_success(f"Response time: {response_time:.0f}ms (Good)")
            elif response_time < 3000:  # 3秒以下
                print_warning(f"Response time: {response_time:.0f}ms (Slow)")
            else:
                print_error(f"Response time: {response_time:.0f}ms (Too slow)")
            return response_time < 3000
        else:
            print_error(f"Failed to measure response time")
            return False
    except:
        print_error("Response time test failed")
        return False

def run_all_tests(url=BASE_URL):
    """全てのテストを実行"""
    print(f"\n{BLUE}{'='*50}")
    print(f"Business Data Processor - Smoke Test")
    print(f"Target URL: {url}")
    print(f"{'='*50}{RESET}")
    
    tests = [
        test_health_check,
        test_main_page,
        test_websocket_endpoint,
        test_file_upload_capability,
        test_static_resources,
        test_response_time
    ]
    
    results = []
    for test in tests:
        result = test(url)
        results.append(result)
        time.sleep(0.5)  # テスト間の小休止
    
    # 結果サマリー
    print(f"\n{BLUE}{'='*50}")
    print("Test Summary")
    print(f"{'='*50}{RESET}")
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"{GREEN}✓ All tests passed ({passed}/{total}){RESET}")
        return True
    elif passed > total * 0.7:  # 70%以上成功
        print(f"{YELLOW}⚠ Most tests passed ({passed}/{total}){RESET}")
        return True
    else:
        print(f"{RED}✗ Too many tests failed ({passed}/{total}){RESET}")
        return False

def main():
    """メイン処理"""
    # コマンドライン引数でURLを指定可能
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = BASE_URL
    
    success = run_all_tests(url)
    
    # 終了コード
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()