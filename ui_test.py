"""
Streamlit App UI Test with Playwright
StreamlitアプリのUI動作確認とスクリーンショット取得
"""

from playwright.sync_api import sync_playwright
import time
import subprocess
import threading
import sys

def start_streamlit_app():
    """バックグラウンドでStreamlitアプリを起動"""
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", "app.py", 
        "--server.headless", "true",
        "--server.port", "8501"
    ])

def test_streamlit_ui():
    """PlaywrightでStreamlitアプリのUIテスト"""
    with sync_playwright() as p:
        # ブラウザ起動
        browser = p.chromium.launch(headless=False)  # headless=Falseで画面表示
        page = browser.new_page()
        
        print("Streamlitアプリにアクセス中...")
        page.goto("http://localhost:8501")
        
        # ページが読み込まれるまで待機
        page.wait_for_selector("h1")
        
        # スクリーンショット取得
        print("初期画面のスクリーンショット取得...")
        page.screenshot(path="screenshots/01_welcome.png")
        
        # タイトルの確認
        title = page.locator("h1").inner_text()
        print(f"ページタイトル: {title}")
        
        # サイドバーの処理選択テスト
        print("サイドバーメニューのテスト...")
        
        # ミライル契約者を選択
        page.select_option("select", "ミライル契約者（残債除外）")
        time.sleep(2)
        page.screenshot(path="screenshots/02_mirail_selected.png")
        print("ミライル契約者画面を表示")
        
        # フェイス契約者を選択
        page.select_option("select", "フェイス契約者（オートコール用）")
        time.sleep(2)
        page.screenshot(path="screenshots/03_faith_selected.png")
        print("フェイス契約者画面を表示")
        
        # アーク新規登録を選択
        page.select_option("select", "アーク新規登録データ変換")
        time.sleep(2)
        page.screenshot(path="screenshots/04_ark_selected.png")
        print("アーク新規登録画面を表示")
        
        # ウェルカム画面に戻る
        page.select_option("select", "選択してください")
        time.sleep(2)
        page.screenshot(path="screenshots/05_back_to_welcome.png")
        print("ウェルカム画面に戻る")
        
        browser.close()
        print("UIテスト完了！")

def main():
    """メイン処理"""
    # スクリーンショット保存用ディレクトリ作成
    import os
    os.makedirs("screenshots", exist_ok=True)
    
    print("Streamlitアプリ起動中...")
    
    # Streamlitアプリをバックグラウンドで起動
    streamlit_thread = threading.Thread(target=start_streamlit_app, daemon=True)
    streamlit_thread.start()
    
    # アプリの起動を待機
    time.sleep(10)
    
    # UIテスト実行
    test_streamlit_ui()

if __name__ == "__main__":
    main()