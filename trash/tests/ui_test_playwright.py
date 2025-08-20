#!/usr/bin/env python3
"""
Playwright UI テストスクリプト - フィルタ条件の行間確認用
"""

import subprocess
import time
import os
import sys
from playwright.sync_api import Playwright, sync_playwright
from pathlib import Path

def run_streamlit_app():
    """Streamlitアプリを起動"""
    try:
        # Streamlitアプリを起動（フルパス使用）
        process = subprocess.Popen(
            ["/Users/tchytky/Library/Python/3.9/bin/streamlit", "run", "app.py", "--server.port=8501", "--server.headless=true"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/Users/tchytky/Desktop/business-data-processor"
        )
        
        # アプリが起動するまで待機
        time.sleep(5)
        
        print("✅ Streamlitアプリを起動しました")
        return process
    except Exception as e:
        print(f"❌ Streamlitアプリの起動に失敗: {e}")
        return None

def test_ui_with_playwright(playwright: Playwright):
    """PlaywrightでUIテスト実行"""
    browser = playwright.chromium.launch(headless=False)  # ブラウザを表示
    context = browser.new_context(viewport={'width': 1920, 'height': 1080})
    page = context.new_page()
    
    try:
        print("🌐 ブラウザでアプリにアクセス中...")
        page.goto("http://localhost:8501")
        
        # ページの読み込み待機
        page.wait_for_selector("h1", timeout=10000)
        print("✅ ページが読み込まれました")
        
        # サイドバーのミライル契約者ボタンをクリック
        print("🔄 ミライル契約者（除外パターン）ボタンをクリック...")
        mirail_button = page.locator("text=契約者（10,000円を除外するパターン）").first
        mirail_button.click()
        
        # 少し待機してページの更新を待つ
        time.sleep(2)
        print("✅ ページ更新待機完了")
        
        # スクリーンショット取得（デスクトップに保存）
        screenshot_path = "/Users/tchytky/Desktop/ui_test_filter_conditions.png"
        page.screenshot(path=screenshot_path, full_page=True)
        print(f"📸 スクリーンショット保存: {screenshot_path}")
        
        # フィルタ条件の各行を取得して行間を確認
        print("\n🔍 フィルタ条件の表示状況:")
        
        # フィルタ条件の各項目を確認
        filter_items = [
            "委託先法人ID → 空白&5",
            "入金予定日 → 前日以前とNaN",
            "回収ランク → 「弁護士介入」除外",
            "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
            "TEL携帯 → 必須"
        ]
        
        for item in filter_items:
            try:
                element = page.locator(f"text={item}").first
                if element.is_visible():
                    bounding_box = element.bounding_box()
                    if bounding_box:
                        print(f"  ✅ {item[:20]}... - Y座標: {bounding_box['y']:.1f}")
                    else:
                        print(f"  ⚠️ {item[:20]}... - 表示されているが位置情報取得不可")
                else:
                    print(f"  ❌ {item[:20]}... - 非表示")
            except Exception as e:
                print(f"  ❌ {item[:20]}... - エラー: {e}")
        
        # 3秒間表示を維持
        print("\n⏱️ 3秒間表示を維持...")
        time.sleep(3)
        
        print("✅ UIテスト完了")
        
    except Exception as e:
        print(f"❌ UIテストエラー: {e}")
        # エラー時もスクリーンショットを取得
        try:
            error_screenshot = "/Users/tchytky/Desktop/ui_test_error.png"
            page.screenshot(path=error_screenshot, full_page=True)
            print(f"📸 エラー時スクリーンショット: {error_screenshot}")
        except:
            pass
    
    finally:
        browser.close()

def main():
    """メイン関数"""
    print("🚀 Playwright UI テスト開始")
    print("=" * 50)
    
    # Streamlitアプリ起動
    streamlit_process = run_streamlit_app()
    
    if not streamlit_process:
        print("❌ Streamlitアプリが起動できませんでした")
        return
    
    try:
        # Playwrightテスト実行
        with sync_playwright() as playwright:
            test_ui_with_playwright(playwright)
    
    finally:
        # Streamlitプロセス終了
        print("🛑 Streamlitアプリを終了中...")
        streamlit_process.terminate()
        streamlit_process.wait(timeout=10)
        print("✅ アプリを終了しました")

if __name__ == "__main__":
    main()