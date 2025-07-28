"""
Simple Streamlit App UI Test with Playwright
シンプルなUIテストとスクリーンショット取得
"""

from playwright.sync_api import sync_playwright
import time
import os

def test_streamlit_ui():
    """PlaywrightでStreamlitアプリのUIテスト"""
    print("Starting UI test...")
    
    # スクリーンショット保存用ディレクトリ作成
    os.makedirs("screenshots", exist_ok=True)
    
    with sync_playwright() as p:
        # ブラウザ起動（headless=Falseで画面表示）
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Accessing Streamlit app...")
        page.goto("http://localhost:8501")
        
        # ページが読み込まれるまで待機
        page.wait_for_selector("h1", timeout=10000)
        
        # 初期画面のスクリーンショット
        print("Taking welcome screen screenshot...")
        page.screenshot(path="screenshots/01_welcome.png")
        
        # タイトル確認
        title = page.locator("h1").inner_text()
        print("Page title retrieved successfully")
        
        # サイドバーのセレクトボックスを確認
        select_element = page.locator("select")
        if select_element.count() > 0:
            print("Found selectbox, testing menu options...")
            
            # ミライル契約者を選択
            select_element.select_option("ミライル契約者（残債除外）")
            time.sleep(3)
            page.screenshot(path="screenshots/02_mirail_selected.png")
            print("Mirail processor selected")
            
            # フェイス契約者を選択
            select_element.select_option("フェイス契約者（オートコール用）")
            time.sleep(3)
            page.screenshot(path="screenshots/03_faith_selected.png")
            print("Faith processor selected")
            
            # アーク新規登録を選択
            select_element.select_option("アーク新規登録データ変換")
            time.sleep(3)
            page.screenshot(path="screenshots/04_ark_selected.png")
            print("Ark processor selected")
            
            # ウェルカム画面に戻る
            select_element.select_option("選択してください")
            time.sleep(3)
            page.screenshot(path="screenshots/05_back_to_welcome.png")
            print("Back to welcome screen")
        
        print("UI test completed successfully!")
        browser.close()

if __name__ == "__main__":
    test_streamlit_ui()