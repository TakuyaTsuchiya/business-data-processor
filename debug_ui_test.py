"""
Debug UI Test - Streamlit要素の詳細確認
"""

from playwright.sync_api import sync_playwright
import time
import os

def debug_streamlit_ui():
    """StreamlitアプリのUI要素を詳細調査"""
    print("Starting debug UI test...")
    
    os.makedirs("screenshots", exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Accessing Streamlit app...")
        page.goto("http://localhost:8501")
        
        # ページが読み込まれるまで十分待機
        page.wait_for_load_state("networkidle")
        time.sleep(5)
        
        print("Taking full page screenshot...")
        page.screenshot(path="screenshots/debug_full_page.png", full_page=True)
        
        # すべてのselect要素を調査
        print("Looking for select elements...")
        selects = page.locator("select").all()
        print(f"Found {len(selects)} select elements")
        
        # すべてのdiv要素の中でstreamlit関連を調査
        print("Looking for Streamlit sidebar...")
        sidebar = page.locator("[data-testid='stSidebar']")
        if sidebar.count() > 0:
            print("Found Streamlit sidebar")
            sidebar.screenshot(path="screenshots/debug_sidebar.png")
            
            # サイドバー内のselect要素
            sidebar_selects = sidebar.locator("select").all()
            print(f"Found {len(sidebar_selects)} select elements in sidebar")
            
            if len(sidebar_selects) > 0:
                select_box = sidebar_selects[0]
                print("Testing selectbox interactions...")
                
                # オプションを取得
                options = select_box.locator("option").all()
                print(f"Found {len(options)} options")
                
                for i, option in enumerate(options):
                    option_text = option.inner_text()
                    print(f"Option {i}: {option_text}")
                
                # 各オプションをテスト
                if len(options) > 1:
                    for i in range(1, min(len(options), 4)):  # 最初の3つのオプション
                        print(f"Selecting option {i}...")
                        select_box.select_option(index=i)
                        time.sleep(3)
                        page.screenshot(path=f"screenshots/debug_option_{i}.png", full_page=True)
        else:
            print("Sidebar not found, looking for any selectbox...")
            all_selects = page.locator("select").all()
            if len(all_selects) > 0:
                print(f"Found {len(all_selects)} select elements on page")
                select_box = all_selects[0]
                options = select_box.locator("option").all()
                for i, option in enumerate(options):
                    option_text = option.inner_text()
                    print(f"Option {i}: {option_text}")
        
        print("Debug test completed")
        time.sleep(2)
        browser.close()

if __name__ == "__main__":
    debug_streamlit_ui()