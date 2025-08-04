#!/usr/bin/env python3
"""
自動UIテスト・修正システム
Puppeteerを使用してStreamlitアプリのUIを自動テスト・比較・修正
"""

import subprocess
import time
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import asyncio

class UIAutoTester:
    def __init__(self, streamlit_url: str = "http://localhost:8501"):
        self.streamlit_url = streamlit_url
        self.screenshots_dir = "screenshots/auto_test"
        self.test_results_dir = "test_results"
        self.ensure_directories()
        
    def ensure_directories(self):
        """必要なディレクトリを作成"""
        os.makedirs(self.screenshots_dir, exist_ok=True)
        os.makedirs(self.test_results_dir, exist_ok=True)
    
    def start_streamlit_server(self) -> subprocess.Popen:
        """Streamlitサーバーを起動"""
        print("🚀 Streamlitサーバーを起動中...")
        process = subprocess.Popen(
            ["streamlit", "run", "app.py", "--server.port", "8501"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(10)  # サーバー起動待機
        return process
    
    def create_puppeteer_script(self, test_name: str) -> str:
        """Puppeteerスクリプトを動的生成"""
        script_content = f"""
const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {{
    const browser = await puppeteer.launch({{
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    }});
    
    const page = await browser.newPage();
    await page.setViewport({{ width: 1200, height: 800 }});
    
    try {{
        console.log('Navigating to {self.streamlit_url}...');
        await page.goto('{self.streamlit_url}', {{ waitUntil: 'networkidle2', timeout: 30000 }});
        
        // Streamlitの読み込み完了を待機
        await page.waitForSelector('[data-testid="stSidebar"]', {{ timeout: 20000 }});
        await page.waitForTimeout(3000);
        
        // 各ミライルプロセッサーのスクリーンショットを取得
        const processors = [
            {{ name: 'contract-without10k', button: '契約者（10,000円を除外するパターン）' }},
            {{ name: 'contract-with10k', button: '契約者（10,000円を除外しないパターン）' }},
            {{ name: 'guarantor-without10k', button: '保証人（10,000円を除外するパターン）' }},
            {{ name: 'guarantor-with10k', button: '保証人（10,000円を除外しないパターン）' }},
            {{ name: 'emergency-without10k', button: '緊急連絡人（10,000円を除外するパターン）' }},
            {{ name: 'emergency-with10k', button: '緊急連絡人（10,000円を除外しないパターン）' }}
        ];
        
        const results = {{}};
        
        for (const processor of processors) {{
            try {{
                console.log(`Testing ${{processor.name}}...`);
                
                // サイドバーのボタンをクリック
                const buttonSelector = `button:contains("${{processor.button}}")`;
                await page.evaluate((buttonText) => {{
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const button = buttons.find(btn => btn.textContent.includes(buttonText));
                    if (button) button.click();
                }}, processor.button);
                
                await page.waitForTimeout(2000);
                
                // フィルタリング条件部分のスクリーンショット
                const filterElement = await page.$('div:has-text("📋 フィルタリング条件")');
                if (filterElement) {{
                    const screenshot = await filterElement.screenshot();
                    fs.writeFileSync(`{self.screenshots_dir}/${{processor.name}}-filters.png`, screenshot);
                    
                    // フォントサイズ情報を取得
                    const fontSize = await page.evaluate(() => {{
                        const filterDiv = document.querySelector('div');
                        if (filterDiv) {{
                            const computedStyle = window.getComputedStyle(filterDiv);
                            return computedStyle.fontSize;
                        }}
                        return null;
                    }});
                    
                    results[processor.name] = {{
                        screenshot: `${{processor.name}}-filters.png`,
                        fontSize: fontSize,
                        timestamp: new Date().toISOString()
                    }};
                }}
            }} catch (error) {{
                console.error(`Error testing ${{processor.name}}:`, error);
                results[processor.name] = {{ error: error.message }};
            }}
        }}
        
        // テスト結果を保存
        fs.writeFileSync('{self.test_results_dir}/{test_name}_results.json', JSON.stringify(results, null, 2));
        console.log('UIテスト完了');
        
    }} catch (error) {{
        console.error('UIテストエラー:', error);
    }} finally {{
        await browser.close();
    }}
}})();
"""
        script_path = f"{test_name}_puppeteer.js"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(script_content)
        return script_path
    
    def run_ui_test(self, test_name: str = None) -> Dict:
        """UIテストを実行"""
        if not test_name:
            test_name = f"ui_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"📋 UIテスト開始: {test_name}")
        
        # Streamlitサーバー起動
        streamlit_process = None
        try:
            streamlit_process = self.start_streamlit_server()
            
            # Puppeteerスクリプト生成・実行
            script_path = self.create_puppeteer_script(test_name)
            
            print("🎭 Puppeteerテスト実行中...")
            result = subprocess.run(
                ["node", script_path],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode == 0:
                print("✅ UIテスト成功")
                # 結果ファイル読み込み
                results_file = f"{self.test_results_dir}/{test_name}_results.json"
                if os.path.exists(results_file):
                    with open(results_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            else:
                print(f"❌ UIテストエラー: {result.stderr}")
                
        except Exception as e:
            print(f"❌ UIテスト例外: {e}")
            
        finally:
            # クリーンアップ
            if streamlit_process:
                streamlit_process.terminate()
                streamlit_process.wait()
            
            # 一時スクリプト削除
            if 'script_path' in locals() and os.path.exists(script_path):
                os.remove(script_path)
        
        return {}
    
    def analyze_ui_inconsistencies(self, test_results: Dict) -> List[Dict]:
        """UI不一致を分析"""
        inconsistencies = []
        
        if not test_results:
            return inconsistencies
        
        # フォントサイズの一致性チェック
        font_sizes = {}
        for processor, data in test_results.items():
            if 'fontSize' in data and data['fontSize']:
                font_sizes[processor] = data['fontSize']
        
        if len(set(font_sizes.values())) > 1:
            inconsistencies.append({
                'type': 'font_size_mismatch',
                'description': 'フォントサイズが統一されていません',
                'details': font_sizes
            })
        
        return inconsistencies
    
    def generate_fix_recommendations(self, inconsistencies: List[Dict]) -> List[str]:
        """修正推奨事項を生成"""
        recommendations = []
        
        for issue in inconsistencies:
            if issue['type'] == 'font_size_mismatch':
                recommendations.append(
                    "app.py内の全てのフィルタリング条件表示部分で統一されたマークダウン形式を使用してください"
                )
                recommendations.append(
                    "<small>タグやunsafe_allow_html=Trueの使用を避け、標準のst.markdown()を使用してください"
                )
        
        return recommendations


def main():
    """メイン実行関数"""
    print("🤖 自動UIテスト・修正システム起動")
    
    # Node.js/Puppeteerの確認
    try:
        subprocess.run(["node", "--version"], check=True, capture_output=True)
        subprocess.run(["npm", "list", "puppeteer"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Node.js または Puppeteer が見つかりません")
        print("インストール方法:")
        print("npm install puppeteer")
        return
    
    tester = UIAutoTester()
    
    # UIテスト実行
    results = tester.run_ui_test()
    
    if results:
        print("📊 テスト結果:")
        print(json.dumps(results, indent=2, ensure_ascii=False))
        
        # 不一致分析
        inconsistencies = tester.analyze_ui_inconsistencies(results)
        
        if inconsistencies:
            print("\n⚠️  UI不一致検出:")
            for issue in inconsistencies:
                print(f"- {issue['description']}")
                print(f"  詳細: {issue['details']}")
            
            # 修正推奨事項
            recommendations = tester.generate_fix_recommendations(inconsistencies)
            print("\n🔧 修正推奨事項:")
            for rec in recommendations:
                print(f"- {rec}")
        else:
            print("✅ UI一致性確認完了")
    else:
        print("❌ テスト結果を取得できませんでした")


if __name__ == "__main__":
    main()