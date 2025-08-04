const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {
    const browser = await puppeteer.launch({
        headless: false,  // 画面を表示してデバッグしやすく
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        slowMo: 1000  // 動作を遅くしてデバッグしやすく
    });
    
    const page = await browser.newPage();
    await page.setViewport({ width: 1200, height: 800 });
    
    try {
        console.log('Streamlitアプリにアクセス中...');
        await page.goto('http://localhost:8501', { waitUntil: 'networkidle2', timeout: 30000 });
        
        // Streamlitの読み込み完了を待機
        await page.waitForSelector('[data-testid="stSidebar"]', { timeout: 20000 });
        await page.waitForTimeout(3000);
        
        console.log('契約者（10,000円を除外するパターン）ボタンをクリック...');
        
        // サイドバーのボタンをクリック
        await page.evaluate(() => {
            const buttons = Array.from(document.querySelectorAll('button'));
            const button = buttons.find(btn => btn.textContent.includes('契約者（10,000円を除外するパターン）'));
            if (button) {
                console.log('ボタンが見つかりました:', button.textContent);
                button.click();
            } else {
                console.log('ボタンが見つかりませんでした');
            }
        });
        
        await page.waitForTimeout(3000);
        
        console.log('スクリーンショット撮影中...');
        
        // フルページのスクリーンショット
        await page.screenshot({ path: 'screenshots/debug_full_page.png', fullPage: true });
        
        // フィルタリング条件部分を探す
        const filterElements = await page.$$('div');
        
        for (let i = 0; i < filterElements.length; i++) {
            const text = await page.evaluate(el => el.textContent, filterElements[i]);
            if (text.includes('📋 フィルタリング条件')) {
                console.log('フィルタリング条件セクションを発見:', text.substring(0, 100));
                
                // このセクションのスタイル情報を取得
                const styles = await page.evaluate(el => {
                    const computed = window.getComputedStyle(el);
                    return {
                        fontSize: computed.fontSize,
                        fontWeight: computed.fontWeight,
                        color: computed.color,
                        innerHTML: el.innerHTML.substring(0, 500)
                    };
                }, filterElements[i]);
                
                console.log('スタイル情報:', styles);
                
                // このセクションのスクリーンショット
                await filterElements[i].screenshot({ path: `screenshots/filter_section_${i}.png` });
                break;
            }
        }
        
        console.log('テスト完了');
        
    } catch (error) {
        console.error('エラー:', error);
    } finally {
        await browser.close();
    }
})();