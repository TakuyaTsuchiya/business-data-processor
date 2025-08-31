"""
スタイル定義モジュール
Business Data Processor

アプリケーション全体のカスタムCSSを管理
"""


def get_custom_css() -> str:
    """カスタムCSSを返す"""
    return """
    <style>
    /* タイトル固定ヘッダーシステム */
    .main .block-container > div:first-child {
        position: sticky !important;
        top: 0 !important;
        background-color: white !important;
        z-index: 999 !important;
        padding-bottom: 1rem !important;
        margin-bottom: 1rem !important;
        border-bottom: 2px solid #0066cc !important;
    }
    
    /* サイドバー固定（400px幅） */
    section[data-testid="stSidebar"] {
        width: 400px !important;
        min-width: 400px !important;
        transform: none !important;
        visibility: visible !important;
    }
    section[data-testid="stSidebar"] > div {
        width: 400px !important;
        min-width: 400px !important;
    }
    
    /* サイドバー折りたたみボタンを完全無効化 */
    button[kind="header"] {
        display: none !important;
    }
    .css-1kyxreq {
        display: none !important;
    }
    .css-1v0mbdj button {
        display: none !important;
    }
    
    /* コンパクトボタン配置（極小余白） */
    .stSidebar .stButton > button {
        margin: 0.05rem !important;
        padding: 0.2rem 0.5rem !important;
        width: 100% !important;
        text-align: left !important;
        font-size: 0.85rem !important;
    }
    .stSidebar .element-container {
        margin-bottom: 0.05rem !important;
        margin-top: 0.05rem !important;
    }
    .stSidebar .stMarkdown {
        margin-bottom: 0.1rem !important;
        margin-top: 0.1rem !important;
    }
    
    /* 業務カテゴリヘッダーのスタイリング */
    .sidebar-category {
        background: linear-gradient(90deg, #0066cc, #0099ff) !important;
        color: white !important;
        padding: 0.5rem !important;
        margin: 0.3rem 0 !important;
        border-radius: 5px !important;
        font-weight: bold !important;
        text-align: center !important;
    }
    
    /* サブカテゴリのスタイリング */
    .sidebar-subcategory {
        background-color: #f0f8ff !important;
        padding: 0.3rem !important;
        margin: 0.2rem 0 !important;
        border-left: 4px solid #0066cc !important;
        font-weight: bold !important;
        color: #0066cc !important;
    }
    
    /* メインコンテンツエリアの最適化 */
    .main .block-container {
        padding-top: 0rem !important;
        margin-top: 0 !important;
    }
    .stApp > header {
        display: none !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* ファイルアップローダーのスタイリング */
    .stFileUploader {
        border: 2px dashed #0066cc !important;
        border-radius: 10px !important;
        padding: 1rem !important;
    }
    
    /* フィルタ条件の行間を狭くする */
    .filter-condition {
        margin-bottom: 0.1rem !important;
        line-height: 1.2 !important;
    }
    .filter-condition p {
        margin: 0.1rem 0 !important;
    }
    </style>
    """