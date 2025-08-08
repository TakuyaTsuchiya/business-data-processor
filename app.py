"""
Business Data Processor v2.3.0 - 革新的UIシステム完全版
統合データ処理システム

🎯 v2.3.0 革新的UIシステム特徴:
1. プルダウンレス常時表示メニュー
2. 階層化された業務カテゴリ構造
3. 固定ヘッダーシステム
4. コンパクトボタン配置
5. サイドバー固定

対応システム:
- ミライル用オートコール（6種類）
- フェイス用オートコール（3種類）
- プラザ用オートコール（3種類）
- フェイスSMS（1種類）
- アーク新規登録
- カプコ新規登録
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

def safe_csv_download(df: pd.DataFrame, filename: str, label: str = "📥 CSVファイルをダウンロード"):
    """安全なCSVダウンロード関数（cp932エンコーディングエラー対応）"""
    try:
        csv_data = df.to_csv(index=False, encoding='cp932', errors='replace')
        csv_bytes = csv_data.encode('cp932', errors='replace')
    except UnicodeEncodeError:
        # cp932でエラーが出る場合はUTF-8で出力
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
        csv_bytes = csv_data.encode('utf-8-sig')
        st.warning("⚠️ 一部の文字がcp932に対応していないため、UTF-8で出力します")
    
    return st.download_button(
        label=label,
        data=csv_bytes,
        file_name=filename,
        mime="text/csv",
        type="primary"
    )

# プロセッサーをインポート
from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data

from processors.faith_autocall.contract.standard import process_faith_contract_data
from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data
from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data

from processors.plaza_autocall.main.standard import process_plaza_main_data
from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data
from processors.plaza_autocall.contact.standard import process_plaza_contact_data

from processors.faith_sms.vacated_contract import process_faith_sms_vacated_contract_data
from processors.ark_registration import process_ark_data
from processors.ark_late_payment_update import process_ark_late_payment_data
from processors.capco_registration import process_capco_data

def main():
    st.set_page_config(
        page_title="Business Data Processor v2.3.0",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # v2.3.0 革新的UIシステム CSS
    st.markdown("""
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
    </style>
    """, unsafe_allow_html=True)
    
    # 固定ヘッダー
    st.title("📊 Business Data Processor v2.3.0")
    st.markdown("**🎯 革新的UIシステム** - プルダウンレス常時表示メニューによる効率的データ処理")
    
    # セッション状態の初期化
    if 'selected_processor' not in st.session_state:
        st.session_state.selected_processor = None
    
    # サイドバー：プルダウンレス常時表示メニュー
    with st.sidebar:
        # ウェルカムボタン
        if st.button("🏠 ホーム", key="home", use_container_width=True):
            st.session_state.selected_processor = None
        
        # 📞 オートコール用CSV加工
        st.markdown('<div class="sidebar-category">📞 オートコール用CSV加工</div>', unsafe_allow_html=True)
        
        # ミライル用オートコール
        st.markdown('<div class="sidebar-subcategory">🏢 ミライル用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者（10,000円を除外するパターン）", key="mirail_contract_without10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_contract_without10k"
        if st.button("契約者（10,000円を除外しないパターン）", key="mirail_contract_with10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_contract_with10k"
        if st.button("保証人（10,000円を除外するパターン）", key="mirail_guarantor_without10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_guarantor_without10k"
        if st.button("保証人（10,000円を除外しないパターン）", key="mirail_guarantor_with10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_guarantor_with10k"
        if st.button("緊急連絡人（10,000円を除外するパターン）", key="mirail_emergency_without10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_emergency_without10k"
        if st.button("緊急連絡人（10,000円を除外しないパターン）", key="mirail_emergency_with10k", use_container_width=True):
            st.session_state.selected_processor = "mirail_emergency_with10k"
        
        # フェイス用オートコール
        st.markdown('<div class="sidebar-subcategory">📱 フェイス用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者用オートコール", key="faith_contract", use_container_width=True):
            st.session_state.selected_processor = "faith_contract"
        if st.button("保証人用オートコール", key="faith_guarantor", use_container_width=True):
            st.session_state.selected_processor = "faith_guarantor"
        if st.button("緊急連絡人用オートコール", key="faith_emergency", use_container_width=True):
            st.session_state.selected_processor = "faith_emergency"
        
        # プラザ用オートコール
        st.markdown('<div class="sidebar-subcategory">🏪 プラザ用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者用オートコール", key="plaza_main", use_container_width=True):
            st.session_state.selected_processor = "plaza_main"
        if st.button("保証人用オートコール", key="plaza_guarantor", use_container_width=True):
            st.session_state.selected_processor = "plaza_guarantor"
        if st.button("緊急連絡人用オートコール", key="plaza_contact", use_container_width=True):
            st.session_state.selected_processor = "plaza_contact"
        
        # 📱 SMS送信用CSV加工
        st.markdown('<div class="sidebar-category">📱 SMS送信用CSV加工</div>', unsafe_allow_html=True)
        if st.button("フェイス_契約者_退去済みSMS用", key="faith_sms_vacated", use_container_width=True):
            st.session_state.selected_processor = "faith_sms_vacated"
        
        # 📋 新規登録用CSV加工
        st.markdown('<div class="sidebar-category">📋 新規登録用CSV加工</div>', unsafe_allow_html=True)
        if st.button("アーク新規登録（東京）", key="ark_registration_tokyo", use_container_width=True):
            st.session_state.selected_processor = "ark_registration_tokyo"
        if st.button("アーク新規登録（大阪）", key="ark_registration_osaka", use_container_width=True):
            st.session_state.selected_processor = "ark_registration_osaka"
        if st.button("アーク新規登録（北海道）", key="ark_registration_hokkaido", use_container_width=True):
            st.session_state.selected_processor = "ark_registration_hokkaido"
        if st.button("アーク新規登録（北関東）", key="ark_registration_kitakanto", use_container_width=True):
            st.session_state.selected_processor = "ark_registration_kitakanto"
        if st.button("カプコ新規登録", key="capco_registration", use_container_width=True):
            st.session_state.selected_processor = "capco_registration"
        
        # 💰 残債の更新用CSV加工
        st.markdown('<div class="sidebar-category">💰 残債の更新用CSV加工</div>', unsafe_allow_html=True)
        if st.button("アーク残債更新", key="ark_late_payment", use_container_width=True):
            st.session_state.selected_processor = "ark_late_payment"
    
    # メインコンテンツエリア
    if st.session_state.selected_processor is None:
        # ウェルカム画面
        st.markdown("""
        ## 🎉 Welcome to Business Data Processor v2.3.0
        
        ### 🎯 革新的UIシステム特徴
        - **プルダウンレス常時表示メニュー**: 全選択肢が一目で確認可能
        - **階層化された業務カテゴリ**: オートコール・SMS・新規登録の3カテゴリで整理
        - **固定ヘッダーシステム**: スクロール時もナビゲーション迷いなし
        - **コンパクトボタン配置**: 効率的な空間利用
        - **サイドバー固定**: 誤操作による隠蔽を防止
        
        ### 📊 対応システム（13種類）
        
        #### 📞 オートコール用CSV加工
        - **🏢 ミライル用** (6種類): 契約者・保証人・緊急連絡人 × 10,000円除外有無
        - **📱 フェイス用** (3種類): 契約者・保証人・緊急連絡人
        - **🏪 プラザ用** (3種類): 契約者・保証人・緊急連絡人
        
        #### 📱 SMS送信用CSV加工
        - フェイス退去済み契約者SMS用
        
        #### 📋 新規登録用CSV加工
        - アーク新規登録（111列フル仕様）
        
        ### 🚀 使用方法
        1. 左サイドバーから処理したい業務を選択
        2. CSVファイルをアップロード
        3. 自動変換処理を実行
        4. 結果をダウンロード
        
        **左サイドバーからお好きな処理を選択してください！**
        """)
        return
    
    # 各プロセッサーの処理画面
    if st.session_state.selected_processor == "mirail_contract_without10k":
        show_mirail_contract_without10k()
    elif st.session_state.selected_processor == "mirail_contract_with10k":
        show_mirail_contract_with10k()
    elif st.session_state.selected_processor == "mirail_guarantor_without10k":
        show_mirail_guarantor_without10k()
    elif st.session_state.selected_processor == "mirail_guarantor_with10k":
        show_mirail_guarantor_with10k()
    elif st.session_state.selected_processor == "mirail_emergency_without10k":
        show_mirail_emergency_without10k()
    elif st.session_state.selected_processor == "mirail_emergency_with10k":
        show_mirail_emergency_with10k()
    elif st.session_state.selected_processor == "faith_contract":
        show_faith_contract()
    elif st.session_state.selected_processor == "faith_guarantor":
        show_faith_guarantor()
    elif st.session_state.selected_processor == "faith_emergency":
        show_faith_emergency()
    elif st.session_state.selected_processor == "plaza_main":
        show_plaza_main()
    elif st.session_state.selected_processor == "plaza_guarantor":
        show_plaza_guarantor()
    elif st.session_state.selected_processor == "plaza_contact":
        show_plaza_contact()
    elif st.session_state.selected_processor == "faith_sms_vacated":
        show_faith_sms_vacated()
    elif st.session_state.selected_processor == "ark_registration_tokyo":
        show_ark_registration_tokyo()
    elif st.session_state.selected_processor == "ark_registration_osaka":
        show_ark_registration_osaka()
    elif st.session_state.selected_processor == "ark_registration_hokkaido":
        show_ark_registration_hokkaido()
    elif st.session_state.selected_processor == "ark_registration_kitakanto":
        show_ark_registration_kitakanto()
    elif st.session_state.selected_processor == "capco_registration":
        show_capco_registration()
    elif st.session_state.selected_processor == "ark_late_payment":
        show_ark_late_payment()

# 以下、各処理画面の関数を実装

def show_mirail_contract_without10k():
    st.header("🏢 ミライル契約者（10,000円を除外するパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 残債除外(CD=1,4かつ滞納残債10,000円・11,000円除外), TEL携帯必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_without10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_contract_without10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_契約者_without10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_contract_with10k():
    st.header("🏢 ミライル契約者（10,000円を除外しないパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 滞納残債フィルタなし(全件処理), TEL携帯必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_with10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_contract_with10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_契約者_with10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_guarantor_without10k():
    st.header("👥 ミライル保証人（10,000円を除外するパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 滞納残債除外(CD=1,4かつ滞納残債10,000円・11,000円除外), 入金予定金額(2,3,5,12除外), TEL携帯.1必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、保証人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_without10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_guarantor_without10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_保証人_without10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_guarantor_with10k():
    st.header("👥 ミライル保証人（10,000円を除外しないパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 滞納残債フィルタなし(全件処理), 入金予定金額(2,3,5,12除外), TEL携帯.1必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、保証人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_with10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_guarantor_with10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_保証人_with10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_without10k():
    st.header("🆘 ミライル緊急連絡人（10,000円を除外するパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 滞納残債除外(CD=1,4かつ滞納残債10,000円・11,000円除外), 入金予定金額(2,3,5,12除外), TEL携帯.2必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、緊急連絡人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_without10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_emergencycontact_without10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_緊急連絡人_without10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_with10k():
    st.header("🆘 ミライル緊急連絡人（10,000円を除外しないパターン）")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(空白&5), 入金予定日(前日以前), 回収ランク(弁護士介入除外), 滞納残債フィルタなし(全件処理), 入金予定金額(2,3,5,12除外), TEL携帯.2必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、緊急連絡人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_with10k_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_mirail_emergencycontact_with10k_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}ミライル_緊急連絡人_with10k.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_contract():
    st.header("📱 フェイス契約者用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(1-4), 入金予定日(前日以前とNaN), 回収ランク(弁護士介入除外), 入金予定金額(2,3,5,12除外), 滞納残債フィルタなし(全件処理), TEL携帯必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_contract_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_faith_contract_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}フェイス_契約者.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_guarantor():
    st.header("👥 フェイス保証人用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(1-4), 入金予定日(前日以前とNaN), 回収ランク(弁護士介入除外), 入金予定金額(2,3,5,12除外), 滞納残債フィルタなし(全件処理), TEL携帯.1必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、保証人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_guarantor_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_faith_guarantor_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}フェイス_保証人.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_emergency():
    st.header("🆘 フェイス緊急連絡人用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID(1-4), 入金予定日(前日以前とNaN), 回収ランク(弁護士介入除外), 入金予定金額(2,3,5,12除外), 滞納残債フィルタなし(全件処理), TEL携帯.2必須")
    st.markdown("**📊 出力**: 28列統一フォーマット（残債列に滞納残債を格納、緊急連絡人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_emergency_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_faith_emergencycontact_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}フェイス_緊急連絡人.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_main():
    st.header("🏪 プラザ契約者用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID=6, 入金予定日(当日以前とNaN), 延滞額合計(0,2,3,5円除外), TEL無効除外, 回収ランク(督促停止・弁護士介入除外)")
    st.markdown("**📊 出力**: 28列統一フォーマット（ContractList 1ファイル処理）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_main_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, stats = process_plaza_main_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}プラザ_契約者.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_guarantor():
    st.header("👥 プラザ保証人用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID=6, 入金予定日(前日以前とNaN), 延滞額合計(0,2,3,5円除外), TEL無効除外, 回収ランク(督促停止・弁護士介入除外)")
    st.markdown("**📊 出力**: 28列統一フォーマット（ContractList 1ファイル処理、保証人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_guarantor_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, stats = process_plaza_guarantor_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}プラザ_保証人.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_contact():
    st.header("🆘 プラザ緊急連絡人用オートコール")
    st.markdown("**📋 フィルタ条件**: 委託先法人ID=6, 入金予定日(前日以前とNaN), 延滞額合計(0,2,3,5円除外), TEL無効除外, 回収ランク(督促停止・弁護士介入除外)")
    st.markdown("**📊 出力**: 28列統一フォーマット（ContractList 1ファイル処理、緊急連絡人電話番号使用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_contact_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, stats = process_plaza_contact_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}プラザ_緊急連絡人.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_sms_vacated():
    st.header("📱 フェイス_契約者_退去済みSMS用")
    st.markdown("**📋 フィルタ条件**: 入居ステータス(退去済み), 委託先法人ID(1-4), TEL携帯必須")
    st.markdown("**📊 出力**: SMS送信用フォーマット（退去済み契約者のSMS送信用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_sms_vacated_file")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, encoding='cp932')
            st.success(f"ファイルを読み込みました: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_faith_sms_vacated_contract_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}フェイス_SMS_退去済み.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_ark_registration_tokyo():
    st.header("📋 アーク新規登録（東京）")
    st.markdown("**📋 フィルタ条件**: 案件取込用レポートとContractListの重複チェック（契約番号↔引継番号）")
    st.markdown("**📊 出力**: 111列フル仕様（テンプレートヘッダー準拠、地域コード1:東京、その他費用、1含む）")
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="ark_registration_tokyo_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            # ファイル内容を読み取り
            file_contents = []
            for file in uploaded_files:
                content = file.read()
                file_contents.append(content)
                st.success(f"{file.name}: ファイル読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=1)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_東京.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_osaka():
    st.header("📋 アーク新規登録（大阪）")
    st.markdown("**📋 フィルタ条件**: 案件取込用レポートとContractListの重複チェック（契約番号↔引継番号）")
    st.markdown("**📊 出力**: 111列フル仕様（テンプレートヘッダー準拠、地域コード2:大阪、その他費用、1含む）")
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="ark_registration_osaka_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            file_contents = []
            for file in uploaded_files:
                content = file.read()
                file_contents.append(content)
                st.success(f"{file.name}: ファイル読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=2)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_大阪.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_hokkaido():
    st.header("📋 アーク新規登録（北海道）")
    st.markdown("**📋 フィルタ条件**: 案件取込用レポートとContractListの重複チェック（契約番号↔引継番号）")
    st.markdown("**📊 出力**: 111列フル仕様（テンプレートヘッダー準拠、地域コード3:北海道、その他費用、1含む）")
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="ark_registration_hokkaido_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            file_contents = []
            for file in uploaded_files:
                content = file.read()
                file_contents.append(content)
                st.success(f"{file.name}: ファイル読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=3)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_北海道.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_kitakanto():
    st.header("📋 アーク新規登録（北関東）")
    st.markdown("**📋 フィルタ条件**: 案件取込用レポートとContractListの重複チェック（契約番号↔引継番号）")
    st.markdown("**📊 出力**: 111列フル仕様（テンプレートヘッダー準拠、地域コード4:北関東、その他費用、1含む）")
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="ark_registration_kitakanto_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            file_contents = []
            for file in uploaded_files:
                content = file.read()
                file_contents.append(content)
                st.success(f"{file.name}: ファイル読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=4)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_北関東.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_capco_registration():
    st.header("📋 カプコ新規登録")
    st.markdown("**📋 フィルタ条件**: カプコデータと契約データの統合処理")
    st.markdown("**📊 出力**: 111列フル仕様（電話番号クリーニング機能付き、混入文字自動除去）")
    st.info("📂 必要ファイル: カプコデータ + 契約データ（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="capco_registration_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            # ファイル内容を読み取り
            file_contents = []
            for file in uploaded_files:
                content = file.read()
                file_contents.append(content)
                st.success(f"{file.name}: ファイル読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, validation_errors, stats = process_capco_data(file_contents[0], file_contents[1])
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    
                    # バリデーションエラーの表示
                    if validation_errors:
                        st.warning("⚠️ バリデーションエラー:")
                        for error in validation_errors:
                            st.write(f"• {error}")
                    
                    st.info(f"📊 統計情報: {stats}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}カプコ_新規登録.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_late_payment():
    st.header("💰 アーク残債更新")
    st.markdown("**📋 フィルタ条件**: アークデータと契約データの統合処理（管理番号マッチング）")
    st.markdown("**📊 出力**: 管理番号・管理前滞納額更新CSV（残債情報更新用）")
    st.info("📂 必要ファイル: アークデータ + 契約データ（2ファイル処理）")
    
    uploaded_files = st.file_uploader("CSVファイル2つをアップロードしてください", type="csv", accept_multiple_files=True, key="ark_late_payment_files")
    
    if uploaded_files and len(uploaded_files) == 2:
        try:
            dfs = []
            for file in uploaded_files:
                df = pd.read_csv(file, encoding='cp932')
                dfs.append(df)
                st.success(f"{file.name}: {df.shape[0]}行 × {df.shape[1]}列")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result = process_ark_late_payment_data(dfs[0], dfs[1])
                    
                if result is not None:
                    result_df, stats = result
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    st.info(f"📊 統計情報: {stats}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    st.dataframe(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_残債更新.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif uploaded_files:
        st.warning("2つのCSVファイルをアップロードしてください。")

if __name__ == "__main__":
    main()