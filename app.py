"""
Business Data Processor v2.3.0
統合データ処理システム

対応システム:
- ミライル用オートコール（6種類）
- フェイス用オートコール（3種類）
- プラザ用オートコール（3種類）
- フェイスSMS（1種類）- 支払期限機能付き
- アーク新規登録
- カプコ新規登録
- アーク残債更新
- カプコ残債の更新
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime, date

# Infrastructure Layer のインポート
from infra.csv.writer import safe_csv_download_button

# Services Layer のインポート
from services.processor_service import ProcessorExecutionService
from services.file_service import FileUploadService
from services.result_service import ResultDisplayService, FilterConditionDisplay

def safe_dataframe_display(df: pd.DataFrame):
    """安全なDataFrame表示関数（空列重複エラー対応）"""
    # DataFrameのコピーを作成して空列問題を回避
    df_display = df.copy()
    
    # 空文字列のカラム名に一時的な名前を付ける
    columns = list(df_display.columns)
    empty_col_counter = 1
    new_columns = []
    for col in columns:
        if col == "":
            new_columns.append(f"空列{empty_col_counter}")
            empty_col_counter += 1
        else:
            new_columns.append(col)
    
    # 一時的なカラム名を設定して表示
    df_display.columns = new_columns
    return st.dataframe(df_display)

def safe_csv_download(df: pd.DataFrame, filename: str, label: str = "📥 CSVファイルをダウンロード"):
    """安全なCSVダウンロード関数（Infrastructure Layer使用）"""
    return safe_csv_download_button(df, filename, label)

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
from processors.ark_registration import process_ark_data, process_arktrust_data
from processors.ark_late_payment_update import process_ark_late_payment_data
from processors.capco_registration import process_capco_data

def main():
    st.set_page_config(
        page_title="Business Data Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # カスタムCSS
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
    
    /* フィルタ条件の行間を狭くする */
    .filter-condition {
        margin-bottom: 0.1rem !important;
        line-height: 1.2 !important;
    }
    .filter-condition p {
        margin: 0.1rem 0 !important;
        line-height: 1.2 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 固定ヘッダー
    st.title("📊 Business Data Processor")
    st.markdown("**📊 Business Data Processor** - 業務データ処理統合システム")
    
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
        if st.button("契約者", key="faith_contract", use_container_width=True):
            st.session_state.selected_processor = "faith_contract"
        if st.button("保証人", key="faith_guarantor", use_container_width=True):
            st.session_state.selected_processor = "faith_guarantor"
        if st.button("緊急連絡人", key="faith_emergency", use_container_width=True):
            st.session_state.selected_processor = "faith_emergency"
        
        # プラザ用オートコール
        st.markdown('<div class="sidebar-subcategory">🏪 プラザ用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者", key="plaza_main", use_container_width=True):
            st.session_state.selected_processor = "plaza_main"
        if st.button("保証人", key="plaza_guarantor", use_container_width=True):
            st.session_state.selected_processor = "plaza_guarantor"
        if st.button("緊急連絡人", key="plaza_contact", use_container_width=True):
            st.session_state.selected_processor = "plaza_contact"
        
        # 📱 SMS送信用CSV加工
        st.markdown('<div class="sidebar-category">📱 SMS送信用CSV加工</div>', unsafe_allow_html=True)
        if st.button("フェイス　契約者", key="faith_sms_vacated", use_container_width=True):
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
        if st.button("アークトラスト新規登録（東京）", key="arktrust_registration_tokyo", use_container_width=True):
            st.session_state.selected_processor = "arktrust_registration_tokyo"
        if st.button("カプコ新規登録", key="capco_registration", use_container_width=True):
            st.session_state.selected_processor = "capco_registration"
        
        # 💰 残債の更新用CSV加工
        st.markdown('<div class="sidebar-category">💰 残債の更新用CSV加工</div>', unsafe_allow_html=True)
        if st.button("アーク残債の更新", key="ark_late_payment", use_container_width=True):
            st.session_state.selected_processor = "ark_late_payment"
        if st.button("カプコ残債の更新", key="capco_debt_update", use_container_width=True):
            st.session_state.selected_processor = "capco_debt_update"
    
    # メインコンテンツエリア
    if st.session_state.selected_processor is None:
        # ウェルカム画面
        st.markdown("""
        ## Welcome to Business Data Processor
        
        #### 📞 オートコール用CSV加工
        - **ミライル用** (6種類): 契約者・保証人・緊急連絡人 × 10,000円除外有無
        - **フェイス用** (3種類): 契約者・保証人・緊急連絡人
        - **プラザ用** (3種類): 契約者・保証人・緊急連絡人
        
        #### 📱 SMS送信用CSV加工
        - フェイス　契約者
        
        #### 📋 新規登録用CSV加工
        - アーク新規登録（東京・大阪・北海道・北関東）
        - カプコ新規登録
        
        #### 💰 残債の更新用CSV加工
        - アーク残債の更新
        - カプコ残債の更新
        
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
    elif st.session_state.selected_processor == "arktrust_registration_tokyo":
        show_arktrust_registration_tokyo()
    elif st.session_state.selected_processor == "capco_registration":
        show_capco_registration()
    elif st.session_state.selected_processor == "ark_late_payment":
        show_ark_late_payment()
    elif st.session_state.selected_processor == "capco_debt_update":
        show_capco_debt_update()

# 以下、各処理画面の関数を実装

def show_mirail_contract_without10k():
    """ミライル契約者without10k版 - Services Layer使用版"""
    st.header("ミライル契約者（10,000円を除外するパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_contract_without10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_contract_without10k_data,
                    upload_result.single_file_content,
                    "ミライル契約者without10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_mirail_contract_with10k():
    """ミライル契約者with10k版 - Services Layer使用版"""
    st.header("ミライル契約者（10,000円を除外しないパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_contract_with10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_contract_with10k_data,
                    upload_result.single_file_content,
                    "ミライル契約者with10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_mirail_guarantor_without10k():
    """ミライル保証人without10k版 - Services Layer使用版"""
    st.header("ミライル保証人（10,000円を除外するパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.1」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_guarantor_without10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_guarantor_without10k_data,
                    upload_result.single_file_content,
                    "ミライル保証人without10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_mirail_guarantor_with10k():
    """ミライル保証人with10k版 - Services Layer使用版"""
    st.header("ミライル保証人（10,000円を除外しないパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.1」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_guarantor_with10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_guarantor_with10k_data,
                    upload_result.single_file_content,
                    "ミライル保証人with10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_mirail_emergency_without10k():
    """ミライル緊急連絡人without10k版 - Services Layer使用版"""
    st.header("ミライル緊急連絡人（10,000円を除外するパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.2」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_emergency_without10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_emergencycontact_without10k_data,
                    upload_result.single_file_content,
                    "ミライル緊急連絡人without10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_mirail_emergency_with10k():
    """ミライル緊急連絡人with10k版 - Services Layer使用版"""
    st.header("ミライル緊急連絡人（10,000円を除外しないパターン）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.2」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="mirail_emergency_with10k_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_mirail_emergencycontact_with10k_data,
                    upload_result.single_file_content,
                    "ミライル緊急連絡人with10k"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_faith_contract():
    """フェイス契約者版 - Services Layer使用版"""
    st.header("フェイス契約者用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="faith_contract_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_faith_contract_data,
                    upload_result.single_file_content,
                    "フェイス契約者"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_faith_guarantor():
    """フェイス保証人版 - Services Layer使用版"""
    st.header("フェイス保証人用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯.1」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="faith_guarantor_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_faith_guarantor_data,
                    upload_result.single_file_content,
                    "フェイス保証人"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_faith_emergency():
    """フェイス緊急連絡人版 - Services Layer使用版"""
    st.header("フェイス緊急連絡人用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯.2」 → 空でない値のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="faith_emergency_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_faith_emergencycontact_data,
                    upload_result.single_file_content,
                    "フェイス緊急連絡人"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_plaza_main():
    """プラザメイン版 - Services Layer使用版"""
    st.header("プラザ契約者用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 6",
        "入金予定日 → <span style='color: red; font-weight: bold;'>当日</span>以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「TEL携帯」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="plaza_main_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_plaza_main_data,
                    upload_result.single_file_content,
                    "プラザメイン"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_plaza_guarantor():
    """プラザ保証人版 - Services Layer使用版"""
    st.header("プラザ保証人用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 6",
        "入金予定日 → 前日以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「TEL携帯.1」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="plaza_guarantor_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_plaza_guarantor_data,
                    upload_result.single_file_content,
                    "プラザ保証人"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_plaza_contact():
    """プラザ連絡先版 - Services Layer使用版"""
    st.header("プラザ緊急連絡人用オートコール")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 6",
        "入金予定日 → 前日以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「緊急連絡人１のTEL（携帯）」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="plaza_contact_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_plaza_contact_data,
                    upload_result.single_file_content,
                    "プラザ連絡先"
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_faith_sms_vacated():
    """フェイスSMS版 - Services Layer使用版"""
    st.header("📱 フェイス　契約者")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="faith_sms_payment_deadline",
        disabled=False,
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "入金予定金額 → 2,3,5円除外",
        "回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外",
        "TEL携帯 → 090/080/070形式のみ"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    upload_result = FileUploadService.handle_single_file_upload(
        label="CSVファイルをアップロードしてください",
        key="faith_sms_vacated_file"
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (カスタムパラメータ付き)
                result = ProcessorExecutionService.execute_single_file_processor(
                    process_faith_sms_vacated_contract_data,
                    upload_result.single_file_content,
                    "フェイスSMS退去済み",
                    payment_deadline_date=payment_deadline_date
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)

def show_ark_registration_tokyo():
    """アーク東京版 - Services Layer使用版"""
    st.header("📋 アーク新規登録（東京）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
        "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
        "地域コード → 1（東京）"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {
            "label": "案件取込用レポート.csvをアップロード",
            "key": "ark_tokyo_file1",
            "description": "📄 ファイル1: 案件取込用レポート"
        },
        {
            "label": "ContractList_*.csvをアップロード",
            "key": "ark_tokyo_file2", 
            "description": "📄 ファイル2: ContractList"
        }
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(
        file_configs,
        show_success=True
    )
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                # プロセッサー実行 (Services Layer使用)
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_ark_data,
                    upload_result.file_contents,
                    "アーク新規登録東京",
                    region_code=1
                )
                
                # 結果表示 (Services Layer使用)
                ResultDisplayService.show_complete_result(result)
                    

def show_ark_registration_osaka():
    """アーク大阪版 - Services Layer使用版"""
    st.header("📋 アーク新規登録（大阪）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
        "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
        "地域コード → 2（大阪）"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {"label": "案件取込用レポート.csvをアップロード", "key": "ark_osaka_file1", "description": "📄 ファイル1: 案件取込用レポート"},
        {"label": "ContractList_*.csvをアップロード", "key": "ark_osaka_file2", "description": "📄 ファイル2: ContractList"}
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(file_configs, show_success=True)
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_ark_data, upload_result.file_contents, "アーク新規登録大阪", region_code=2
                )
                ResultDisplayService.show_complete_result(result)
                    

def show_ark_registration_hokkaido():
    """アーク北海道版 - Services Layer使用版"""
    st.header("📋 アーク新規登録（北海道）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
        "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
        "地域コード → 3（北海道）"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {"label": "案件取込用レポート.csvをアップロード", "key": "ark_hokkaido_file1", "description": "📄 ファイル1: 案件取込用レポート"},
        {"label": "ContractList_*.csvをアップロード", "key": "ark_hokkaido_file2", "description": "📄 ファイル2: ContractList"}
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(file_configs, show_success=True)
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_ark_data, upload_result.file_contents, "アーク新規登録北海道", region_code=3
                )
                ResultDisplayService.show_complete_result(result)

def show_ark_registration_kitakanto():
    """アーク北関東版 - Services Layer使用版"""
    st.header("📋 アーク新規登録（北関東）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
        "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
        "地域コード → 4（北関東）"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {"label": "案件取込用レポート.csvをアップロード", "key": "ark_kitakanto_file1", "description": "📄 ファイル1: 案件取込用レポート"},
        {"label": "ContractList_*.csvをアップロード", "key": "ark_kitakanto_file2", "description": "📄 ファイル2: ContractList"}
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(file_configs, show_success=True)
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_ark_data, upload_result.file_contents, "アーク新規登録北関東", region_code=4
                )
                ResultDisplayService.show_complete_result(result)

def show_arktrust_registration_tokyo():
    """アークトラスト東京版 - Services Layer使用版"""
    st.header("📋 アークトラスト新規登録（東京）")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
        "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
        "地域コード → 1（東京）"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {"label": "案件取込用レポート.csvをアップロード", "key": "arktrust_tokyo_file1", "description": "📄 ファイル1: 案件取込用レポート"},
        {"label": "ContractList_*.csvをアップロード", "key": "arktrust_tokyo_file2", "description": "📄 ファイル2: ContractList"}
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(file_configs, show_success=True)
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_arktrust_data, upload_result.file_contents, "アークトラスト新規登録東京"
                )
                ResultDisplayService.show_complete_result(result)
                    

def show_capco_registration():
    """カプコ登録版 - Services Layer使用版"""
    st.header("📋 カプコ新規登録")
    
    # フィルタ条件表示 (Services Layer使用)
    filter_conditions = [
        "データ統合 → カプコデータ + ContractList の結合処理"
    ]
    FilterConditionDisplay.show_filter_conditions(filter_conditions)
    
    # ファイルアップロード (Services Layer使用)
    file_configs = [
        {"label": "カプコデータ.csvをアップロード", "key": "capco_file1", "description": "📄 ファイル1: カプコデータ"},
        {"label": "ContractList_*.csvをアップロード", "key": "capco_file2", "description": "📄 ファイル2: ContractList"}
    ]
    
    upload_result = FileUploadService.handle_multiple_file_upload(file_configs, show_success=True)
    
    if upload_result.success:
        if st.button("処理を実行", type="primary"):
            with st.spinner("処理中..."):
                result = ProcessorExecutionService.execute_multiple_file_processor(
                    process_capco_data, upload_result.file_contents, "カプコ新規登録"
                )
                ResultDisplayService.show_complete_result(result)
                    

def show_ark_late_payment():
    st.header("💰 アーク残債の更新")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• データ統合 → アークデータ + ContractList の結合処理")
    st.markdown("• マッチング → 管理番号での照合処理")
    st.markdown("• 残債更新 → 管理前滞納額の更新処理")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: アークデータ + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: アークデータ**")
        file1 = st.file_uploader("アークデータ.csvをアップロード", type="csv", key="ark_late_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="ark_late_file2")
    
    if file1 and file2:
        try:
            # ファイル内容を読み取り
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result = process_ark_late_payment_data(file_contents[0], file_contents[1])
                    
                if result is not None:
                    result_df, output_filename = result
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # ダウンロード
                    safe_csv_download(result_df, output_filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_capco_debt_update():
    st.header("💰 カプコ残債の更新")
    st.info("📂 必要ファイル: csv_arrear_*.csv + ContractList_*.csv（2ファイル処理）")
    st.warning("⏱️ **処理時間**: 処理には5分ほどかかります。お待ちください。")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: カプコ滞納データ**")
        file1 = st.file_uploader("csv_arrear_*.csvをアップロード", type="csv", key="capco_debt_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="capco_debt_file2")
    
    if file1 and file2:
        st.success(f"✅ {file1.name}: 読み込み完了")
        st.success(f"✅ {file2.name}: 読み込み完了")
        
        if st.button("処理を実行", type="primary"):
            try:
                # ファイル内容を読み取り
                file_contents = [file1.read(), file2.read()]
                
                # プログレスバーとステータステキストを作成
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # プログレスコールバック関数
                def update_progress(progress, message):
                    progress_bar.progress(progress)
                    status_text.text(message)
                
                from processors.capco_debt_update import process_capco_debt_update
                result_df, output_filename, stats = process_capco_debt_update(
                    file_contents[0], 
                    file_contents[1], 
                    progress_callback=update_progress
                )
                
                # プログレスバーを完了状態に
                progress_bar.progress(1.0)
                status_text.text("処理完了！")
                
                # 少し待ってからプログレスバーを非表示に
                import time
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
                    
                if len(result_df) > 0:
                    st.success(f"✅ 処理完了: {len(result_df)}件のデータを出力します")
                    
                    # ダウンロード機能
                    safe_csv_download(result_df, output_filename)
                    
                    # 処理統計情報の詳細表示
                    with st.expander("📊 処理統計情報", expanded=True):
                        st.markdown("**処理統計情報:**")
                        st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
                        
                        st.markdown("**Step 2: データ抽出**")
                        st.markdown(f"• 滞納データ列数: {stats.get('arrear_columns', 0)}列")
                        st.markdown(f"• 契約No重複削除: {stats.get('arrear_unique_before', 0):,} → {stats.get('arrear_unique_after', 0):,} 件")
                        st.markdown(f"• 削除件数: {stats.get('arrear_duplicates_removed', 0):,} 件")
                        
                        st.markdown("**Step 2.5: フィルタリング**")
                        st.markdown(f"• ContractList: {stats.get('contract_extracted', 0):,} 件")
                        st.markdown(f"• CD=1,4抽出: {stats.get('client_cd_before', 0):,} → {stats.get('client_cd_after', 0):,} 件")
                        st.markdown(f"• 除外件数: {stats.get('client_cd_excluded', 0):,} 件")
                        
                        st.markdown("**Step 3-4: マッチング**")
                        st.markdown(f"• マッチ成功: {stats.get('match_success', 0):,} 件")
                        st.markdown(f"• マッチ失敗: {stats.get('match_failed', 0):,} 件")
                        st.markdown(f"• 残債増加: {stats.get('diff_increased', 0):,} 件")
                        st.markdown(f"• 残債減少: {stats.get('diff_decreased', 0):,} 件")
                        
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                        
                else:
                    st.warning("⚠️ 更新が必要なデータが存在しませんでした。")
                    st.info("""
                    以下の条件を確認してください：
                    - クライアントCDが1または4のデータが存在するか
                    - 新旧の残債額に差分があるデータが存在するか
                    - ファイルのヘッダー形式が正しいか
                    """)
                    
            except Exception as e:
                st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")
    
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• データ抽出 → csv_arrear_*.csvから契約Noと滞納額合計")
    st.markdown("• データ統合 → ContractListから管理番号・引継番号・滞納残債・クライアントCD")
    st.markdown("• クライアントCD → 1,4のみ抽出")
    st.markdown("• マッチング → 引継番号と契約Noで照合")
    st.markdown("• 差分抽出 → 新旧残債額が異なるデータのみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("""
    ### 🔍 抽出する列情報
    **csv_arrear_*.csv から**:
    - A列: 契約No
    - Y列: 滞納額合計
    
    **ContractList_*.csv から**:
    - A列: 管理番号
    - B列: 引継番号
    - BT列: 滞納残債
    - CT列: クライアントCD
    """)



if __name__ == "__main__":
    main()