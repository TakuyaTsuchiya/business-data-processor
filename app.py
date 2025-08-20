"""
Business Data Processor v2.3.0
統合データ処理システム

対応システム:
- ミライル用オートコール（6種類）
- フェイス用オートコール（3種類）
- プラザ用オートコール（3種類）
- フェイスSMS（1種類）
- アーク新規登録
- カプコ新規登録
- アーク残債更新
- カプコ残債の更新
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

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
    """安全なCSVダウンロード関数（cp932エンコーディングエラー対応）"""
    # DataFrameのコピーを作成して空列問題を回避
    df_copy = df.copy()
    
    # 空文字列のカラム名に一時的な名前を付ける
    columns = list(df_copy.columns)
    empty_col_counter = 1
    for i, col in enumerate(columns):
        if col == "":
            columns[i] = f"_empty_col_{empty_col_counter}_"
            empty_col_counter += 1
    
    # 一時的なカラム名を設定
    df_copy.columns = columns
    
    try:
        # CSVとして出力する際に元のカラム名に戻す
        csv_data = df_copy.to_csv(index=False, encoding='cp932', errors='replace', header=list(df.columns))
        csv_bytes = csv_data.encode('cp932', errors='replace')
    except UnicodeEncodeError:
        # cp932でエラーが出る場合はUTF-8で出力
        csv_data = df_copy.to_csv(index=False, encoding='utf-8-sig', header=list(df.columns))
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
        if st.button("フェイス　契約者　退去済み", key="faith_sms_vacated", use_container_width=True):
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
        - フェイス　契約者　退去済み
        
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
    st.header("ミライル契約者（10,000円を除外するパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_contract_without10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # ダウンロード
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_contract_with10k():
    st.header("ミライル契約者（10,000円を除外しないパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_contract_with10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # ダウンロード
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_guarantor_without10k():
    st.header("ミライル保証人（10,000円を除外するパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯.1」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_guarantor_without10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_guarantor_with10k():
    st.header("ミライル保証人（10,000円を除外しないパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯.1」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_guarantor_with10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_without10k():
    st.header("ミライル緊急連絡人（10,000円を除外するパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯.2」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_emergencycontact_without10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_with10k():
    st.header("ミライル緊急連絡人（10,000円を除外しないパターン）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 空白&5")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 「TEL携帯.2」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, filtered_df, logs, filename = process_mirail_emergencycontact_with10k_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_contract():
    st.header("フェイス契約者用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 「TEL携帯」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_contract_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_faith_contract_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_guarantor():
    st.header("フェイス保証人用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 「TEL携帯.1」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_guarantor_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_faith_guarantor_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_emergency():
    st.header("フェイス緊急連絡人用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 回収ランク → 「弁護士介入」除外")
    st.markdown("• 入金予定金額 → 2,3,5,12除外")
    st.markdown("• 滞納残債フィルタ → なし（全件処理）")
    st.markdown("• 「TEL携帯.2」 → 空でない値のみ")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_emergency_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_faith_emergencycontact_data(uploaded_file.read())
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_main():
    st.header("プラザ契約者用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 6")
    st.markdown("• 入金予定日 → <span style='color: red; font-weight: bold;'>当日</span>以前とNaN", unsafe_allow_html=True)
    st.markdown("• 入金予定金額 → 2,3,5,12円除外")
    st.markdown("• 「TEL携帯」 → 空でない値のみ")
    st.markdown("• 回収ランク → 「督促停止」「弁護士介入」除外")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_main_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_plaza_main_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_guarantor():
    st.header("プラザ保証人用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 6")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 入金予定金額 → 2,3,5,12円除外")
    st.markdown("• 「TEL携帯.1」 → 空でない値のみ")
    st.markdown("• 回収ランク → 「督促停止」「弁護士介入」除外")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_guarantor_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_plaza_guarantor_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_plaza_contact():
    st.header("プラザ緊急連絡人用オートコール")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 6")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 入金予定金額 → 2,3,5,12円除外")
    st.markdown("• 「緊急連絡人１のTEL（携帯）」 → 空でない値のみ")
    st.markdown("• 回収ランク → 「督促停止」「弁護士介入」除外")
    st.markdown('</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_contact_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    filtered_df, result_df, logs, filename = process_plaza_contact_data(file_content)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # filenameは関数から取得済み
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_faith_sms_vacated():
    st.header("📱 フェイス　契約者　退去済み")
    st.markdown("**📋 フィルタ条件**: 入居ステータス(退去済み), 委託先法人ID(1-4), TEL携帯必須")
    st.markdown("**📊 出力**: SMS送信用フォーマット（退去済み契約者のSMS送信用）")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_sms_vacated_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, stats = process_faith_sms_vacated_contract_data(df)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}フェイス_SMS_退去済み.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")

def show_ark_registration_tokyo():
    st.header("📋 アーク新規登録（東京）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）")
    st.markdown("• 新規データ → 重複除外後の案件取込用レポートデータのみ統合")
    st.markdown("• 地域コード → 1（東京）")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: 案件取込用レポート**")
        file1 = st.file_uploader("案件取込用レポート.csvをアップロード", type="csv", key="ark_tokyo_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="ark_tokyo_file2")
    
    if file1 and file2:
        try:
            # ファイル内容を読み取り
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=1)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # ログ表示
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_東京.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_osaka():
    st.header("📋 アーク新規登録（大阪）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）")
    st.markdown("• 新規データ → 重複除外後の案件取込用レポートデータのみ統合")
    st.markdown("• 地域コード → 2（大阪）")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: 案件取込用レポート**")
        file1 = st.file_uploader("案件取込用レポート.csvをアップロード", type="csv", key="ark_osaka_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="ark_osaka_file2")
    
    if file1 and file2:
        try:
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=2)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_大阪.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_hokkaido():
    st.header("📋 アーク新規登録（北海道）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）")
    st.markdown("• 新規データ → 重複除外後の案件取込用レポートデータのみ統合")
    st.markdown("• 地域コード → 3（北海道）")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: 案件取込用レポート**")
        file1 = st.file_uploader("案件取込用レポート.csvをアップロード", type="csv", key="ark_hokkaido_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="ark_hokkaido_file2")
    
    if file1 and file2:
        try:
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=3)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_北海道.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_ark_registration_kitakanto():
    st.header("📋 アーク新規登録（北関東）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）")
    st.markdown("• 新規データ → 重複除外後の案件取込用レポートデータのみ統合")
    st.markdown("• 地域コード → 4（北関東）")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: 案件取込用レポート**")
        file1 = st.file_uploader("案件取込用レポート.csvをアップロード", type="csv", key="ark_kitakanto_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="ark_kitakanto_file2")
    
    if file1 and file2:
        try:
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=4)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アーク_新規登録_北関東.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_arktrust_registration_tokyo():
    st.header("📋 アークトラスト新規登録（東京）")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）")
    st.markdown("• 新規データ → 重複除外後の案件取込用レポートデータのみ統合")
    st.markdown("• 地域コード → 1（東京）")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: 案件取込用レポート**")
        file1 = st.file_uploader("案件取込用レポート.csvをアップロード", type="csv", key="arktrust_tokyo_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="arktrust_tokyo_file2")
    
    if file1 and file2:
        try:
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, stats = process_ark_data(file_contents[0], file_contents[1], region_code=1)
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    if logs:
                        st.info("処理ログ:")
                        for log in logs:
                            st.write(f"• {log}")
                    
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}アークトラスト_新規登録_東京.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

def show_capco_registration():
    st.header("📋 カプコ新規登録")
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• データ統合 → カプコデータ + ContractList の結合処理")
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("📂 必要ファイル: カプコデータ + ContractList（2ファイル処理）")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: カプコデータ**")
        file1 = st.file_uploader("カプコデータ.csvをアップロード", type="csv", key="capco_file1")
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="capco_file2")
    
    if file1 and file2:
        try:
            # ファイル内容を読み取り
            file_contents = [file1.read(), file2.read()]
            st.success(f"✅ {file1.name}: 読み込み完了")
            st.success(f"✅ {file2.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_capco_data(file_contents[0], file_contents[1])
                    
                if not result_df.empty:
                    st.success(f"処理完了: {len(result_df)}件のデータを出力")
                    
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=True):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # データプレビュー
                    st.subheader("処理結果プレビュー")
                    safe_dataframe_display(result_df.head(10))
                    
                    # ダウンロード
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}カプコ_新規登録.csv"
                    safe_csv_download(result_df, filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

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