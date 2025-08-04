"""
Business Data Processor v2.4.0
統合データ処理システム

対応システム（完全版）:
- アーク新規登録データ変換（111列フル仕様）
- アーク残債更新処理（管理番号・管理前滞納額）
- カプコ新規登録（重複チェック・完全変換）
- ミライル用オートコール（契約者・保証人・緊急連絡人・6種類）
- フェイス用オートコール（契約者・保証人・緊急連絡人・3種類）
- プラザ用オートコール（契約者・緊急連絡人・2種類実装済み）
- フェイスSMS（退去済み契約者・実装済み）

★ 簡略版廃止・動作実績コード100%採用
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime


def safe_csv_download(df: pd.DataFrame, filename: str, label: str = "📥 CSVファイルをダウンロード"):
    """安全なCSVダウンロード関数（cp932エンコーディングエラー対応）"""
    try:
        csv_data = df.to_csv(index=False, encoding='cp932', errors='ignore')
        csv_bytes = csv_data.encode('cp932', errors='ignore')
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

# アーク残債更新プロセッサーをインポート
from processors.ark_late_payment_update import process_ark_late_payment_data

def main():
    st.set_page_config(
        page_title="Business Data Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # サイドバー幅の拡張とタイトル余白調整（CSS）
    st.markdown("""
    <style>
    /* サイドバー幅設定 */
    .css-1d391kg {
        width: 400px !important;
    }
    .css-1lcbmhc {
        width: 400px !important;
    }
    section[data-testid="stSidebar"] {
        width: 400px !important;
        min-width: 400px !important;
    }
    section[data-testid="stSidebar"] > div {
        width: 400px !important;
        min-width: 400px !important;
    }
    .stSelectbox > div > div {
        min-width: 350px !important;
    }
    
    /* タイトル部分の余白を極限まで削減 */
    .main .block-container {
        padding-top: 0rem !important;
        margin-top: 0 !important;
        padding-bottom: 0 !important;
    }
    .main .block-container h1 {
        margin-top: 0 !important;
        padding-top: 0 !important;
        margin-bottom: 1rem !important;
    }
    .stApp > header {
        background-color: transparent;
        height: 0rem !important;
        display: none !important;
    }
    .stApp {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    /* Streamlit の各種要素の余白削除 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp > div:first-child {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    div[data-testid="stVerticalBlock"] > div:first-child > div {
        padding-top: 0 !important;
        margin-top: 0 !important;
    }
    .element-container:first-child {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    
    /* サイドバーボタンの余白を極小に */
    .stSidebar .stButton > button {
        margin-bottom: 0.1rem !important;
        margin-top: 0.1rem !important;
        padding: 0.2rem 0.5rem !important;
    }
    .stSidebar .element-container {
        margin-bottom: 0.05rem !important;
        margin-top: 0.05rem !important;
    }
    .stSidebar .stMarkdown {
        margin-bottom: 0.15rem !important;
        margin-top: 0.15rem !important;
    }
    section[data-testid="stSidebar"] .element-container {
        margin-bottom: 0.05rem !important;
        margin-top: 0.05rem !important;
    }
    /* ボタンコンテナの余白も削減 */
    .stSidebar div[data-testid="column"] {
        gap: 0.05rem !important;
    }
    
    /* サイドバーを常時表示に固定 */
    section[data-testid="stSidebar"] {
        transform: none !important;
        visibility: visible !important;
    }
    /* サイドバー折りたたみボタンを非表示 */
    button[kind="header"] {
        display: none !important;
    }
    .css-1kyxreq {
        display: none !important;
    }
    /* サイドバーの閉じるボタンを無効化 */
    .css-1v0mbdj button {
        display: none !important;
    }
    
    /* タイトル部分を固定表示 */
    .main .block-container > div:first-child {
        position: sticky !important;
        top: 0 !important;
        background-color: white !important;
        z-index: 999 !important;
        padding-bottom: 1rem !important;
        margin-bottom: 1rem !important;
        border-bottom: 1px solid #e0e0e0 !important;
    }
    
    /* メインコンテンツエリアの調整 */
    .main .block-container {
        padding-top: 0rem !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # ヘッダー
    st.title("Business Data Processor")
    st.markdown("**統合データ処理システム** - CSVデータの自動変換・フィルタリングツール")
    st.info("左側のメニューから処理したい業務を選択してください")
    
    # サイドバーメニュー（シンプル構成）
    # セッション状態の初期化
    if 'selected_processor' not in st.session_state:
        st.session_state.selected_processor = "🏠 トップ"
    
    # オートコールカテゴリ
    st.sidebar.markdown("### 📞 オートコール用CSV加工")
    
    # ミライル
    st.sidebar.markdown("**ミライル用オートコール**")
    if st.sidebar.button("契約者（10,000円を除外するパターン）", key="mirail_contract_without10k", use_container_width=True):
        st.session_state.selected_processor = "🏢 ミライル契約者（10,000円を除外するパターン）"
    if st.sidebar.button("契約者（10,000円を除外しないパターン）", key="mirail_contract_with10k", use_container_width=True):
        st.session_state.selected_processor = "🏢 ミライル契約者（10,000円を除外しないパターン）"
    if st.sidebar.button("保証人（10,000円を除外するパターン）", key="mirail_guarantor_without10k", use_container_width=True):
        st.session_state.selected_processor = "👥 ミライル保証人（10,000円を除外するパターン）"
    if st.sidebar.button("保証人（10,000円を除外しないパターン）", key="mirail_guarantor_with10k", use_container_width=True):
        st.session_state.selected_processor = "👥 ミライル保証人（10,000円を除外しないパターン）"
    if st.sidebar.button("緊急連絡人（10,000円を除外するパターン）", key="mirail_emergency_without10k", use_container_width=True):
        st.session_state.selected_processor = "🚨 ミライル緊急連絡人（10,000円を除外するパターン）"
    if st.sidebar.button("緊急連絡人（10,000円を除外しないパターン）", key="mirail_emergency_with10k", use_container_width=True):
        st.session_state.selected_processor = "🚨 ミライル緊急連絡人（10,000円を除外しないパターン）"
    
    # フェイス
    st.sidebar.markdown("**フェイス用オートコール**")
    if st.sidebar.button("契約者", key="faith_contract", use_container_width=True):
        st.session_state.selected_processor = "📱 フェイス契約者"
    if st.sidebar.button("保証人", key="faith_guarantor", use_container_width=True):
        st.session_state.selected_processor = "👥 フェイス保証人"
    if st.sidebar.button("緊急連絡人", key="faith_emergency", use_container_width=True):
        st.session_state.selected_processor = "🚨 フェイス緊急連絡人"
    
    # プラザ
    st.sidebar.markdown("**プラザ用オートコール**")
    if st.sidebar.button("契約者", key="plaza_contract", use_container_width=True):
        st.session_state.selected_processor = "🏪 プラザ契約者"
    if st.sidebar.button("保証人", key="plaza_guarantor", use_container_width=True):
        st.session_state.selected_processor = "👥 プラザ保証人"
    if st.sidebar.button("緊急連絡人", key="plaza_emergency", use_container_width=True):
        st.session_state.selected_processor = "🚨 プラザ緊急連絡人"
    
    st.sidebar.markdown("---")
    
    # SMSカテゴリ
    st.sidebar.markdown("### 📱 SMS送信用CSV加工")
    if st.sidebar.button("フェイス_契約者_退去済みSMS用", key="faith_sms", use_container_width=True):
        st.session_state.selected_processor = "📱 フェイス_契約者_退去済みSMS用"
    
    st.sidebar.markdown("---")
    
    # 新規登録カテゴリ
    st.sidebar.markdown("### 📋 新規登録用CSV加工")
    if st.sidebar.button("アーク新規登録", key="ark_registration", use_container_width=True):
        st.session_state.selected_processor = "📋 アーク新規登録"
    if st.sidebar.button("カプコ新規登録", key="capco_registration", use_container_width=True):
        st.session_state.selected_processor = "📋 カプコ新規登録"
    
    st.sidebar.markdown("---")
    
    # 残債更新カテゴリ
    st.sidebar.markdown("### 残債の更新用CSV加工")
    if st.sidebar.button("アーク残債更新", key="ark_late_payment", use_container_width=True):
        st.session_state.selected_processor = "アーク残債更新"
    
    # 選択されたプロセッサーを取得
    processor_type = st.session_state.selected_processor
    
    # 選択された処理に応じて画面を表示
    if (processor_type is None or 
        processor_type == "🏠 トップ"):
        # ウェルカム画面
        show_welcome_screen()
    elif processor_type == "🏢 ミライル契約者（10,000円を除外するパターン）":
        show_mirail_contract_without10k_processor()
    elif processor_type == "🏢 ミライル契約者（10,000円を除外しないパターン）":
        show_mirail_contract_with10k_processor()
    elif processor_type == "👥 ミライル保証人（10,000円を除外するパターン）":
        show_mirail_guarantor_without10k_processor()
    elif processor_type == "👥 ミライル保証人（10,000円を除外しないパターン）":
        show_mirail_guarantor_with10k_processor()
    elif processor_type == "🚨 ミライル緊急連絡人（10,000円を除外するパターン）":
        show_mirail_emergencycontact_without10k_processor()
    elif processor_type == "🚨 ミライル緊急連絡人（10,000円を除外しないパターン）":
        show_mirail_emergencycontact_with10k_processor()
    elif processor_type == "📱 フェイス契約者":
        show_faith_contract_processor()
    elif processor_type == "👥 フェイス保証人":
        show_faith_guarantor_processor()
    elif processor_type == "🚨 フェイス緊急連絡人":
        show_faith_emergencycontact_processor()
    elif processor_type == "🏪 プラザ契約者":
        show_plaza_main_processor()
    elif processor_type == "👥 プラザ保証人":
        show_plaza_guarantor_processor()
    elif processor_type == "🚨 プラザ緊急連絡人":
        show_plaza_contact_processor()
    elif processor_type == "📱 フェイス_契約者_退去済みSMS用":
        show_faith_sms_vacated_contract_processor()
    elif processor_type == "📋 アーク新規登録":
        show_ark_processor()
    elif processor_type == "📋 カプコ新規登録":
        show_capco_processor()
    elif processor_type == "アーク残債更新":
        process_ark_late_payment_page()

def show_welcome_screen():
    """ウェルカム画面の表示"""
    st.markdown("## 🏠 ようこそ")
    st.markdown("### 業務別データ処理システム")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📞 オートコール
        **対応システム:**
        - 🏢 ミライル（契約者/保証人/緊急連絡人 × without10k/with10k）
        - 📱 フェイス
        - 🏪 プラザ（契約者/保証人/緊急連絡人）
        
        **機能:**
        - ContractListから各種データを抽出
        - 条件別フィルタリング
        - オートコール用CSVを生成
        """)
    
    with col2:
        st.markdown("""
        ### 📱 SMS
        **対応システム:**
        - 📱 フェイス_契約者_退去済みSMS用
        
        **機能:**
        - 退去済み契約者SMS送信用データ処理
        - 電話番号の正規化・フィルタリング
        - SMS用CSVファイル生成
        """)
    
    with col3:
        st.markdown("""
        ### 新規登録
        **対応システム:**
        - アーク新規登録
        - カプコ新規登録
        
        **機能:**
        - 案件取込用レポートとContractListの統合
        - 住所分割・保証人判定を自動実行
        - 統合CSVを生成
        - 重複チェック・データ変換
        """)
    
    # 残債更新の説明を追加
    st.markdown("---")
    st.markdown("""
    ### 残債の更新
    **対応システム:**
    - アーク残債更新
    
    **機能:**
    - アーク残債CSVとContractListの自動紐付け
    - 契約番号と引継番号でのマッチング
    - 管理番号と管理前滞納額の2列出力
    - エンコーディング自動検出
    """)
    

def show_mirail_contract_without10k_processor():
    """ミライル契約者処理画面"""
    st.markdown("## 契約者（10,000円を除外するパターン）")
    st.markdown("ContractListから契約者の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **回収ランク**: 弁護士介入を除外
    - **滞納残債**: クライアントCD=1で1万円・1万1千円を除外
    - **TEL携帯**: 空でない値のみ
    - **入金予定金額**: 2,3,5,12を除外
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライルプロセッサーをインポート
                    from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_contract_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_contract_with10k_processor():
    """ミライル（残債含む）契約者処理画面"""
    st.markdown("## 契約者（10,000円を除外しないパターン）")
    st.markdown("ContractListから契約者の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **回収ランク**: 弁護士介入を除外
    - **滞納残債**: 除外なし（1万円・1万1千円含む全件）
    - **TEL携帯**: 空でない値のみ
    - **入金予定金額**: 2,3,5,12を除外
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_contract_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 残債含有率の表示
                    if len(df_filtered) > 0 and "残債" in df_filtered.columns:
                        debt_10k = (df_filtered["残債"].astype(str) == "10000").sum()
                        debt_11k = (df_filtered["残債"].astype(str) == "11000").sum()
                        st.info(f"残債1万円: {debt_10k}件, 残債1万1千円: {debt_11k}件（除外されていません）")
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_contract_processor():
    """フェイス契約者処理画面"""
    st.markdown("## 📱 フェイス契約者オートコール")
    st.markdown("ContractListからフェイス委託先の契約者データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **入金予定金額**: 2,3,5,12を除外
    - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
    - **TEL携帯**: 空でない値のみ
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_contract_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_contract_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス契約者プロセッサーをインポート
                    from processors.faith_autocall.contract.standard import process_faith_contract_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_contract_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_guarantor_without10k_processor():
    """ミライル保証人（残債除外）処理画面"""
    st.markdown("## 保証人（10,000円を除外するパターン）")
    st.markdown("ContractListから保証人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）  
    - **回収ランク**: 弁護士介入を除外
    - **残債除外**: クライアントCD=1かつ残債10,000円・11,000円のレコードのみ除外
    - **入金予定金額**: 2,3,5,12を除外
    - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_guarantor_without10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_guarantor_without10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル保証人（残債除外）プロセッサーをインポート
                    from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_guarantor_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_guarantor_with10k_processor():
    """ミライル保証人（残債含む）処理画面"""
    st.markdown("## 保証人（10,000円を除外しないパターン）")
    st.markdown("ContractListから保証人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **回収ランク**: 弁護士介入を除外
    - **残債含む**: 全件処理（10,000円・11,000円も含む）
    - **クライアント**: 全クライアント対象（CDフィルタなし）
    - **入金予定金額**: 2,3,5,12を除外
    - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_guarantor_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_guarantor_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル保証人（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_guarantor_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_emergencycontact_without10k_processor():
    """ミライル緊急連絡人（残債除外）処理画面"""
    st.markdown("## 緊急連絡人（10,000円を除外するパターン）")
    st.markdown("ContractListから緊急連絡人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **回収ランク**: 弁護士介入を除外
    - **残債除外**: クライアントCD=1かつ残債10,000円・11,000円のレコードのみ除外
    - **入金予定金額**: 2,3,5,12を除外
    - **緊急連絡人１のTEL（携帯）**: 空でない値のみ（緊急連絡人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_emergencycontact_without10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_emergencycontact_without10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル緊急連絡人（残債除外）プロセッサーをインポート
                    from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_emergencycontact_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_emergencycontact_with10k_processor():
    """ミライル緊急連絡人（残債含む）処理画面"""
    st.markdown("## 緊急連絡人（10,000円を除外しないパターン）")
    st.markdown("ContractListから緊急連絡人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 空白と5
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **回収ランク**: 弁護士介入を除外
    - **残債含む**: 全件処理（10,000円・11,000円も含む）
    - **クライアント**: 全クライアント対象（CDフィルタなし）
    - **入金予定金額**: 2,3,5,12を除外
    - **緊急連絡人１のTEL（携帯）**: 空でない値のみ（緊急連絡人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_emergencycontact_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_emergencycontact_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル緊急連絡人（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_emergencycontact_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_guarantor_processor():
    """フェイス保証人処理画面"""
    st.markdown("## 👥 フェイス保証人オートコール")
    st.markdown("ContractListからフェイス委託先の保証人データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **入金予定金額**: 2,3,5,12を除外
    - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
    - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_guarantor_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_guarantor_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス保証人プロセッサーをインポート
                    from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_guarantor_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_emergencycontact_processor():
    """フェイス緊急連絡人処理画面"""
    st.markdown("## 🚨 フェイス緊急連絡人オートコール")
    st.markdown("ContractListからフェイス委託先の緊急連絡人データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
    - **入金予定日**: 前日以前またはNaN（当日は除外）
    - **入金予定金額**: 2,3,5,12を除外
    - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
    - **緊急連絡人１のTEL（携帯）**: 空でない値のみ（緊急連絡人電話番号）
    """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_emergencycontact_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_emergencycontact_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス緊急連絡人プロセッサーをインポート
                    from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_emergencycontact_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_ark_processor():
    """アーク新規登録処理画面"""
    st.markdown("## アーク新規登録")
    st.markdown("案件取込用レポートとContractListを統合し、アーク新規登録用のCSVを生成します")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 案件取込用レポート")
        report_file = st.file_uploader(
            "【東京支店】①案件取込用レポート*.csv",
            type=['csv'],
            key="ark_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="ark_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    if report_file is not None and contract_file is not None:
        st.success("両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("処理開始", key="ark_process", type="primary"):
            with st.spinner("データを統合・変換中..."):
                try:
                    # アークプロセッサーをインポート
                    from processors.ark_registration import process_ark_data
                    
                    # ファイル内容を取得
                    report_content = report_file.getvalue()
                    contract_content = contract_file.getvalue()
                    
                    # データ処理実行
                    df_output, logs, output_filename = process_ark_data(report_content, contract_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    if len(df_output) > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("出力件数", len(df_output))
                        with col2:
                            phone_count = sum([
                                df_output["契約者TEL自宅"].notna().sum(),
                                df_output["契約者TEL携帯"].notna().sum()
                            ])
                            st.metric("電話番号件数", phone_count)
                        with col3:
                            room_count = df_output["部屋番号"].notna().sum()
                            st.metric("部屋番号あり", room_count)
                        
                        # データプレビュー表示
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        # 表示用に列を選択
                        preview_columns = [
                            "引継番号", "契約者氏名", "契約者カナ", 
                            "契約者TEL携帯", "物件名", "部屋番号",
                            "月額賃料", "退去手続き費用"
                        ]
                        available_columns = [col for col in preview_columns if col in df_output.columns]
                        st.dataframe(df_output[available_columns].head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                        
                        # 詳細統計情報
                        with st.expander("📈 詳細統計情報"):
                            st.markdown("#### データ品質統計")
                            
                            # 電話番号統計
                            home_tel_count = df_output["契約者TEL自宅"].notna().sum()
                            mobile_tel_count = df_output["契約者TEL携帯"].notna().sum()
                            st.text(f"契約者TEL自宅: {home_tel_count}件")
                            st.text(f"契約者TEL携帯: {mobile_tel_count}件")
                            
                            # 住所統計
                            if "契約者現住所1" in df_output.columns:
                                addr1_count = df_output["契約者現住所1"].notna().sum()
                                st.text(f"都道府県あり: {addr1_count}件")
                            
                            # 金額統計
                            if "月額賃料" in df_output.columns:
                                rent_avg = pd.to_numeric(df_output["月額賃料"], errors='coerce').mean()
                                if pd.notna(rent_avg):
                                    st.text(f"平均賃料: {rent_avg:,.0f}円")
                    else:
                        st.warning("⚠️ 処理対象データがありませんでした")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_main_processor():
    """プラザ契約者処理画面"""
    st.markdown("## 🏪 プラザ契約者オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ契約者のオートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **2ファイル処理**: ContractList + Excel報告書の結合処理
    - **延滞額フィルター**: 0円、2円、3円、5円を除外
    - **TEL無効除外**: "TEL無効"を含むレコードを除外
    - **回収ランクフィルター**: 督促停止、弁護士介入を除外
    - **契約者TEL携帯**: 空でない値のみ（動的列検出）
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_main_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_main_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="plaza_main_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ契約者プロセッサーをインポート
                    from processors.plaza_autocall.main.standard import process_plaza_main_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_main_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_guarantor_processor():
    """プラザ保証人処理画面"""
    st.markdown("## 👥 プラザ保証人オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ保証人のオートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **2ファイル処理**: ContractList + Excel報告書の結合処理
    - **延滞額フィルター**: 0円、2円、3円、5円を除外
    - **TEL無効除外**: "TEL無効"を含むレコードを除外
    - **回収ランクフィルター**: 督促停止、弁護士介入を除外
    - **保証人電話番号**: 保証人TEL携帯列の空でない値のみ
    - **出力形式**: 保証人名、契約者名、管理番号を含むオートコール用CSV
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_guarantor_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_guarantor_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="plaza_guarantor_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ保証人プロセッサーをインポート
                    from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_guarantor_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.success("✅ プラザ保証人データ処理が完了しました")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_contact_processor():
    """プラザ緊急連絡人処理画面"""
    st.markdown("## 🚨 プラザ緊急連絡人オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ緊急連絡人のオートコール用CSVを生成します")
    
    # 処理条件の表示
    st.markdown("**フィルタリング条件**")
    st.markdown("""
    - **2ファイル処理**: ContractList + Excel報告書の結合処理
    - **延滞額フィルター**: 0円、2円、3円、5円を除外
    - **TEL無効除外**: "TEL無効"を含むレコードを除外
    - **回収ランクフィルター**: 督促停止、弁護士介入を除外
    - **緊急連絡人１のTEL（携帯）**: 空でない値のみ
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_contact_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_contact_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="plaza_contact_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ緊急連絡人プロセッサーをインポート
                    from processors.plaza_autocall.contact.standard import process_plaza_contact_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_contact_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        safe_csv_download(df_output, output_filename)
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_sms_vacated_contract_processor():
    """フェイスSMS退去済み契約者処理画面"""
    st.markdown("## 📱 フェイスSMS退去済み契約者処理")
    
    # 機能説明
    st.markdown("""
    ### 🎯 主な処理機能
    - **退去済み契約者抽出**: 契約状態「退去済み」の契約者をフィルタリング
    - **電話番号の正規化**: ハイフン除去・形式統一（090-xxxx-xxxx → 09012345678）
    - **SMS送信可能番号**: 090/080/070番号のみ抽出
    - **重複除去処理**: 同一電話番号の重複を自動除去
    - **CSV出力**: SMS送信システム用の専用フォーマットで出力
    
    ### 📋 入力ファイル形式
    **ContractList*.csv** - フェイス契約管理システムからのエクスポートファイル
    """)
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader(
        "ContractList*.csv ファイルをアップロードしてください",
        type="csv",
        help="フェイス退去済み契約者データファイルを選択してください"
    )
    
    if uploaded_file is not None:
        if st.button("🚀 データ処理を開始", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    from processors.faith_sms.vacated_contract import process_faith_sms_vacated_contract_data
                    
                    file_content = uploaded_file.getvalue()
                    
                    processed_df, output_filename, initial_rows, final_rows = process_faith_sms_vacated_contract_data(file_content)
                    
                    st.success(f"✅ 処理完了: {initial_rows}行 → {final_rows}行")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("### 📊 処理統計")
                        st.write(f"- 入力行数: {initial_rows:,}行")
                        st.write(f"- 出力行数: {final_rows:,}行")
                        st.write(f"- 除外行数: {initial_rows - final_rows:,}行")
                        st.write(f"- 出力列数: {len(processed_df.columns)}列")
                    
                    with col2:
                        st.markdown("### 🔍 データプレビュー")
                        st.dataframe(processed_df.head(3), use_container_width=True)
                    
                    csv_data = processed_df.to_csv(index=False, encoding='cp932')
                    
                    st.download_button(
                        label=f"📥 {output_filename} をダウンロード",
                        data=csv_data.encode('cp932'),
                        file_name=output_filename,
                        mime="text/csv"
                    )
                    
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {str(e)}")
                    st.info("💡 ファイル形式やエンコーディングを確認してください")


def show_capco_processor():
    """カプコ新規登録処理画面"""
    st.markdown("## カプコ新規登録")
    st.markdown("カプコ元データとContractListを統合し、ミライル顧客システム用CSVを生成します")
    
    # ファイルアップロードエリアのスタイル改善
    st.markdown("""
    <style>
    .stFileUploader > div > div > div > div {
        min-height: 120px !important;
    }
    .stFileUploader [data-testid="stFileUploaderDropzone"] {
        min-height: 120px !important;
        padding: 2rem !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # ファイルアップロード（最優先で配置）
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### カプコ元データ.csv")
        capco_file = st.file_uploader(
            "カプコ元データ.csv をアップロード",
            type=['csv'],
            key="capco_data"
        )
        if capco_file:
            st.success(f"ファイル: {capco_file.name}")
    
    with col2:
        st.markdown("#### ContractList_*.csv")
        contract_file = st.file_uploader(
            "ContractList_*.csv をアップロード",
            type=['csv'],
            key="capco_contract",
            help="最大100MB、CSVファイルのみ対応"
        )
        if contract_file:
            st.success(f"ファイル: {contract_file.name}")

    if capco_file is not None and contract_file is not None:
        # 処理ボタン（中央揃え・サイズ拡大・爽やかな色）
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
            <style>
            .stButton > button {
                width: 100%;
                height: 3rem;
                font-size: 1.2rem;
                font-weight: bold;
                background-color: #4A90E2;
                color: white;
                border: none;
                border-radius: 0.5rem;
                transition: all 0.3s ease;
            }
            .stButton > button:hover {
                background-color: #357ABD;
                transform: translateY(-2px);
                box-shadow: 0 4px 8px rgba(74, 144, 226, 0.3);
            }
            </style>
            """, unsafe_allow_html=True)
            
            if st.button("処理開始", key="capco_process"):
                with st.spinner("データを統合・変換中..."):
                    try:
                        # ファイルサイズチェック
                        MAX_SIZE = 100 * 1024 * 1024  # 100MB
                        capco_size = len(capco_file.getvalue())
                        contract_size = len(contract_file.getvalue())
                        
                        if capco_size > MAX_SIZE:
                            st.error(f"エラー: カプコファイルサイズが制限を超えています: {capco_size:,} bytes > {MAX_SIZE:,} bytes")
                            return
                        if contract_size > MAX_SIZE:
                            st.error(f"エラー: ContractListファイルサイズが制限を超えています: {contract_size:,} bytes > {MAX_SIZE:,} bytes")
                            return
                        
                        st.info(f"ファイルサイズ確認完了 - カプコ: {capco_size:,} bytes, ContractList: {contract_size:,} bytes")
                        
                        # カプコプロセッサーをインポート
                        from processors.capco_registration import process_capco_data
                        
                        # ファイル内容を取得
                        capco_content = capco_file.getvalue()
                        contract_content = contract_file.getvalue()
                        
                        # データ処理実行
                        df_output, logs, output_filename = process_capco_data(capco_content, contract_content)
                        
                        # 処理結果表示
                        st.success("処理が完了しました")
                        
                        # 処理ログ表示
                        with st.expander("処理ログ"):
                            for log in logs:
                                st.text(log)
                        
                        # CSVダウンロード
                        if len(df_output) > 0:
                            safe_csv_download(df_output, output_filename)
                        else:
                            st.warning("処理対象データがありませんでした")
                            
                    except ImportError as e:
                        st.error(f"モジュールインポートエラー: {e}")
                    except Exception as e:
                        st.error(f"エラーが発生しました: {e}")
                        with st.expander("エラー詳細"):
                            st.exception(e)

    # 処理機能説明（最後に配置）
    st.markdown("**主な処理機能**")
    st.markdown("""
    #### 重複チェック・新規抽出
    - **照合キー**: カプコ元データ「契約No」⟷ ContractList「引継番号」
    - **新規案件のみ抽出**: 既存データとの重複を自動除外
    
    #### データ変換
    - **基本情報**: 氏名・カナ正規化（スペース除去、ひらがな→カタカナ変換）
    - **電話番号処理**: 携帯優先ロジック（携帯空欄時は自宅番号を携帯欄に移動）
    - **住所分割**: 都道府県・市区町村・残り住所への3分割
    - **物件住所生成**: 契約者住所から物件名・部屋番号を除去
    
    #### 口座・業務情報
    - **支店CD判定**: 中央支店→763、東海支店→730
    - **クライアントCD**: 約定日による自動判定（1004年→1、1005年→4）
    - **引継情報**: 「カプコ一括登録 ●保証開始日：{契約開始}」自動生成
    
    #### 固定値・空白値設定
    - **固定値**: 入居中・未精算・契約中・バックレント・普通・9・25・0など
    - **空白値**: 勤務先・保証人・緊急連絡人情報など37項目を明示的に空白化
    """)


def process_ark_late_payment_page():
    """アーク残債更新処理画面"""
    st.markdown("## アーク残債更新処理")
    st.markdown("アーク残債CSVとContractListを紐付けて、ミライル顧客システム用の取込ファイルを生成します")
    
    # 処理条件の表示
    with st.expander("処理仕様詳細"):
        st.markdown("""
        ### 必要ファイル
        1. **アーク残債CSVファイル**
           - ファイル名: 【アーク継続中】②残債取込用CSV_*.csv
           - 必須カラム: 契約番号、管理前滞納額
        
        2. **ContractListファイル**
           - ファイル名: ContractList*.csv
           - 必須カラム: 引継番号、管理番号
        
        ### 処理フロー
        1. ファイル読み込み（エンコーディング自動検出）
        2. 必須カラム確認・バリデーション
        3. データ紐付け・統計計算
        4. 2列CSV出力・完了報告
        """)
    
    # ファイルアップロード
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### アーク残債CSVファイル")
        arc_file = st.file_uploader(
            "【アーク継続中】②残債取込用CSV_*.csv",
            type=['csv'],
            key="arc_csv_upload"
        )
    
    with col2:
        st.markdown("### ContractListファイル")
        contract_file = st.file_uploader(
            "ContractList*.csv",
            type=['csv'],
            key="contract_list_upload"
        )
    
    # 処理実行
    if arc_file and contract_file:
        if st.button("処理開始", key="process_ark_late_payment", type="primary"):
            try:
                # 処理実行
                result = process_ark_late_payment_data(arc_file, contract_file)
                
                if result:
                    df_output, output_filename = result
                    
                    # 結果表示
                    st.success(f"処理が完了しました！")
                    
                    # データプレビュー
                    st.markdown("### 出力データプレビュー")
                    st.dataframe(df_output.head(10))
                    
                    # CSV出力（CP932エンコーディング）
                    csv_buffer = io.BytesIO()
                    df_output.to_csv(
                        csv_buffer, 
                        index=False, 
                        encoding='cp932',
                        errors='replace'
                    )
                    csv_data = csv_buffer.getvalue()
                    
                    # ダウンロードボタン
                    st.download_button(
                        label=f"{output_filename} をダウンロード",
                        data=csv_data,
                        file_name=output_filename,
                        mime="text/csv",
                        type="primary"
                    )
                    
                    st.info("ダウンロードしたファイルをミライル顧客システムに取り込んでください")
                else:
                    st.error("処理に失敗しました。ファイルの内容を確認してください。")
                    
            except Exception as e:
                st.error(f"❌ エラーが発生しました: {str(e)}")
                with st.expander("エラー詳細"):
                    st.exception(e)
    else:
        pass


if __name__ == "__main__":
    main()