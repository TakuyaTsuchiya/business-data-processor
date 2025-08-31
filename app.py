"""
Business Data Processor v2.3.0
統合データ処理システム - SMS機能削除版

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
import zipfile
from datetime import datetime, date

# 共通UIコンポーネントをインポート
from components.common_ui import (
    safe_dataframe_display,
    safe_csv_download,
    display_processing_logs,
    display_filter_conditions
)
from components.styles import get_custom_css
from components.sidebar import build_sidebar_menu
from components.result_display import display_processing_result, display_error_result
from components.welcome import show_welcome_screen

# screens import
from screens.mirail_autocall import (
    show_mirail_contract_without10k,
    show_mirail_contract_with10k,
    show_mirail_guarantor_without10k,
    show_mirail_guarantor_with10k,
    show_mirail_emergency_without10k,
    show_mirail_emergency_with10k
)
from screens.faith_autocall import (
    show_faith_contract,
    show_faith_guarantor,
    show_faith_emergency
)
from screens.plaza_autocall import (
    show_plaza_main,
    show_plaza_guarantor,
    show_plaza_contact
)
from screens.sms.faith import (
    show_faith_sms_vacated,
    show_faith_sms_guarantor,
    show_faith_sms_emergency_contact
)
from screens.sms.mirail import (
    show_mirail_sms_contract,
    show_mirail_sms_guarantor,
    show_mirail_sms_emergencycontact
)
from screens.sms.plaza import (
    show_plaza_sms_contract,
    show_plaza_sms_guarantor,
    show_plaza_sms_contact
)

# プロセッサーをインポート

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
    
    # プロセッサーのマッピング
    PROCESSOR_MAPPING = {
        "mirail_contract_without10k": show_mirail_contract_without10k,
        "mirail_contract_with10k": show_mirail_contract_with10k,
        "mirail_guarantor_without10k": show_mirail_guarantor_without10k,
        "mirail_guarantor_with10k": show_mirail_guarantor_with10k,
        "mirail_emergency_without10k": show_mirail_emergency_without10k,
        "mirail_emergency_with10k": show_mirail_emergency_with10k,
        "faith_contract": show_faith_contract,
        "faith_guarantor": show_faith_guarantor,
        "faith_emergency": show_faith_emergency,
        "plaza_main": show_plaza_main,
        "plaza_guarantor": show_plaza_guarantor,
        "plaza_contact": show_plaza_contact,
        "faith_sms_vacated": show_faith_sms_vacated,
        "faith_sms_guarantor": show_faith_sms_guarantor,
        "faith_sms_emergency_contact": show_faith_sms_emergency_contact,
        "mirail_sms_guarantor": show_mirail_sms_guarantor,
        "mirail_sms_emergencycontact": show_mirail_sms_emergencycontact,
        "mirail_sms_contract": show_mirail_sms_contract,
        "plaza_sms_contract": show_plaza_sms_contract,
        "plaza_sms_guarantor": show_plaza_sms_guarantor,
        "plaza_sms_contact": show_plaza_sms_contact,
        "ark_registration_tokyo": show_ark_registration_tokyo,
        "ark_registration_osaka": show_ark_registration_osaka,
        "ark_registration_hokkaido": show_ark_registration_hokkaido,
        "ark_registration_kitakanto": show_ark_registration_kitakanto,
        "arktrust_registration_tokyo": show_arktrust_registration_tokyo,
        "capco_registration": show_capco_registration,
        "ark_late_payment": show_ark_late_payment,
        "capco_debt_update": show_capco_debt_update
    }
    
    # カスタムCSSを適用
    st.markdown(get_custom_css(), unsafe_allow_html=True)
    
    # 固定ヘッダー
    st.title("📊 Business Data Processor")
    st.markdown("**📊 Business Data Processor** - 業務データ処理統合システム")
    
    # セッション状態の初期化
    if 'selected_processor' not in st.session_state:
        st.session_state.selected_processor = None
    
    # サイドバー：プルダウンレス常時表示メニュー
    build_sidebar_menu()
    
    # メインコンテンツエリア
    if st.session_state.selected_processor is None:
        show_welcome_screen()
        return
    
    # 各プロセッサーの処理画面
    processor = st.session_state.selected_processor
    if processor in PROCESSOR_MAPPING:
        PROCESSOR_MAPPING[processor]()

# 以下、各処理画面の関数を実装

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
                    
                # ダウンロードファイル名を指定
                timestamp = datetime.now().strftime("%m%d")
                filename = f"{timestamp}アーク_新規登録_東京.csv"
                
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    
                # ダウンロードファイル名を指定
                timestamp = datetime.now().strftime("%m%d")
                filename = f"{timestamp}アーク_新規登録_大阪.csv"
                
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    
                # ダウンロードファイル名を指定
                timestamp = datetime.now().strftime("%m%d")
                filename = f"{timestamp}アーク_新規登録_北海道.csv"
                
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    
                # ダウンロードファイル名を指定
                timestamp = datetime.now().strftime("%m%d")
                filename = f"{timestamp}アーク_新規登録_北関東.csv"
                
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    result_df, logs, filename = process_arktrust_data(file_contents[0], file_contents[1])
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    
                # ダウンロードファイル名を再設定（関数からのファイル名を使用）
                if filename:
                    # 共通コンポーネントで結果表示
                    display_processing_result(result_df, logs, filename)
                else:
                    # ファイル名がない場合はデフォルトを設定
                    timestamp = datetime.now().strftime("%m%d")
                    filename = f"{timestamp}カプコ_新規登録.csv"
                    display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                    # 共通コンポーネントで結果表示
                    display_processing_result(result_df, [], output_filename)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
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
                display_error_result(f"エラーが発生しました: {str(e)}")
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