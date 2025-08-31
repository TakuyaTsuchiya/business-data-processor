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

from processors.faith_sms.contract import process_faith_sms_contract_data
from processors.plaza_sms.contract import process_plaza_sms_contract_data
# 共通化されたSMS処理
from processors.sms_common.factory import (
    process_faith_guarantor_sms,
    process_faith_emergency_sms,
    process_mirail_contract_sms,
    process_mirail_guarantor_sms,
    process_mirail_emergency_sms,
    process_plaza_guarantor_sms,
    process_plaza_emergency_sms
)
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
        # ウェルカム画面
        st.markdown("""
## Welcome to Business Data Processor

#### 📞 オートコール用CSV加工
- **ミライル用** (6種類): 契約者・保証人・緊急連絡人（10,000円除外あり/なし）
- **フェイス用** (3種類): 契約者・保証人・緊急連絡人
- **プラザ用** (3種類): 契約者・保証人・緊急連絡人

#### 📱 SMS送信用CSV加工
- **ミライル用SMS** (3種類): 契約者・保証人・緊急連絡人
- **フェイス用SMS** (3種類): 契約者・保証人・緊急連絡人
- **プラザ用SMS** (3種類): 契約者・保証人・緊急連絡人（開発中）

#### 📋 新規登録用CSV加工
- アーク新規登録（東京・大阪・北海道・北関東）
- アークトラスト新規登録（東京）
- カプコ新規登録

#### 💰 残債の更新用CSV加工
- アーク残債の更新
- カプコ残債の更新
""")
        return
    
    # 各プロセッサーの処理画面
    processor = st.session_state.selected_processor
    if processor in PROCESSOR_MAPPING:
        PROCESSOR_MAPPING[processor]()

# 以下、各処理画面の関数を実装

def show_mirail_contract_without10k():
    st.header("ミライル契約者（10,000円を除外するパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_contract_without10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_mirail_contract_with10k():
    st.header("ミライル契約者（10,000円を除外しないパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN", 
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_contract_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_contract_with10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_mirail_guarantor_without10k():
    st.header("ミライル保証人（10,000円を除外するパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.1」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_guarantor_without10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_faith_sms_guarantor():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("フェイス　保証人")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="faith_sms_guarantor_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_sms_guarantor_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary", key="faith_sms_guarantor_process"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_faith_guarantor_sms(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                    # フィルタ条件表示
                    st.markdown("**フィルタ条件:**")
                    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
                    st.markdown("• 委託先法人ID → 1-4")
                    st.markdown("• 入金予定日 → 前日以前とNaN")
                    st.markdown("• 入金予定金額 → 2,3,5円除外")
                    st.markdown("• 回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外")
                    st.markdown("• AU列TEL携帯 → 090/080/070形式のみ（保証人電話番号）")
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 入金予定金額 → 2,3,5円除外")
    st.markdown("• 回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外")
    st.markdown("• AU列TEL携帯 → 090/080/070形式のみ（保証人電話番号）")
    st.markdown('</div>', unsafe_allow_html=True)

def show_faith_sms_emergency_contact():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("フェイス　連絡人")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="faith_sms_emergency_contact_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_sms_emergency_contact_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary", key="faith_sms_emergency_contact_process"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_faith_emergency_sms(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 入金予定金額 → 2,3,5円除外")
    st.markdown("• 回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外")
    st.markdown("• BE列「緊急連絡人１のTEL（携帯）」 → 090/080/070形式のみ")
    st.markdown('</div>', unsafe_allow_html=True)

def show_mirail_sms_guarantor():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("ミライル　保証人")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="mirail_sms_guarantor_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_sms_guarantor_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary", key="mirail_sms_guarantor_process"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_mirail_guarantor_sms(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• DO列　委託先法人ID → 5と空白セルのみ選択")
    st.markdown("• CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外")
    st.markdown("• BU列　入金予定日 → 前日以前が対象（当日は除外）")
    st.markdown("• BV列　入金予定金額 → 2,3,5,12を除外")
    st.markdown("• AU列　TEL携帯 → 090/080/070形式の携帯電話番号のみ")
    st.markdown('</div>', unsafe_allow_html=True)

def show_mirail_sms_contract():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("ミライル　契約者")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="mirail_sms_contract_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_sms_contract_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary", key="mirail_sms_contract_process"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_mirail_contract_sms(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• DO列　委託先法人ID → 5と空白セルのみ選択")
    st.markdown("• CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外")
    st.markdown("• BU列　入金予定日 → 前日以前が対象（当日は除外）")
    st.markdown("• BV列　入金予定金額 → 2,3,5,12を除外")
    st.markdown("• AB列　TEL携帯 → 090/080/070形式の携帯電話番号のみ")
    st.markdown('</div>', unsafe_allow_html=True)

def show_mirail_sms_emergencycontact():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("ミライル　連絡人")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="mirail_sms_emergencycontact_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_sms_emergencycontact_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary", key="mirail_sms_emergencycontact_process"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_mirail_emergency_sms(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• DO列　委託先法人ID → 5と空白セルのみ選択")
    st.markdown("• CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外")
    st.markdown("• BU列　入金予定日 → 前日以前が対象（当日は除外）")
    st.markdown("• BV列　入金予定金額 → 2,3,5,12を除外")
    st.markdown("• BE列　TEL携帯 → 090/080/070形式の携帯電話番号のみ")
    st.markdown('</div>', unsafe_allow_html=True)

def show_mirail_guarantor_with10k():
    st.header("ミライル保証人（10,000円を除外しないパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.1」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_guarantor_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_guarantor_with10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_without10k():
    st.header("ミライル緊急連絡人（10,000円を除外するパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "残債除外 → CD=1,4かつ滞納残債10,000円・11,000円除外",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.2」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_without10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_emergencycontact_without10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_mirail_emergency_with10k():
    st.header("ミライル緊急連絡人（10,000円を除外しないパターン）")
    display_filter_conditions([
        "委託先法人ID → 空白&5",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "滞納残債フィルタ → なし（全件処理）",
        "入金予定金額 → 2,3,5,12除外",
        "「TEL携帯.2」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="mirail_emergency_with10k_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_mirail_emergencycontact_with10k_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_faith_contract():
    st.header("フェイス契約者用オートコール")
    display_filter_conditions([
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_contract_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_faith_contract_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_faith_guarantor():
    st.header("フェイス保証人用オートコール")
    display_filter_conditions([
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯.1」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_guarantor_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_faith_guarantor_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_faith_emergency():
    st.header("フェイス緊急連絡人用オートコール")
    display_filter_conditions([
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「弁護士介入」除外",
        "入金予定金額 → 2,3,5,12除外",
        "滞納残債フィルタ → なし（全件処理）",
        "「TEL携帯.2」 → 空でない値のみ"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_emergency_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_faith_emergencycontact_data(uploaded_file.read())
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_plaza_main():
    st.header("プラザ契約者用オートコール")
    display_filter_conditions([
        "委託先法人ID → 6",
        "入金予定日 → 当日以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「TEL携帯」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_main_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_plaza_main_data(file_content)
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_plaza_guarantor():
    st.header("プラザ保証人用オートコール")
    display_filter_conditions([
        "委託先法人ID → 6",
        "入金予定日 → 前日以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「TEL携帯.1」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_guarantor_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_plaza_guarantor_data(file_content)
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_plaza_contact():
    st.header("プラザ緊急連絡人用オートコール")
    display_filter_conditions([
        "委託先法人ID → 6",
        "入金予定日 → 前日以前とNaN",
        "入金予定金額 → 2,3,5,12円除外",
        "「緊急連絡人１のTEL（携帯）」 → 空でない値のみ",
        "回収ランク → 「督促停止」「弁護士介入」除外"
    ])
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="plaza_contact_file")
    
    if uploaded_file is not None:
        try:
            # ファイル内容をbytesで読み取り
            file_content = uploaded_file.read()
            st.success(f"ファイルを読み込みました: {uploaded_file.name}")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, filename = process_plaza_contact_data(file_content)
                    
                # 共通コンポーネントで結果表示
                display_processing_result(result_df, logs, filename)
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_faith_sms_vacated():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("フェイス　契約者")
    
    # 支払期限日付入力
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),  # デフォルト値: 今日の日付
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key="faith_sms_payment_deadline",
        disabled=False,  # カレンダー選択は有効
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv", key="faith_sms_vacated_file")
    
    if uploaded_file is not None:
        try:
            st.success(f"✅ {uploaded_file.name}: 読み込み完了")
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    # 戻り値を一時変数で受け取る
                    result = process_faith_sms_contract_data(uploaded_file.read(), payment_deadline_date)
                    result_df, logs, filename, stats = result
                    
                if not result_df.empty:
                    st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示（データの有無に関わらず表示）
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
                    
                else:
                    st.warning("条件に合致するデータがありませんでした。")
                    
                    # 処理ログ表示（エラー時も表示）
                    display_processing_logs(logs, expanded=True)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    
    # フィルタ条件を常時表示（画面下部）
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• 委託先法人ID → 1-4")
    st.markdown("• 入金予定日 → 前日以前とNaN")
    st.markdown("• 入金予定金額 → 2,3,5円除外")
    st.markdown("• 回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外")
    st.markdown("• TEL携帯 → 090/080/070形式のみ")
    st.markdown('</div>', unsafe_allow_html=True)

def show_plaza_sms_contract():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("プラザ　契約者")
    
    display_filter_conditions([
        "委託先法人ID → 6のみ",
        "入金予定日 → 前日以前とNaN（当日除外）",
        "入金予定金額 → 2,3,5,12円除外",
        "回収ランク → 「弁護士介入」「死亡決定」「破産決定」除外",
        "契約者携帯電話 → 090/080/070のみ（固定電話・IP番号・空白除外）",
        "国籍 → VLOOKUP(引継番号↔会員番号)で日本/外国に分離",
        "支払期限 → 日付選択で指定"
    ])
    
    # 支払期限日付選択
    st.markdown("### 📅 支払期限の設定")
    payment_deadline = st.date_input(
        "支払期限日付を選択してください",
        value=date.today(),
        format="YYYY/MM/DD"
    )
    
    # 2ファイルアップロード
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: ContractList**")
        file1 = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="plaza_sms_contract_file1")
        if file1:
            st.success(f"✅ {file1.name}: 読み込み完了")
    with col2:
        st.markdown("**📄 ファイル2: コールセンター回収委託**")
        file2 = st.file_uploader("YYYY年MM月コールセンター回収委託.csvをアップロード", type="csv", key="plaza_sms_contract_file2")
        if file2:
            st.success(f"✅ {file2.name}: 読み込み完了")
    
    if file1 and file2:
        try:
            # ファイル内容を読み取り
            contract_content = file1.read()
            callcenter_content = file2.read()
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    japanese_df, foreign_df, logs, japanese_filename, foreign_filename, stats = process_plaza_sms_contract_data(
                        contract_content, callcenter_content, payment_deadline
                    )
                    
                # 処理結果表示
                st.success(f"処理完了: 日本人向け {len(japanese_df)}件, 外国人向け {len(foreign_df)}件")
                
                # ZIP一括ダウンロード
                st.markdown("### 📦 2つのCSVをダウンロード")
                
                # ZIPファイル作成
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    # 日本人向けCSV: Unicode文字列 → CP932バイト列
                    japanese_csv_string = japanese_df.to_csv(index=False)
                    japanese_csv_bytes = japanese_csv_string.encode('cp932', errors='replace')
                    zip_file.writestr(japanese_filename, japanese_csv_bytes)
                    
                    # 外国人向けCSV: Unicode文字列 → CP932バイト列
                    foreign_csv_string = foreign_df.to_csv(index=False)
                    foreign_csv_bytes = foreign_csv_string.encode('cp932', errors='replace')
                    zip_file.writestr(foreign_filename, foreign_csv_bytes)
                
                # ZIPファイルダウンロードボタン
                date_str = datetime.now().strftime("%m%d")
                zip_filename = f"{date_str}プラザ契約者SMS.zip"
                
                st.download_button(
                    label="📦 2つのCSVをダウンロード",
                    data=zip_buffer.getvalue(),
                    file_name=zip_filename,
                    mime="application/zip",
                    type="primary"
                )
                
                # 処理ログ表示
                if logs:
                    display_processing_logs(logs, expanded=True)
                
                # 統計情報表示
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("元データ件数", f"{stats.get('initial_rows', 0):,}件")
                with col2:
                    st.metric("処理後合計件数", f"{stats.get('processed_rows', 0):,}件")
                with col3:
                    st.metric("日本人向け", f"{stats.get('japanese_rows', 0):,}件")
                with col4:
                    st.metric("外国人向け", f"{stats.get('foreign_rows', 0):,}件")
                        
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")
    

def show_plaza_sms_guarantor():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("プラザ　保証人")
    
    display_filter_conditions([
        "委託先法人ID → 6のみ",
        "入金予定日 → 前日以前とNaN（当日除外）",
        "入金予定金額 → 2,3,5,12円除外",
        "回収ランク → 「弁護士介入」「死亡決定」「破産決定」除外",
        "AU列TEL携帯 → 090/080/070のみ（固定電話・IP番号・空白除外）",
        "支払期限 → 日付選択で指定"
    ])
    
    # 支払期限日付選択
    st.markdown("### 📅 支払期限の設定")
    payment_deadline = st.date_input(
        "支払期限日付を選択してください",
        value=date.today(),
        format="YYYY/MM/DD"
    )
    
    # ファイルアップロード
    st.markdown("**📄 ファイル: ContractList**")
    uploaded_file = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="plaza_sms_guarantor_file")
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
    
    if uploaded_file:
        try:
            # ファイル内容を読み取り
            file_content = uploaded_file.read()
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, output_filename, stats = process_plaza_guarantor_sms(
                        file_content, payment_deadline
                    )
                    
                # 処理結果表示
                st.success(f"処理完了: {len(result_df)}件のデータを出力")
                
                # ダウンロードボタン
                st.markdown("### 📥 CSVファイルをダウンロード")
                safe_csv_download(result_df, output_filename, "📥 プラザSMS保証人CSVをダウンロード")
                
                # 処理ログ表示
                if logs:
                    display_processing_logs(logs, expanded=True)
                
                # 統計情報表示
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("元データ件数", f"{stats.get('initial_rows', 0):,}件")
                with col2:
                    st.metric("処理後件数", f"{stats.get('processed_rows', 0):,}件")
                        
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

def show_plaza_sms_contact():
    st.title("📱 SMS送信用CSV加工")
    st.subheader("プラザ　連絡人")
    
    display_filter_conditions([
        "委託先法人ID → 6のみ",
        "入金予定日 → 前日以前とNaN（当日除外）",
        "入金予定金額 → 2,3,5,12円除外",
        "回収ランク → 「弁護士介入」「死亡決定」「破産決定」除外",
        "BE列緊急連絡人１TEL → 090/080/070のみ（固定電話・IP番号・空白除外）",
        "支払期限 → 日付選択で指定"
    ])
    
    # 支払期限日付選択
    st.markdown("### 📅 支払期限の設定")
    payment_deadline = st.date_input(
        "支払期限日付を選択してください",
        value=date.today(),
        format="YYYY/MM/DD"
    )
    
    # ファイルアップロード
    st.markdown("**📄 ファイル: ContractList**")
    uploaded_file = st.file_uploader("ContractList_*.csvをアップロード", type="csv", key="plaza_sms_contact_file")
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
    
    if uploaded_file:
        try:
            # ファイル内容を読み取り
            file_content = uploaded_file.read()
            
            if st.button("処理を実行", type="primary"):
                with st.spinner("処理中..."):
                    result_df, logs, output_filename, stats = process_plaza_emergency_sms(
                        file_content, payment_deadline
                    )
                    
                # 処理結果表示
                st.success(f"処理完了: {len(result_df)}件のデータを出力")
                
                # ダウンロードボタン
                st.markdown("### 📥 CSVファイルをダウンロード")
                safe_csv_download(result_df, output_filename, "📥 プラザSMS連絡人CSVをダウンロード")
                
                # 処理ログ表示
                if logs:
                    display_processing_logs(logs, expanded=True)
                
                # 統計情報表示
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("元データ件数", f"{stats.get('initial_rows', 0):,}件")
                with col2:
                    st.metric("処理後件数", f"{stats.get('processed_rows', 0):,}件")
                        
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

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