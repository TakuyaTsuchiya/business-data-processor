"""
ミライルSMS処理画面モジュール
Business Data Processor

ミライル用のSMS処理画面（3種類）
- 契約者
- 保証人
- 緊急連絡人
"""

import streamlit as st
from datetime import date
from components.common_ui import (
    safe_csv_download,
    display_processing_logs
)
from components.result_display import display_error_result
from processors.sms_common.factory import (
    process_mirail_contract_sms,
    process_mirail_guarantor_sms,
    process_mirail_emergency_sms
)


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