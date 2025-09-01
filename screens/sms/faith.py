"""
フェイスSMS処理画面モジュール
Business Data Processor

フェイス用のSMS処理画面（3種類）
- 契約者（退去済み）
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
from services.sms import (
    process_faith_sms_contract_data,
    process_faith_sms_guarantor_data,
    process_faith_sms_emergency_contact_data
)


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
                    result = process_faith_sms_guarantor_data(uploaded_file.read(), payment_deadline_date)
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
                    result = process_faith_sms_emergency_contact_data(uploaded_file.read(), payment_deadline_date)
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