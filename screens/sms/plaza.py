"""
プラザSMS処理画面モジュール
Business Data Processor

プラザ用のSMS処理画面（3種類）
- 契約者
- 保証人
- 緊急連絡人
"""

import streamlit as st
import io
import zipfile
from datetime import datetime, date
from components.common_ui import (
    safe_csv_download,
    display_processing_logs,
    display_filter_conditions
)
from components.result_display import display_error_result
from processors.plaza_sms.contract import process_plaza_sms_contract_data
from processors.sms_common.factory import (
    process_plaza_guarantor_sms,
    process_plaza_emergency_sms
)


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