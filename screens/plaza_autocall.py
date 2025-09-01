"""
プラザオートコール画面モジュール
Business Data Processor

プラザ用のオートコール処理画面（3種類）
- 契約者（main）
- 保証人
- 緊急連絡人（contact）
"""

import streamlit as st
from components.common_ui import display_filter_conditions
from components.result_display import display_processing_result, display_error_result
from services.autocall import (
    process_plaza_main_data,
    process_plaza_guarantor_data,
    process_plaza_contact_data
)


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