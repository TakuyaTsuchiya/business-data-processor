"""
フェイスオートコール画面モジュール
Business Data Processor

フェイス用のオートコール処理画面（3種類）
- 契約者
- 保証人
- 緊急連絡人
"""

import streamlit as st
from components.common_ui import display_filter_conditions
from components.result_display import display_processing_result, display_error_result
from services.autocall import (
    process_faith_contract_data,
    process_faith_guarantor_data,
    process_faith_emergencycontact_data
)


def show_faith_contract():
    st.header("フェイス契約者用オートコール")
    display_filter_conditions([
        "委託先法人ID → 1-4",
        "入金予定日 → 前日以前とNaN",
        "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
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
        "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
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
        "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
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