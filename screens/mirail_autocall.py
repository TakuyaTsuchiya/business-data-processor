"""
ミライルオートコール画面モジュール
Business Data Processor

ミライル用のオートコール処理画面（6種類）
- 契約者（10,000円除外あり/なし）
- 保証人（10,000円除外あり/なし）  
- 緊急連絡人（10,000円除外あり/なし）
"""

import streamlit as st
from components.common_ui import display_filter_conditions
from components.result_display import display_processing_result, display_error_result
from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data


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