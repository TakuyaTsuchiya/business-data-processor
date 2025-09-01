"""
アーク新規登録処理画面モジュール
Business Data Processor

アーク用の新規登録処理画面（4地域）
- 東京
- 大阪
- 北海道
- 北関東
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from processors.ark_registration import process_ark_data


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