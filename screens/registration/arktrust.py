"""
アークトラスト新規登録処理画面モジュール
Business Data Processor

アークトラスト用の新規登録処理画面
- 東京
"""

import streamlit as st
from components.result_display import display_processing_result, display_error_result
from services.registration import process_arktrust_data


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