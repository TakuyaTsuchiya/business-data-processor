"""
カプコ新規登録処理画面モジュール
Business Data Processor

カプコ用の新規登録処理画面
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from processors.capco_registration import process_capco_data


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