"""
アーク遅延損害金処理画面モジュール
Business Data Processor

アーク残債の更新処理画面
"""

import streamlit as st
from components.result_display import display_processing_result, display_error_result
from services.debt_update import process_ark_late_payment_data


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