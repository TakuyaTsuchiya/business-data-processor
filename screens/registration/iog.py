"""
IOG新規登録処理画面モジュール
Business Data Processor

合同会社IOG（日本賃貸保証返却データ）用の新規登録処理画面
譲渡一覧ファイル複数対応版
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from services.registration import process_jid_data


def show_jid_registration():
    """IOG新規登録画面（譲渡一覧結合対応）"""
    timestamp = datetime.now().strftime("%m%d")

    st.title("📋 IOG新規登録")

    # フィルタ条件表示
    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• データ変換 → IOG返却データを111列テンプレートに変換")
    st.markdown("• 氏名マッチング → 譲渡一覧データと結合（スペース正規化）")
    st.markdown("• 同姓同名 → 最初にマッチした1件を使用")
    st.markdown("• 固定値設定 → 回収口座情報、ステータス情報などを自動設定")
    st.markdown('</div>', unsafe_allow_html=True)

    st.info("📂 必要ファイル: IOG返却データ（xlsx）1個 + 譲渡一覧（xls）0〜9個（任意）")

    # ファイルアップロード
    st.markdown("### ファイル1: IOG返却データ（必須）")
    iog_file = st.file_uploader(
        "合同会社IOG（日本賃貸保証返却データ）YYYYMMDD.xlsxをアップロード",
        type=["xlsx"],
        key="iog_file"
    )

    st.markdown("### ファイル2: 譲渡一覧（任意・複数選択可）")
    transfer_files = st.file_uploader(
        "譲渡一覧.xlsファイルをアップロード（複数選択可、0〜9個）",
        type=["xls"],
        accept_multiple_files=True,
        key="transfer_files"
    )

    # ファイル読み込み確認
    if iog_file:
        st.success(f"✅ IOGファイル: {iog_file.name} - 読み込み完了")

    if transfer_files:
        st.success(f"✅ 譲渡一覧: {len(transfer_files)}個のファイル - 読み込み完了")
        with st.expander("📁 譲渡一覧ファイルリスト"):
            for i, f in enumerate(transfer_files, 1):
                st.write(f"{i}. {f.name}")

    # 処理実行ボタン
    if iog_file:
        if st.button("処理を実行", type="primary", key="iog_process"):
            with st.spinner("処理中..."):
                try:
                    # ファイルデータ準備
                    iog_content = iog_file.read()

                    transfer_contents = None
                    if transfer_files:
                        transfer_contents = [f.read() for f in transfer_files]

                    # 処理実行
                    result_df, logs, filename = process_jid_data(iog_content, transfer_contents)

                    # ファイル名をカスタマイズ
                    custom_filename = f"{timestamp}iog_新規登録.csv"

                    # 結果表示
                    if not result_df.empty:
                        display_processing_result(result_df, logs, custom_filename)
                    else:
                        st.warning("データが生成されませんでした。")
                        if logs:
                            st.expander("📊 処理ログ", expanded=True)
                            for log in logs:
                                st.write(f"• {log}")

                except Exception as e:
                    display_error_result(f"エラーが発生しました: {str(e)}")
    else:
        st.warning("IOGファイル（xlsx）をアップロードしてください。")
