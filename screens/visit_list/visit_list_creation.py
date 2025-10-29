"""
訪問リスト作成画面モジュール
Business Data Processor
"""
import streamlit as st
import pandas as pd
import io
from processors.visit_list.processor import process_visit_list


def render_visit_list_creation():
    """訪問リスト作成画面"""

    st.title("訪問リスト作成")
    st.subheader("訪問スタッフ用の訪問リストを作成します")

    # 説明
    with st.expander("処理内容", expanded=True):
        st.markdown("""
        **入力**: ContractList.csv

        **処理内容**:
        - 回収ランクでフィルタリング（「交渉困難」「死亡決定」「弁護士介入」のみ）
        - 入金予定日でフィルタリング（当日以前）
        - 入金予定金額でフィルタリング（2, 3, 5を除外）
        - 委託先法人IDでフィルタリング（5と空白のみ）
        - 住所がないレコードを除外
        - 5種類の人物別にシート分け（契約者、保証人1、保証人2、連絡人1、連絡人2）
        - 住所を結合（現住所1+2+3）
        - 都道府県順（北から南）に並び替え

        **出力**: `MMDD訪問リスト.xlsx`（5シート構成）
        """)

    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList.csv をアップロードしてください",
        type="csv",
        key="visit_list_file"
    )

    if uploaded_file:
        st.success(f"{uploaded_file.name}: 読み込み完了")

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="visit_list_process"):
            with st.spinner("処理中..."):
                try:
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()

                    # プロセッサー呼び出し
                    excel_content, logs, filename = process_visit_list(file_content)

                    # 成功メッセージ
                    st.success(f"{filename} の作成が完了しました")

                    # Excelダウンロードボタン
                    st.download_button(
                        label=f"{filename} をダウンロード",
                        data=excel_content,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="visit_list_download",
                        type="primary"
                    )

                    # 処理ログ表示
                    if logs:
                        with st.expander("処理ログ", expanded=True):
                            for log in logs:
                                st.text(log)

                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
                    import traceback
                    with st.expander("詳細エラー情報"):
                        st.code(traceback.format_exc())
