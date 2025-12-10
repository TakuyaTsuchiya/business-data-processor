"""
ファイン履歴画面モジュール
Business Data Processor

携帯Mirail社納品データ_*.csvからNegotiatesInfo形式のファイン履歴を生成
"""
import streamlit as st
import pandas as pd
import io
from processors.fine_history import FineHistoryProcessor
from components.file_utils import read_csv_with_encoding


def render_fine_history():
    """ファイン履歴作成画面"""

    st.title("ファイン履歴")
    st.subheader("携帯Mirail社納品データからNegotiatesInfo形式の履歴を生成します")

    # 説明
    with st.expander("処理内容", expanded=True):
        st.markdown("""
        **入力**: `携帯Mirail社納品データ_*.csv`

        **処理仕様**:
        1. 管理番号が空の行を除外
        2. 発信日 + 発信時刻を結合（秒を削除）

        **出力**: `MMDDファイン履歴.csv`（NegotiatesInfo形式、10列）
        - 管理番号、交渉日時、担当、相手、手段、回収ランク、結果、入金予定日、予定金額、交渉備考
        - 交渉備考: `{架電先}オートコール`
        """)

    # 相手プルダウン
    target_person = st.selectbox(
        "相手を選択してください",
        options=["契約者", "保証人", "連絡人", "勤務先"],
        key="fine_history_target_person"
    )

    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "携帯Mirail社納品データ_*.csv をアップロードしてください",
        type="csv",
        key="fine_history_file"
    )

    if uploaded_file:
        st.success(f"{uploaded_file.name}: 読み込み完了")

        # CSVデータを読み込み（エンコーディング自動判定）
        file_data = uploaded_file.getvalue()
        try:
            df_input = read_csv_with_encoding(file_data, low_memory=False)
        except ValueError as e:
            st.error(f"ファイル読み込みエラー: {str(e)}")
            return

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="fine_history_process"):
            with st.spinner("処理中..."):
                try:
                    # プロセッサー呼び出し
                    processor = FineHistoryProcessor(target_person=target_person)
                    result_df = processor.process(df_input)
                    csv_bytes, logs = processor.generate_csv(result_df)
                    output_filename = processor.generate_output_filename()

                    # 成功メッセージ
                    st.success(f"処理完了: {len(result_df)}件のデータを生成しました")

                    # プレビュー表示
                    with st.expander("データプレビュー（先頭10件）", expanded=True):
                        st.dataframe(result_df.head(10))

                    # 処理ログ表示
                    if logs:
                        with st.expander("処理ログ"):
                            for log in logs:
                                st.write(f"・ {log}")

                    # CSVダウンロードボタン
                    st.download_button(
                        label=f"{output_filename} をダウンロード",
                        data=csv_bytes,
                        file_name=output_filename,
                        mime="text/csv",
                        key="fine_history_download",
                        type="primary"
                    )

                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
                    import traceback
                    with st.expander("詳細エラー情報"):
                        st.code(traceback.format_exc())
