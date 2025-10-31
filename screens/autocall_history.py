"""
オートコール履歴画面モジュール
Business Data Processor

list_export*.csvからNegotiatesInfo形式のオートコール履歴を生成
"""
import streamlit as st
import pandas as pd
import io
from processors.autocall_history import AutocallHistoryProcessor


def render_autocall_history():
    """オートコール履歴作成画面"""

    st.title("オートコール履歴")
    st.subheader("list_export*.csvからNegotiatesInfo形式の履歴を生成します")

    # 説明
    with st.expander("📋 処理内容", expanded=True):
        st.markdown("""
        **入力**: `list_export*.csv`

        **処理仕様**:
        1. **最終架電日の空白処理**: 空白セルは1つ上の行の値でフォワードフィル
        2. **通話済除外**: 「架電結果」が「通話済」のレコードを除外

        **出力**: `MMDDオートコール履歴.csv`（NegotiatesInfo形式、10列）
        - 管理番号、交渉日時、担当、相手、手段、回収ランク、結果、入金予定日、予定金額、交渉備考
        - 交渉備考: `架電番号{架電番号}オートコール　残債{残債}円`
        """)

    # 相手プルダウン
    target_person = st.selectbox(
        "相手を選択してください",
        options=["契約者", "保証人", "連絡人", "勤務先"],
        key="autocall_history_target_person"
    )

    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "list_export*.csv をアップロードしてください",
        type="csv",
        key="autocall_history_file"
    )

    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")

        # CSVデータを読み込み（エンコーディング自動判定）
        file_data = uploaded_file.getvalue()
        try:
            df_input = pd.read_csv(io.BytesIO(file_data), encoding='cp932', low_memory=False)
        except UnicodeDecodeError:
            try:
                df_input = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis', low_memory=False)
            except UnicodeDecodeError:
                df_input = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig', low_memory=False)

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="autocall_history_process"):
            with st.spinner("処理中..."):
                try:
                    # プロセッサー呼び出し
                    processor = AutocallHistoryProcessor(target_person=target_person)
                    result_df = processor.process(df_input)
                    output_filename = processor.generate_output_filename()

                    # 成功メッセージ
                    st.success(f"✅ 処理完了: {len(result_df)}件のデータを生成しました")

                    # プレビュー表示
                    with st.expander("📊 データプレビュー（先頭10件）", expanded=True):
                        st.dataframe(result_df.head(10))

                    # CSVダウンロードボタン
                    csv_buffer = io.BytesIO()
                    result_df.to_csv(csv_buffer, index=False, encoding='cp932')
                    csv_buffer.seek(0)

                    st.download_button(
                        label=f"📥 {output_filename} をダウンロード",
                        data=csv_buffer,
                        file_name=output_filename,
                        mime="text/csv",
                        key="autocall_history_download",
                        type="primary"
                    )

                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
                    import traceback
                    with st.expander("詳細エラー情報"):
                        st.code(traceback.format_exc())
