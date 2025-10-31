"""
訪問リスト作成画面モジュール
Business Data Processor

ContractList.csvから訪問スタッフ用の訪問リストExcelを生成
"""
import streamlit as st
import pandas as pd
import io
from processors.visit_list.processor import process_visit_list


def render_visit_list():
    """訪問リスト作成画面"""

    st.title("訪問リスト作成")
    st.subheader("ContractList.csvから訪問スタッフ用の訪問リストを生成します")

    # 説明
    with st.expander("📋 処理内容", expanded=True):
        st.markdown("""
        **入力**: `ContractList.csv`

        **フィルタ条件**:
        1. **回収ランク**: 「交渉困難」「死亡決定」「弁護士介入」を**除外**
        2. **入金予定日**: 当日以前または空白
        3. **入金予定金額**: 2, 3, 5を除外
        4. **委託先法人ID**: 5と空白のみ
        5. **現住所1**: 空白を除外
        6. **滞納残債**: 1円以上
        7. **クライアントCD**: 9268を除外
        8. **受託状況**: 「契約中」「契約中(口振停止)」

        **出力**: `MMDD訪問リスト.xlsx`（游ゴシック Regular 11pt）
        - 5シート構成: 契約者、保証人1、保証人2、連絡人1、連絡人2
        - 各シート22列: 管理番号、最新契約種類、受託状況、入居ステータス、滞納ステータス、
          退去手続き（実費）、営業担当者、氏名（※）、結合住所、現住所1/2/3、滞納残債、
          入金予定日、入金予定金額、月額賃料合計、回収ランク、クライアントCD、
          クライアント名、委託先法人ID、委託先法人名、解約日
        - ※氏名列は各シートで異なる（契約者氏名/保証人１氏名/保証人２氏名/緊急連絡人１氏名/緊急連絡人２氏名）
        - 都道府県順（北→南）でソート
        """)

    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList.csv をアップロードしてください",
        type="csv",
        key="visit_list_file"
    )

    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")

        # CSVデータを読み込み（エンコーディング自動判定）
        file_data = uploaded_file.getvalue()
        try:
            df_preview = pd.read_csv(io.BytesIO(file_data), encoding='cp932', low_memory=False)
        except UnicodeDecodeError:
            try:
                df_preview = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis', low_memory=False)
            except UnicodeDecodeError:
                df_preview = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig', low_memory=False)

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="visit_list_process"):
            with st.spinner("処理中..."):
                try:
                    # プロセッサー呼び出し
                    excel_buffer, filename, message, logs = process_visit_list(df_preview)

                    if excel_buffer is None:
                        st.warning(message)
                    else:
                        # 成功メッセージ
                        st.success(message)

                        # Excelダウンロードボタン
                        st.download_button(
                            label=f"📥 {filename} をダウンロード",
                            data=excel_buffer,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="visit_list_download",
                            type="primary"
                        )

                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=True):
                            for log in logs:
                                st.write(f"• {log}")

                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
                    import traceback
                    with st.expander("詳細エラー情報"):
                        st.code(traceback.format_exc())
