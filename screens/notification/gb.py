"""
ガレージバンク催告書 契約者 画面モジュール
"""
import streamlit as st
import pandas as pd
import io
from components.common_ui import display_filter_conditions, safe_excel_download
from processors.gb_notification import process_gb_notification


def show_gb_notification():
    """ガレージバンク催告書 契約者 差し込みリスト 画面"""
    st.title("ガレージバンク催告書 契約者 差し込みリスト")
    st.subheader("ガレージバンク向けの郵送用リストを作成します")

    # フィルタ条件表示
    with st.expander("フィルタ条件", expanded=True):
        display_filter_conditions([
            "委託先法人ID = 7",
            "入金予定日 < 本日（空白含む）",
            "滞納残債 >= 1円",
            "入金予定金額 = 2, 3, 5を除外",
            "回収ランク: 死亡決定, 破産決定, 弁護士介入を除外",
            "住所: 郵便番号・現住所1-3が全て入力済"
        ])

    st.markdown("---")

    # ファイルアップローダー（2つ）
    st.markdown("### ファイルアップロード")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**1. ContractList**")
        contract_file = st.file_uploader(
            "ContractList_*.csv をアップロード",
            type="csv",
            key="gb_notification_contract"
        )

    with col2:
        st.markdown("**2. 情報連携シート**")
        billing_file = st.file_uploader(
            "情報連携シート_*.xlsx をアップロード",
            type="xlsx",
            key="gb_notification_billing"
        )

    # 両方のファイルがアップロードされた場合
    if contract_file and billing_file:
        st.success(f"ContractList: {contract_file.name}")
        st.success(f"情報連携シート: {billing_file.name}")

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="gb_notification_process"):
            with st.spinner("処理中..."):
                try:
                    # ContractList読み込み
                    contract_data = contract_file.read()
                    try:
                        contract_df = pd.read_csv(io.BytesIO(contract_data), encoding='cp932', header=None)
                    except UnicodeDecodeError:
                        try:
                            contract_df = pd.read_csv(io.BytesIO(contract_data), encoding='shift_jis', header=None)
                        except UnicodeDecodeError:
                            contract_df = pd.read_csv(io.BytesIO(contract_data), encoding='utf-8-sig', header=None)

                    # ヘッダー行を除外（1行目がヘッダーの場合）
                    if contract_df.iloc[0, 0] == '管理番号' or str(contract_df.iloc[0, 0]).startswith('管理'):
                        contract_df = contract_df.iloc[1:].reset_index(drop=True)

                    # 情報連携シート読み込み（01_請求データ）
                    billing_df = pd.read_excel(
                        billing_file,
                        sheet_name='01_請求データ',
                        header=0
                    )

                    # プロセッサー呼び出し
                    result_df, filename, message, logs = process_gb_notification(
                        contract_df, billing_df
                    )

                    # 成功メッセージ
                    st.success(message)

                    # Excelダウンロードボタン
                    if not result_df.empty:
                        safe_excel_download(result_df, filename)

                    # 処理ログ表示
                    if logs:
                        with st.expander("処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"- {log}")

                    # 結果プレビュー表示
                    if not result_df.empty:
                        st.subheader("処理結果プレビュー")
                        st.dataframe(result_df.head(10))
                    else:
                        st.warning("出力対象のデータがありませんでした")

                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
    else:
        st.info("2つのファイルをアップロードしてください")
