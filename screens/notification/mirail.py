"""
ミライル（フェイス封筒）差込み用リスト画面モジュール
契約者、保証人、連絡人の郵送用リストを作成する画面
"""
import streamlit as st
import pandas as pd
import io
from openpyxl import Workbook
from openpyxl.styles import Font
from components.common_ui import display_filter_conditions, display_processing_logs
from processors.mirail_notification import process_mirail_notification


def render_mirail_notification(target_type: str, client_pattern: str):
    """ミライル（フェイス封筒）共通レンダリング処理"""

    # タイトル設定
    target_name_map = {
        'contractor': '契約者',
        'guarantor': '保証人',
        'contact': '連絡人'
    }
    target_name = target_name_map.get(target_type, target_type)
    pattern_text = "（1,4,5）" if client_pattern == 'included' else "（1,4,5以外）"

    st.title("📝 ミライル 催告書 差し込みリスト")
    st.subheader(f"{target_name}{pattern_text}のリストを作成")

    # フィルタ条件表示
    with st.expander("📋 フィルタ条件", expanded=True):
        base_conditions = [
            "委託先法人ID = 5と空白のみ選択",
            "入金予定日 < 本日（本日を除く）",
            "入金予定金額 = 2, 3, 5, 12を除外",
            "回収ランク ≠ 弁護士介入（除外）"
        ]

        # クライアントCDフィルタ
        if client_pattern == 'included':
            base_conditions.append("クライアントCD = 1, 4, 5を選択")
        else:
            base_conditions.append("クライアントCD ≠ 1, 4, 5, 10, 40（すべて除外）")

        # 住所フィルタ
        if target_type == 'contractor':
            base_conditions.append("契約者住所（郵便番号・現住所1・2・3）がすべて入力されている")
        elif target_type == 'guarantor':
            base_conditions.append("保証人1または保証人2の住所が完全（少なくとも一方の郵送が可能）")
        else:  # contact
            base_conditions.append("緊急連絡人1または緊急連絡人2の住所が完全（少なくとも一方の郵送が可能）")

        display_filter_conditions(base_conditions)

    # ファイルアップロード
    st.markdown("### 📂 ファイルアップロード")
    uploaded_file = st.file_uploader(
        "ContractList*.csvファイルを選択してください",
        type=['csv'],
        key=f"mirail_{target_type}_{client_pattern}"
    )

    if uploaded_file is not None:
        # 処理実行ボタン
        if st.button("🚀 処理を実行", type="primary", key=f"process_mirail_{target_type}_{client_pattern}"):
            with st.spinner("処理中..."):
                try:
                    # ファイル読み込み
                    file_content = uploaded_file.read()

                    # プロセッサー実行
                    result_df, filename, message, logs = process_mirail_notification(
                        file_content, target_type, client_pattern
                    )

                    # ログ表示
                    display_processing_logs(logs)

                    # 結果処理
                    if result_df is not None and len(result_df) > 0:
                        # 成功メッセージ
                        st.success(message)

                        # データプレビュー
                        with st.expander(f"📊 データプレビュー（先頭10件）", expanded=True):
                            st.dataframe(result_df.head(10))

                        # Excel形式でダウンロード（游ゴシック 12pt）
                        output = io.BytesIO()

                        # openpyxlで直接ワークブックを作成
                        wb = Workbook()
                        ws = wb.active
                        ws.title = 'Sheet1'

                        # フォント設定（游ゴシック Regular 12pt、罫線なし）
                        custom_font = Font(
                            name='游ゴシック',  # Yu Gothic / 游ゴシック体 / YuGothic
                            size=12,
                            bold=False
                        )

                        # ヘッダーを書き込み（フォント適用）
                        for col_num, column_title in enumerate(result_df.columns, 1):
                            cell = ws.cell(row=1, column=col_num, value=column_title)
                            cell.font = custom_font

                        # データを書き込み（フォント適用）
                        for row_num, row_data in enumerate(result_df.values, 2):
                            for col_num, cell_value in enumerate(row_data, 1):
                                # NaNやNoneの処理
                                if pd.isna(cell_value):
                                    cell_value = ''
                                cell = ws.cell(row=row_num, column=col_num, value=cell_value)
                                cell.font = custom_font

                        # 保存
                        wb.save(output)
                        output.seek(0)

                        st.download_button(
                            label=f"📥 {filename}をダウンロード",
                            data=output.getvalue(),
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            key=f"download_mirail_{target_type}_{client_pattern}"
                        )
                    else:
                        # エラーメッセージ（0件の場合）
                        st.error(message)

                except Exception as e:
                    st.error(f"処理中にエラーが発生しました: {str(e)}")


# app.pyから呼ばれる6つの関数
def show_mirail_contractor_145():
    """契約者（1,4,5）画面"""
    render_mirail_notification('contractor', 'included')


def show_mirail_contractor_not145():
    """契約者（1,4,5以外）画面"""
    render_mirail_notification('contractor', 'excluded')


def show_mirail_guarantor_145():
    """保証人（1,4,5）画面"""
    render_mirail_notification('guarantor', 'included')


def show_mirail_guarantor_not145():
    """保証人（1,4,5以外）画面"""
    render_mirail_notification('guarantor', 'excluded')


def show_mirail_contact_145():
    """連絡人（1,4,5）画面"""
    render_mirail_notification('contact', 'included')


def show_mirail_contact_not145():
    """連絡人（1,4,5以外）画面"""
    render_mirail_notification('contact', 'excluded')