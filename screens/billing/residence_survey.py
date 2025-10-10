"""
居住訪問調査報告書 請求書作成画面モジュール
"""
import streamlit as st
import pandas as pd
import io
from processors.residence_survey.billing_processor import process_residence_survey_billing


def render_residence_survey_billing():
    """居住訪問調査報告書 請求書作成画面"""

    st.title("居住訪問調査報告書")
    st.subheader("弁護士法人ごとの請求書作成用データを生成します")

    # 説明
    with st.expander("📋 処理内容", expanded=True):
        st.markdown("""
        **入力**: Kintoneからエクスポートした `居住訪問調査報告書_*.csv`

        **処理内容**:
        - 提出日から対象月を自動判定
        - 弁護士法人ごとに請求データを生成
        - 調査回数に応じた請求行を作成（1回目のみ/2回目まで/3回目のみ）
        - 高橋裕次郎法律事務所の特例対応（3回目の場合は1〜3回目全て）
        - エリア外判定（鹿児島、大分、宮崎、沖縄、石川、新潟、秋田、北海道、青森、岩手、山形）

        **出力**: `【居住訪問調査報告書】YYYYMM請求内訳.xlsx`
        - 弁護士法人ごとにシート分割
        - 金額列は空白（手入力想定）
        """)

    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "居住訪問調査報告書_*.csv をアップロードしてください",
        type="csv",
        key="residence_survey_billing_file"
    )

    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")

        # CSVデータを読み込み（エンコーディング自動判定）
        file_data = uploaded_file.getvalue()
        try:
            df_preview = pd.read_csv(io.BytesIO(file_data), encoding='cp932')
        except UnicodeDecodeError:
            try:
                df_preview = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis')
            except UnicodeDecodeError:
                df_preview = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig')

        # 調査月の抽出
        available_months = set()
        for col in ['調査日時【１回目】', '調査日時【２回目】', '調査日時【３回目】']:
            if col in df_preview.columns:
                for date_val in df_preview[col].dropna():
                    try:
                        date = pd.to_datetime(date_val)
                        available_months.add(date.strftime('%Y%m'))
                    except:
                        continue

        # 調査月選択UI
        if available_months:
            sorted_months = sorted(available_months, reverse=True)
            month_options = {month: f"{month[:4]}年{month[4:]}月" for month in sorted_months}

            selected_month = st.selectbox(
                "調査月を選択してください",
                options=sorted_months,
                format_func=lambda x: month_options[x],
                key="residence_survey_month_select"
            )
        else:
            st.warning("⚠️ 調査日時データが見つかりませんでした。従来の提出日ベースで処理します。")
            selected_month = None

        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="residence_survey_billing_process"):
            with st.spinner("処理中..."):
                try:
                    # プロセッサー呼び出し（既に読み込んだdf_previewを使用）
                    excel_buffer, filename, message, logs = process_residence_survey_billing(
                        df_preview,
                        selected_month=selected_month
                    )

                    # 成功メッセージ
                    st.success(message)

                    # Excelダウンロードボタン
                    st.download_button(
                        label=f"📥 {filename} をダウンロード",
                        data=excel_buffer,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="residence_survey_billing_download",
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
