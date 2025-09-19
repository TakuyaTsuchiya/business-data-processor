"""
フェイス差込み用リスト画面モジュール
契約者、連帯保証人、緊急連絡人の3つの画面を定義
"""
import streamlit as st
import pandas as pd
import io
from components.common_ui import display_filter_conditions, safe_csv_download
from processors.faith_notification import process_faith_notification


def render_faith_notification_contractor():
    """フェイス差込み用リスト（契約者）画面"""
    
    st.title("📝 フェイス差込み用リスト（契約者）")
    st.subheader("フェイス向けの契約者宛て郵送用リストを作成します")
    
    # フィルタ条件表示
    display_filter_conditions([
        "委託先法人ID = 1, 2, 3, 4",
        "入金予定日 < 本日",
        "入金予定金額 ≠ 2, 3, 5",
        "回収ランク ≠ 死亡決定, 弁護士介入",
        "契約者住所情報が完全（郵便番号・住所1-3すべて存在）"
    ])
    
    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList_*.csv をアップロードしてください", 
        type="csv", 
        key="faith_notification_contractor_file"
    )
    
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
        
        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="faith_notification_contractor_process"):
            with st.spinner("処理中..."):
                try:
                    # CSVデータを読み込み
                    file_data = uploaded_file.read()
                    try:
                        df = pd.read_csv(io.BytesIO(file_data), encoding='cp932')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis')
                        except UnicodeDecodeError:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig')
                    
                    # プロセッサー呼び出し
                    result_df, filename, message, logs = process_faith_notification(df, 'contractor')
                    
                    # 成功メッセージ
                    st.success(message)
                    
                    # CSVダウンロードボタン
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # 結果プレビュー表示
                    if not result_df.empty:
                        st.subheader("処理結果プレビュー")
                        st.dataframe(result_df.head(10))
                    
                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")


def render_faith_notification_guarantor():
    """フェイス差込み用リスト（連帯保証人）画面"""
    
    st.title("📝 フェイス差込み用リスト（連帯保証人）")
    st.subheader("フェイス向けの連帯保証人宛て郵送用リストを作成します")
    
    # フィルタ条件表示
    display_filter_conditions([
        "委託先法人ID = 1, 2, 3, 4",
        "入金予定日 < 本日",
        "入金予定金額 ≠ 2, 3, 5",
        "回収ランク ≠ 死亡決定, 弁護士介入",
        "保証人住所情報が完全（郵便番号・住所1-3すべて存在）",
        "保証人1・保証人2を分離して出力（番号列で識別）"
    ])
    
    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList_*.csv をアップロードしてください", 
        type="csv", 
        key="faith_notification_guarantor_file"
    )
    
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
        
        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="faith_notification_guarantor_process"):
            with st.spinner("処理中..."):
                try:
                    # CSVデータを読み込み
                    file_data = uploaded_file.read()
                    try:
                        df = pd.read_csv(io.BytesIO(file_data), encoding='cp932')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis')
                        except UnicodeDecodeError:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig')
                    
                    # プロセッサー呼び出し
                    result_df, filename, message, logs = process_faith_notification(df, 'guarantor')
                    
                    # 成功メッセージ
                    st.success(message)
                    
                    # CSVダウンロードボタン
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # 結果プレビュー表示
                    if not result_df.empty:
                        st.subheader("処理結果プレビュー")
                        st.dataframe(result_df.head(10))
                    
                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")


def render_faith_notification_contact():
    """フェイス差込み用リスト（連絡人）画面"""
    
    st.title("📝 フェイス差込み用リスト（連絡人）")
    st.subheader("フェイス向けの緊急連絡人宛て郵送用リストを作成します")
    
    # フィルタ条件表示
    display_filter_conditions([
        "委託先法人ID = 1, 2, 3, 4",
        "入金予定日 < 本日",
        "入金予定金額 ≠ 2, 3, 5",
        "回収ランク ≠ 死亡決定, 弁護士介入",
        "連絡人住所情報が完全（郵便番号・住所1-3すべて存在）",
        "連絡人1・連絡人2を分離して出力（番号列で識別）"
    ])
    
    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList_*.csv をアップロードしてください", 
        type="csv", 
        key="faith_notification_contact_file"
    )
    
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
        
        # 処理実行ボタン
        if st.button("処理を実行", type="primary", key="faith_notification_contact_process"):
            with st.spinner("処理中..."):
                try:
                    # CSVデータを読み込み
                    file_data = uploaded_file.read()
                    try:
                        df = pd.read_csv(io.BytesIO(file_data), encoding='cp932')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis')
                        except UnicodeDecodeError:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig')
                    
                    # プロセッサー呼び出し
                    result_df, filename, message, logs = process_faith_notification(df, 'contact')
                    
                    # 成功メッセージ
                    st.success(message)
                    
                    # CSVダウンロードボタン
                    safe_csv_download(result_df, filename)
                    
                    # 処理ログ表示
                    if logs:
                        with st.expander("📊 処理ログ", expanded=False):
                            for log in logs:
                                st.write(f"• {log}")
                    
                    # 結果プレビュー表示
                    if not result_df.empty:
                        st.subheader("処理結果プレビュー")
                        st.dataframe(result_df.head(10))
                    
                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")