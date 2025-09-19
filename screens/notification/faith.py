"""
フェイス差込み用リスト画面モジュール
契約者、連帯保証人、緊急連絡人の各3種類（入居中/訴訟中、入居中/訴訟対象外、退去済み）の画面を統合
"""
import streamlit as st
import pandas as pd
import io
from components.common_ui import display_filter_conditions, safe_csv_download
from processors.faith_notification import process_faith_notification


def render_faith_notification():
    """フェイス差込み用リスト統合画面（9ボタン）"""
    
    st.title("📝 フェイス差込み用リスト作成")
    st.subheader("フェイス向けの郵送用リストを作成します")
    
    # 共通フィルタ条件表示
    with st.expander("📋 共通フィルタ条件", expanded=False):
        display_filter_conditions([
            "委託先法人ID = 1, 2, 3, 4",
            "入金予定日 < 本日",
            "入金予定金額 ≠ 2, 3, 5"
        ])
    
    # ファイルアップローダー
    uploaded_file = st.file_uploader(
        "ContractList_*.csv をアップロードしてください", 
        type="csv", 
        key="faith_notification_file"
    )
    
    if uploaded_file:
        st.success(f"✅ {uploaded_file.name}: 読み込み完了")
        
        # CSVデータを読み込み（ボタンクリック前に読み込んでおく）
        file_data = uploaded_file.read()
        
        # 処理実行関数
        def process_with_params(target_type, occupancy_status, filter_type):
            """指定されたパラメータで処理を実行"""
            with st.spinner("処理中..."):
                try:
                    # CSVデータを読み込み
                    try:
                        df = pd.read_csv(io.BytesIO(file_data), encoding='cp932')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='shift_jis')
                        except UnicodeDecodeError:
                            df = pd.read_csv(io.BytesIO(file_data), encoding='utf-8-sig')
                    
                    # プロセッサー呼び出し
                    result_df, filename, message, logs = process_faith_notification(
                        df, target_type, occupancy_status, filter_type
                    )
                    
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
        
        # 9ボタンのレイアウト
        st.markdown("---")
        st.subheader("処理対象を選択してください")
        
        # 契約者グループ
        st.markdown("#### 🏠 契約者")
        col1_1, col1_2, col1_3 = st.columns(3)
        with col1_1:
            if st.button("契約者「入居中」「訴訟中」", key="btn_c_1", use_container_width=True):
                process_with_params('contractor', '入居中', 'litigation_only')
        with col1_2:
            if st.button("契約者「入居中」「訴訟対象外」", key="btn_c_2", use_container_width=True):
                process_with_params('contractor', '入居中', 'litigation_excluded')
        with col1_3:
            if st.button("契約者「退去済み」", key="btn_c_3", use_container_width=True):
                process_with_params('contractor', '退去済み', 'evicted')
        
        # 連帯保証人グループ
        st.markdown("#### 👥 連帯保証人")
        col2_1, col2_2, col2_3 = st.columns(3)
        with col2_1:
            if st.button("連帯保証人「入居中」「訴訟中」", key="btn_g_1", use_container_width=True):
                process_with_params('guarantor', '入居中', 'litigation_only')
        with col2_2:
            if st.button("連帯保証人「入居中」「訴訟対象外」", key="btn_g_2", use_container_width=True):
                process_with_params('guarantor', '入居中', 'litigation_excluded')
        with col2_3:
            if st.button("連帯保証人「退去済み」", key="btn_g_3", use_container_width=True):
                process_with_params('guarantor', '退去済み', 'evicted')
        
        # 緊急連絡人グループ
        st.markdown("#### 📞 緊急連絡人")
        col3_1, col3_2, col3_3 = st.columns(3)
        with col3_1:
            if st.button("緊急連絡人「入居中」「訴訟中」", key="btn_e_1", use_container_width=True):
                process_with_params('contact', '入居中', 'litigation_only')
        with col3_2:
            if st.button("緊急連絡人「入居中」「訴訟対象外」", key="btn_e_2", use_container_width=True):
                process_with_params('contact', '入居中', 'litigation_excluded')
        with col3_3:
            if st.button("緊急連絡人「退去済み」", key="btn_e_3", use_container_width=True):
                process_with_params('contact', '退去済み', 'evicted')
        
        # 追加のフィルタ条件説明
        with st.expander("📋 各ボタンのフィルタ条件詳細", expanded=False):
            st.markdown("""
            **「入居中」「訴訟中」**
            - 入居ステータス = "入居中"
            - 回収ランク = "訴訟中" のみ抽出
            
            **「入居中」「訴訟対象外」**
            - 入居ステータス = "入居中" 
            - 回収ランク ≠ "破産決定", "死亡決定", "弁護士介入", "訴訟中"
            
            **「退去済み」**
            - 入居ステータス = "退去済み"
            - 回収ランク ≠ "死亡決定", "破産決定", "弁護士介入"
            
            **対象別の追加条件:**
            - 契約者: 契約者住所情報が完全（郵便番号・住所1-3すべて存在）
            - 連帯保証人: 保証人住所情報が完全、保証人1・2を分離して出力
            - 緊急連絡人: 連絡人住所情報が完全、連絡人1・2を分離して出力
            """)


# 旧関数との互換性のため残す（将来的に削除予定）
def render_faith_notification_contractor():
    """旧: フェイス差込み用リスト（契約者）画面"""
    render_faith_notification()


def render_faith_notification_guarantor():
    """旧: フェイス差込み用リスト（連帯保証人）画面"""
    render_faith_notification()


def render_faith_notification_contact():
    """旧: フェイス差込み用リスト（連絡人）画面"""
    render_faith_notification()