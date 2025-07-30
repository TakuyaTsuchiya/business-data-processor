"""
Business Data Processor
統合データ処理システム - Streamlit版

対応システム:
- アーク新規登録データ変換
- ミライル契約者データ処理
- フェイス契約者データ処理
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

def main():
    st.set_page_config(
        page_title="Business Data Processor",
        page_icon="📊",
        layout="wide"
    )
    
    # ヘッダー
    st.title("📊 Business Data Processor")
    st.markdown("**統合データ処理システム** - CSVデータの自動変換・フィルタリングツール")
    
    # サイドバーで処理種別選択（フラット構造）
    st.sidebar.header("🔧 処理種別選択")
    processor_type = st.sidebar.selectbox(
        "処理種別を選択してください",
        [
            "選択してください",
            "─── 📞 オートコール処理 ───",
            "🏢 ミライル契約者（without10k）",
            "🏢 ミライル契約者（with10k）",
            "👥 ミライル保証人（without10k）",
            "👥 ミライル保証人（with10k）",
            "🚨 ミライル緊急連絡人（without10k）",
            "🚨 ミライル緊急連絡人（with10k）",
            "📱 フェイス契約者",
            "👥 フェイス保証人", 
            "🚨 フェイス緊急連絡人",
            "🏪 プラザ契約者",
            "👥 プラザ保証人", 
            "🚨 プラザ緊急連絡人",
            "─── 📱 SMS処理 ───",
            "📱 フェイスSMS退去済み契約者",
            "🔔 フェイスSMS（準備中）",
            "─── 📋 データ変換 ───",
            "📋 アーク新規登録"
        ]
    )
    
    # 区切り線項目は選択不可にして、ウェルカム画面を表示
    if (processor_type is None or 
        processor_type == "選択してください" or 
        processor_type.startswith("───")):
        # ウェルカム画面
        show_welcome_screen()
    elif processor_type == "🏢 ミライル契約者（without10k）":
        show_mirail_contract_without10k_processor()
    elif processor_type == "🏢 ミライル契約者（with10k）":
        show_mirail_contract_with10k_processor()
    elif processor_type == "👥 ミライル保証人（without10k）":
        show_mirail_guarantor_without10k_processor()
    elif processor_type == "👥 ミライル保証人（with10k）":
        show_mirail_guarantor_with10k_processor()
    elif processor_type == "🚨 ミライル緊急連絡人（without10k）":
        show_mirail_emergencycontact_without10k_processor()
    elif processor_type == "🚨 ミライル緊急連絡人（with10k）":
        show_mirail_emergencycontact_with10k_processor()
    elif processor_type == "📱 フェイス契約者":
        show_faith_contract_processor()
    elif processor_type == "👥 フェイス保証人":
        show_faith_guarantor_processor()
    elif processor_type == "🚨 フェイス緊急連絡人":
        show_faith_emergencycontact_processor()
    elif processor_type == "🏪 プラザ契約者":
        show_plaza_main_processor()
    elif processor_type == "👥 プラザ保証人":
        show_plaza_guarantor_processor()
    elif processor_type == "🚨 プラザ緊急連絡人":
        show_plaza_contact_processor()
    elif processor_type == "📱 フェイスSMS退去済み契約者":
        show_faith_sms_vacated_contract_processor()
    elif processor_type == "🔔 フェイスSMS（準備中）":
        show_faith_sms_processor()
    elif processor_type == "📋 アーク新規登録":
        show_ark_processor()

def show_welcome_screen():
    """ウェルカム画面の表示"""
    st.markdown("## 🏠 ようこそ")
    st.markdown("### 業務別データ処理システム")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📞 オートコール処理
        **対応システム:**
        - 🏢 ミライル（契約者/保証人/緊急連絡人 × without10k/with10k）
        - 📱 フェイス
        - 🏪 プラザ（契約者/保証人/緊急連絡人）
        
        **機能:**
        - ContractListから各種データを抽出
        - 条件別フィルタリング
        - オートコール用CSVを生成
        """)
    
    with col2:
        st.markdown("""
        ### 📱 SMS処理
        **対応システム:**
        - 🔔 フェイスSMS（準備中）
        
        **機能:**
        - SMS送信用データ処理
        - 電話番号の正規化
        - SMS用テンプレート生成
        """)
    
    with col3:
        st.markdown("""
        ### 📋 アーク新規登録
        **機能:**
        - 案件取込用レポートとContractListの統合
        - 住所分割・保証人判定を自動実行
        - 111列の統合CSVを生成
        - 重複チェック・データ変換
        """)
    
    st.info("👈 左側のメニューから業務カテゴリを選択してください")

def show_mirail_contract_without10k_processor():
    """ミライル契約者処理画面"""
    st.markdown("## 🏢 ミライルオートコール契約者データ処理")
    st.markdown("ContractListから契約者の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **滞納残債**: クライアントCD=1で1万円・1万1千円を除外
        - **TEL携帯**: 空でない値のみ
        - **入金予定金額**: 2,3,5,12を除外
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライルプロセッサーをインポート
                    from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_contract_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_contract_with10k_processor():
    """ミライル（残債含む）契約者処理画面"""
    st.markdown("## 🏢 ミライルオートコール契約者データ処理（残債含む全件）")
    st.markdown("ContractListから契約者の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **滞納残債**: 除外なし（1万円・1万1千円含む全件）
        - **TEL携帯**: 空でない値のみ
        - **入金予定金額**: 2,3,5,12を除外
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_contract_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 残債含有率の表示
                    if len(df_filtered) > 0 and "残債" in df_filtered.columns:
                        debt_10k = (df_filtered["残債"].astype(str) == "10000").sum()
                        debt_11k = (df_filtered["残債"].astype(str) == "11000").sum()
                        st.info(f"残債1万円: {debt_10k}件, 残債1万1千円: {debt_11k}件（除外されていません）")
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_contract_processor():
    """フェイス契約者処理画面"""
    st.markdown("## 📱 フェイス契約者オートコール")
    st.markdown("ContractListからフェイス委託先の契約者データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **入金予定金額**: 2,3,5を除外
        - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
        - **TEL携帯**: 空でない値のみ
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_contract_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_contract_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス契約者プロセッサーをインポート
                    from processors.faith_autocall.contract.standard import process_faith_contract_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_contract_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_guarantor_without10k_processor():
    """ミライル保証人（残債除外）処理画面"""
    st.markdown("## 👥 ミライル保証人オートコール（残債10,000円・11,000円除外）")
    st.markdown("ContractListから保証人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **クライアントコード**: 1のみ
        - **残債**: 10,000円・11,000円を除外
        - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_guarantor_without10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_guarantor_without10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル保証人（残債除外）プロセッサーをインポート
                    from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_guarantor_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_guarantor_with10k_processor():
    """ミライル保証人（残債含む）処理画面"""
    st.markdown("## 👥 ミライル保証人オートコール（残債含む全件）")
    st.markdown("ContractListから保証人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **クライアントコード**: 1のみ
        - **残債**: 除外なし（10,000円・11,000円含む全件）
        - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_guarantor_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_guarantor_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル保証人（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_guarantor_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_emergencycontact_without10k_processor():
    """ミライル緊急連絡人（残債除外）処理画面"""
    st.markdown("## 🚨 ミライル緊急連絡人オートコール（残債10,000円・11,000円除外）")
    st.markdown("ContractListから緊急連絡人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円除外）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **クライアントコード**: 1のみ
        - **残債**: 10,000円・11,000円を除外
        - **TEL携帯.2**: 空でない値のみ（緊急連絡人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_emergencycontact_without10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_emergencycontact_without10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル緊急連絡人（残債除外）プロセッサーをインポート
                    from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_emergencycontact_without10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_mirail_emergencycontact_with10k_processor():
    """ミライル緊急連絡人（残債含む）処理画面"""
    st.markdown("## 🚨 ミライル緊急連絡人オートコール（残債含む全件）")
    st.markdown("ContractListから緊急連絡人の電話番号を抽出し、オートコール用CSVを生成します（残債1万円・1万1千円含む）")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 空白のみ（直接管理案件）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **回収ランク**: 弁護士介入を除外
        - **クライアントコード**: 1のみ
        - **残債**: 除外なし（10,000円・11,000円含む全件）
        - **TEL携帯.2**: 空でない値のみ（緊急連絡人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="mirail_emergencycontact_with10k_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="mirail_emergencycontact_with10k_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # ミライル緊急連絡人（残債含む）プロセッサーをインポート
                    from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_mirail_emergencycontact_with10k_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_guarantor_processor():
    """フェイス保証人処理画面"""
    st.markdown("## 👥 フェイス保証人オートコール")
    st.markdown("ContractListからフェイス委託先の保証人データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **入金予定金額**: 2,3,5を除外
        - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
        - **TEL携帯.1**: 空でない値のみ（保証人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_guarantor_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_guarantor_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス保証人プロセッサーをインポート
                    from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_guarantor_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_emergencycontact_processor():
    """フェイス緊急連絡人処理画面"""
    st.markdown("## 🚨 フェイス緊急連絡人オートコール")
    st.markdown("ContractListからフェイス委託先の緊急連絡人データを抽出し、オートコール用CSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **委託先法人ID**: 1,2,3,4のみ（フェイス1〜4）
        - **入金予定日**: 前日以前またはNaN（当日は除外）
        - **入金予定金額**: 2,3,5を除外
        - **回収ランク**: 死亡決定、破産決定、弁護士介入を除外
        - **緊急連絡人１のTEL（携帯）**: 空でない値のみ（緊急連絡人電話番号）
        """)
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "ContractList_*.csv ファイルをアップロードしてください",
        type=['csv'],
        key="faith_emergencycontact_file"
    )
    
    if uploaded_file is not None:
        st.success(f"ファイルアップロード完了: {uploaded_file.name}")
        
        # ファイル情報表示
        file_size = len(uploaded_file.getvalue())
        st.info(f"ファイルサイズ: {file_size:,} bytes")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="faith_emergencycontact_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # フェイス緊急連絡人プロセッサーをインポート
                    from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data
                    
                    # ファイル内容を取得
                    file_content = uploaded_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_faith_emergencycontact_data(file_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # 委託先法人ID別の内訳表示
                    if len(df_filtered) > 0 and "委託先法人ID" in df_filtered.columns:
                        st.markdown("### 📊 委託先法人ID別内訳")
                        client_counts = df_filtered["委託先法人ID"].value_counts().sort_index()
                        for client_id, count in client_counts.items():
                            if pd.notna(client_id):
                                st.text(f"フェイス{int(client_id)}: {count}件")
                    
                    # プレビュー表示  
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_ark_processor():
    """アーク新規登録処理画面"""
    st.markdown("## 📋 アーク新規登録データ変換")
    st.markdown("案件取込用レポートとContractListを統合し、アーク新規登録用のCSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 主な処理機能"):
        st.markdown("""
        - **重複チェック**: ContractListとの照合により既存データを自動除外
        - **住所分割**: 都道府県、市区町村、残り住所に自動分割
        - **物件名処理**: 物件名から部屋番号を自動抽出・分離
        - **電話番号処理**: 自宅TELのみの場合は携帯TELに自動移動
        - **保証人・緊急連絡人判定**: 種別により自動判定
        - **金額計算**: 退去手続き費用の自動計算（最低70,000円保証）
        - **引継情報生成**: 督促手数料注意書き + 入居日
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 案件取込用レポート")
        report_file = st.file_uploader(
            "【東京支店】①案件取込用レポート*.csv",
            type=['csv'],
            key="ark_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📋 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="ark_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    if report_file is not None and contract_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="ark_process", type="primary"):
            with st.spinner("データを統合・変換中..."):
                try:
                    # アークプロセッサーをインポート
                    from processors.ark_registration import process_ark_data
                    
                    # ファイル内容を取得
                    report_content = report_file.getvalue()
                    contract_content = contract_file.getvalue()
                    
                    # データ処理実行
                    df_output, logs, output_filename = process_ark_data(report_content, contract_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    if len(df_output) > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("出力件数", len(df_output))
                        with col2:
                            phone_count = sum([
                                df_output["契約者TEL自宅"].notna().sum(),
                                df_output["契約者TEL携帯"].notna().sum()
                            ])
                            st.metric("電話番号件数", phone_count)
                        with col3:
                            room_count = df_output["部屋番号"].notna().sum()
                            st.metric("部屋番号あり", room_count)
                        
                        # データプレビュー表示
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        # 表示用に列を選択
                        preview_columns = [
                            "引継番号", "契約者氏名", "契約者カナ", 
                            "契約者TEL携帯", "物件名", "部屋番号",
                            "月額賃料", "退去手続き費用"
                        ]
                        available_columns = [col for col in preview_columns if col in df_output.columns]
                        st.dataframe(df_output[available_columns].head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                        
                        # 詳細統計情報
                        with st.expander("📈 詳細統計情報"):
                            st.markdown("#### データ品質統計")
                            
                            # 電話番号統計
                            home_tel_count = df_output["契約者TEL自宅"].notna().sum()
                            mobile_tel_count = df_output["契約者TEL携帯"].notna().sum()
                            st.text(f"契約者TEL自宅: {home_tel_count}件")
                            st.text(f"契約者TEL携帯: {mobile_tel_count}件")
                            
                            # 住所統計
                            if "契約者現住所1" in df_output.columns:
                                addr1_count = df_output["契約者現住所1"].notna().sum()
                                st.text(f"都道府県あり: {addr1_count}件")
                            
                            # 金額統計
                            if "月額賃料" in df_output.columns:
                                rent_avg = pd.to_numeric(df_output["月額賃料"], errors='coerce').mean()
                                if pd.notna(rent_avg):
                                    st.text(f"平均賃料: {rent_avg:,.0f}円")
                    else:
                        st.warning("⚠️ 処理対象データがありませんでした")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_main_processor():
    """プラザ契約者処理画面"""
    st.markdown("## 🏪 プラザ契約者オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ契約者のオートコール用CSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **2ファイル処理**: ContractList + Excel報告書の結合処理
        - **延滞額フィルター**: 0円、2円、3円、5円を除外
        - **TEL無効除外**: "TEL無効"を含むレコードを除外
        - **回収ランクフィルター**: 督促停止、弁護士介入を除外
        - **契約者TEL携帯**: 空でない値のみ（動的列検出）
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_main_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_main_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="plaza_main_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ契約者プロセッサーをインポート
                    from processors.plaza_autocall.main.standard import process_plaza_main_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_main_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_guarantor_processor():
    """プラザ保証人処理画面"""
    st.markdown("## 👥 プラザ保証人オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ保証人のオートコール用CSVを生成します")
    
    # 未実装の警告表示
    st.warning("⚠️ プラザ保証人処理は現在未実装です")
    st.info("📝 基本構造のみ提供されています。元のリポジトリに実装が見つかりませんでした。")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件（予定）"):
        st.markdown("""
        - **2ファイル処理**: ContractList + Excel報告書の結合処理
        - **延滞額フィルター**: 0円、2円、3円、5円を除外
        - **TEL無効除外**: "TEL無効"を含むレコードを除外
        - **回収ランクフィルター**: 督促停止、弁護士介入を除外
        - **保証人電話番号**: 空でない値のみ（要実装）
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_guarantor_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_guarantor_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始（基本構造のみ）", key="plaza_guarantor_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ保証人プロセッサーをインポート
                    from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_guarantor_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.warning("✅ 基本処理が完了しました（未実装警告）")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_plaza_contact_processor():
    """プラザ緊急連絡人処理画面"""
    st.markdown("## 🚨 プラザ緊急連絡人オートコール")
    st.markdown("ContractListとExcel報告書を組み合わせて、プラザ緊急連絡人のオートコール用CSVを生成します")
    
    # 処理条件の表示
    with st.expander("📋 フィルタリング条件"):
        st.markdown("""
        - **2ファイル処理**: ContractList + Excel報告書の結合処理
        - **延滞額フィルター**: 0円、2円、3円、5円を除外
        - **TEL無効除外**: "TEL無効"を含むレコードを除外
        - **回収ランクフィルター**: 督促停止、弁護士介入を除外
        - **緊急連絡人１のTEL（携帯）**: 空でない値のみ
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📄 ContractList")
        contract_file = st.file_uploader(
            "ContractList_*.csv",
            type=['csv'],
            key="plaza_contact_contract"
        )
        if contract_file:
            st.info(f"ファイルサイズ: {len(contract_file.getvalue()):,} bytes")
    
    with col2:
        st.markdown("### 📊 Excel報告書")
        report_file = st.file_uploader(
            "Excel報告書ファイル (*.xlsx)",
            type=['xlsx'],
            key="plaza_contact_report"
        )
        if report_file:
            st.info(f"ファイルサイズ: {len(report_file.getvalue()):,} bytes")
    
    if contract_file is not None and report_file is not None:
        st.success("✅ 両方のファイルアップロード完了")
        
        # 処理ボタン
        if st.button("🚀 処理開始", key="plaza_contact_process", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    # プラザ緊急連絡人プロセッサーをインポート
                    from processors.plaza_autocall.contact.standard import process_plaza_contact_data
                    
                    # ファイル内容を取得
                    contract_content = contract_file.getvalue()
                    report_content = report_file.getvalue()
                    
                    # データ処理実行
                    df_filtered, df_output, logs, output_filename = process_plaza_contact_data(contract_content, report_content)
                    
                    # 処理結果表示
                    st.success("✅ 処理が完了しました！")
                    
                    # 処理ログ表示
                    with st.expander("📊 処理ログ"):
                        for log in logs:
                            st.text(log)
                    
                    # 結果統計
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("フィルタリング後件数", len(df_filtered))
                    with col2:
                        st.metric("出力件数", len(df_output))
                    with col3:
                        st.metric("有効電話番号", df_output["電話番号"].notna().sum())
                    
                    # プレビュー表示
                    if len(df_output) > 0:
                        st.markdown("### 📋 出力データプレビュー（上位10件）")
                        st.dataframe(df_output.head(10), use_container_width=True)
                        
                        # CSVダウンロード
                        csv_data = df_output.to_csv(index=False, encoding='cp932')
                        st.download_button(
                            label="📥 CSVファイルをダウンロード",
                            data=csv_data.encode('cp932'),
                            file_name=output_filename,
                            mime="text/csv",
                            type="primary"
                        )
                    else:
                        st.warning("⚠️ フィルタリング条件に合致するデータがありません")
                        
                except ImportError as e:
                    st.error(f"モジュールインポートエラー: {e}")
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")
                    with st.expander("エラー詳細"):
                        st.exception(e)

def show_faith_sms_vacated_contract_processor():
    """フェイスSMS退去済み契約者処理画面"""
    st.markdown("## 📱 フェイスSMS退去済み契約者処理")
    st.markdown("**ContractList*.csv** から退去済み契約者のSMS送信用データを生成します")
    
    uploaded_file = st.file_uploader(
        "ContractList*.csv ファイルをアップロードしてください",
        type="csv",
        help="フェイス退去済み契約者データファイルを選択してください"
    )
    
    if uploaded_file is not None:
        if st.button("🚀 データ処理を開始", type="primary"):
            with st.spinner("データを処理中..."):
                try:
                    from processors.faith_sms.vacated_contract import process_faith_sms_vacated_contract_data
                    
                    file_content = uploaded_file.getvalue()
                    
                    with open("/tmp/temp_contractlist.csv", "wb") as f:
                        f.write(file_content)
                    
                    processed_df, output_filename, initial_rows, final_rows = process_faith_sms_vacated_contract_data("/tmp/temp_contractlist.csv")
                    
                    st.success(f"✅ 処理完了: {initial_rows}行 → {final_rows}行")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("### 📊 処理統計")
                        st.write(f"- 入力行数: {initial_rows:,}行")
                        st.write(f"- 出力行数: {final_rows:,}行")
                        st.write(f"- 除外行数: {initial_rows - final_rows:,}行")
                        st.write(f"- 出力列数: {len(processed_df.columns)}列")
                    
                    with col2:
                        st.markdown("### 🔍 データプレビュー")
                        st.dataframe(processed_df.head(3), use_container_width=True)
                    
                    csv_data = processed_df.to_csv(index=False, encoding='cp932')
                    
                    st.download_button(
                        label=f"📥 {output_filename} をダウンロード",
                        data=csv_data.encode('cp932'),
                        file_name=output_filename,
                        mime="text/csv"
                    )
                    
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {str(e)}")
                    st.info("💡 ファイル形式やエンコーディングを確認してください")

def show_faith_sms_processor():
    """フェイスSMS処理画面（準備中）"""
    st.markdown("## 🔔 フェイスSMS処理")
    st.warning("⚠️ フェイスSMS機能は準備中です")
    st.info("📝 この機能は将来のアップデートで追加予定です")

if __name__ == "__main__":
    main()