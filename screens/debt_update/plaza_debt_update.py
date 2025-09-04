"""
プラザ残債更新処理画面モジュール

プラザの残債更新処理画面
- 管理前滞納額情報CSV
- 交渉履歴CSV
の2つのファイルを生成
"""

import streamlit as st
import io
import zipfile
from datetime import datetime
from components.common_ui import display_processing_logs
from components.result_display import display_processing_result, display_error_result
from processors.plaza_debt_update import process_plaza_debt_update


def show_plaza_debt_update():
    """プラザ残債更新処理画面"""
    
    st.header("💰 プラザ残債の更新")
    st.subheader("管理前滞納額情報・交渉履歴CSVの生成")
    
    st.info("📂 必要ファイル: 前日・当日のコールセンター回収委託情報（Excel） + 1241件.csv")
    
    # 3つのファイルアップロード
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📄 前日の回収委託情報**")
        yesterday_file = st.file_uploader(
            "前日のExcelファイル",
            type=["xlsx"],
            key="plaza_yesterday",
            help="コールセンター回収委託_入金退去情報_YYYYMMDD_ミライル委託分.xlsx"
        )
    
    with col2:
        st.markdown("**📄 当日の回収委託情報**")
        today_file = st.file_uploader(
            "当日のExcelファイル",
            type=["xlsx"],
            key="plaza_today",
            help="コールセンター回収委託_入金退去情報_YYYYMMDD_ミライル委託分.xlsx"
        )
    
    with col3:
        st.markdown("**📄 プラザ依頼分リスト**")
        plaza_list_file = st.file_uploader(
            "1241件.csv",
            type=["csv"],
            key="plaza_list",
            help="引継番号と管理番号の対応表"
        )
    
    # ファイルが全てアップロードされたら処理実行
    if yesterday_file and today_file and plaza_list_file:
        st.success(f"✅ {yesterday_file.name}: 読み込み完了")
        st.success(f"✅ {today_file.name}: 読み込み完了")
        st.success(f"✅ {plaza_list_file.name}: 読み込み完了")
        
        if st.button("🚀 処理を開始", type="primary", use_container_width=True):
            try:
                with st.spinner("処理中..."):
                    # プロセッサー実行
                    outputs, filenames, logs, stats = process_plaza_debt_update(
                        yesterday_file.read(),
                        today_file.read(),
                        plaza_list_file.read()
                    )
                    
                    # ログ表示
                    display_processing_logs(logs)
                    
                    # 統計情報表示
                    if stats:
                        st.info(f"""
                        📊 処理結果:
                        - 総レコード数: {stats['total_records']}件
                        - 管理番号マッチ: {stats['management_matched']}件
                        - 入金総額: {stats['total_payment']:,}円
                        - 入金あり: {stats['positive_payments']}件
                        - 入金なし: {stats['zero_payments']}件
                        - 残債増加: {stats['negative_payments']}件
                        """)
                    
                    # 2つのCSVをZIPにまとめてダウンロード
                    if len(outputs) == 2 and len(filenames) == 2:
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                            # 1つ目のCSV（管理前滞納額情報）
                            csv1_content = outputs[0].to_csv(index=False, encoding='cp932')
                            zip_file.writestr(filenames[0], csv1_content.encode('cp932'))
                            
                            # 2つ目のCSV（交渉履歴）
                            csv2_content = outputs[1].to_csv(index=False, encoding='cp932')
                            zip_file.writestr(filenames[1], csv2_content.encode('cp932'))
                        
                        # ZIPファイルのダウンロードボタン
                        zip_buffer.seek(0)
                        date_str = datetime.now().strftime("%m%d")
                        zip_filename = f"{date_str}プラザ残債更新.zip"
                        
                        st.download_button(
                            label="📦 2つのCSVをダウンロード（ZIP）",
                            data=zip_buffer.getvalue(),
                            file_name=zip_filename,
                            mime="application/zip",
                            type="primary",
                            use_container_width=True
                        )
                    
            except Exception as e:
                display_error_result(f"エラーが発生しました: {str(e)}")