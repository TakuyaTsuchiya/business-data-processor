"""
カプコ債務更新処理画面モジュール
Business Data Processor

カプコ残債の更新処理画面
"""

import streamlit as st
import time
from components.common_ui import safe_csv_download, display_processing_logs
from components.result_display import display_error_result
from services.debt_update import process_capco_debt_update


def show_capco_debt_update():
    st.header("💰 カプコ残債の更新")
    st.info("📂 必要ファイル: csv_arrear_*.csv + ContractList_*.csv（2ファイル処理）")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📄 ファイル1: カプコ滞納データ**")
        file1 = st.file_uploader(
            "csv_arrear_*.csvをアップロード", type="csv", key="capco_debt_file1"
        )
    with col2:
        st.markdown("**📄 ファイル2: ContractList**")
        file2 = st.file_uploader(
            "ContractList_*.csvをアップロード", type="csv", key="capco_debt_file2"
        )

    if file1 and file2:
        st.success(f"✅ {file1.name}: 読み込み完了")
        st.success(f"✅ {file2.name}: 読み込み完了")

        if st.button("処理を実行", type="primary"):
            try:
                # ファイル内容を読み取り
                file_contents = [file1.read(), file2.read()]

                # プログレスバーとステータステキストを作成
                progress_bar = st.progress(0)
                status_text = st.empty()

                # プログレスコールバック関数
                def update_progress(progress, message):
                    progress_bar.progress(progress)
                    status_text.text(message)

                result_df, output_filename, stats, logs = process_capco_debt_update(
                    file_contents[0],
                    file_contents[1],
                    progress_callback=update_progress,
                )

                # プログレスバーを完了状態に
                progress_bar.progress(1.0)
                status_text.text("処理完了！")

                # 少し待ってからプログレスバーを非表示に
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()

                # ログ表示
                display_processing_logs(logs)

                if len(result_df) > 0:
                    st.success(f"✅ 処理完了: {len(result_df)}件のデータを出力します")

                    # ダウンロード機能
                    safe_csv_download(result_df, output_filename)

                    # 処理統計情報の詳細表示
                    with st.expander("📊 処理統計情報", expanded=True):
                        st.markdown("**処理統計情報:**")
                        st.markdown(
                            '<div class="filter-condition">', unsafe_allow_html=True
                        )

                        st.markdown("**Step 2: データ抽出**")
                        st.markdown(
                            f"• 滞納データ列数: {stats.get('arrear_columns', 0)}列"
                        )
                        st.markdown(
                            f"• 契約No重複削除: {stats.get('arrear_unique_before', 0):,} → {stats.get('arrear_unique_after', 0):,} 件"
                        )
                        st.markdown(
                            f"• 削除件数: {stats.get('arrear_duplicates_removed', 0):,} 件"
                        )

                        st.markdown("**Step 2.5: フィルタリング**")
                        st.markdown(
                            f"• ContractList: {stats.get('contract_extracted', 0):,} 件"
                        )
                        st.markdown(
                            f"• CD=1,4抽出: {stats.get('client_cd_before', 0):,} → {stats.get('client_cd_after', 0):,} 件"
                        )
                        st.markdown(
                            f"• 除外件数: {stats.get('client_cd_excluded', 0):,} 件"
                        )

                        st.markdown("**Step 3-4: マッチング**")
                        st.markdown(
                            f"• マッチ成功: {stats.get('match_success', 0):,} 件"
                        )
                        st.markdown(
                            f"• マッチ失敗: {stats.get('match_failed', 0):,} 件"
                        )
                        st.markdown(
                            f"• 残債増加: {stats.get('diff_increased', 0):,} 件"
                        )
                        st.markdown(
                            f"• 残債減少: {stats.get('diff_decreased', 0):,} 件"
                        )

                        st.markdown("</div>", unsafe_allow_html=True)

                else:
                    st.warning("⚠️ 更新が必要なデータが存在しませんでした。")
                    st.info("""
                    以下の条件を確認してください：
                    - クライアントCDが1、4、または9306のデータが存在するか
                    - 新旧の残債額に差分があるデータが存在するか
                    - ファイルのヘッダー形式が正しいか
                    """)

            except Exception as e:
                display_error_result(f"エラーが発生しました: {str(e)}")
    elif file1 or file2:
        st.warning("2つのCSVファイルをアップロードしてください。")

    st.markdown("**フィルタ条件:**")
    st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
    st.markdown("• データ抽出 → csv_arrear_*.csvから契約Noと滞納額合計")
    st.markdown(
        "• データ統合 → ContractListから管理番号・引継番号・滞納残債・クライアントCD"
    )
    st.markdown("• クライアントCD → 1,4,9306のみ抽出")
    st.markdown("• 残債差分 → 新旧の差分がある場合のみ更新対象")
    st.markdown("• マッチング → 管理番号での照合処理")
    st.markdown("</div>", unsafe_allow_html=True)
