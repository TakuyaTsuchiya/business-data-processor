"""
画面テンプレート共通化モジュール
Business Data Processor

全画面で使用される共通のUI構造を提供
"""

import streamlit as st
from typing import Dict, List, Callable, Optional, Any, Union, Tuple
from datetime import date
from components.common_ui import display_filter_conditions, safe_csv_download, display_processing_logs
from components.result_display import display_processing_result, display_error_result


class ScreenConfig:
    """画面設定のデータクラス"""
    def __init__(
        self,
        title: str,
        subtitle: str,
        filter_conditions: List[str],
        process_function: Callable,
        file_count: int = 1,
        info_message: Optional[str] = None,
        payment_deadline_input: Optional[Callable] = None,
        file_labels: Optional[List[str]] = None,
        success_message_template: str = "処理完了: {processed_rows}件のデータを出力",
        no_data_message: str = "条件に合致するデータがありませんでした。",
        title_icon: str = "",
        processing_time_message: Optional[str] = None,
        file_types: Optional[List[str]] = None
    ):
        self.title = title
        self.subtitle = subtitle
        self.filter_conditions = filter_conditions
        self.process_function = process_function
        self.file_count = file_count
        self.info_message = info_message
        self.payment_deadline_input = payment_deadline_input
        self.file_labels = file_labels or [f"ファイル{i+1}" for i in range(file_count)]
        self.success_message_template = success_message_template
        self.no_data_message = no_data_message
        self.title_icon = title_icon
        self.processing_time_message = processing_time_message
        self.file_types = file_types or ["csv"]


def render_screen(config: ScreenConfig, key_prefix: str):
    """
    統一された画面レンダリング関数
    
    Args:
        config: ScreenConfig - 画面設定
        key_prefix: str - Streamlitウィジェットのキープレフィックス
    """
    # 1. タイトル表示（パターン2形式）
    if config.title_icon:
        st.title(f"{config.title_icon} {config.title}")
    else:
        st.title(config.title)
    
    if config.subtitle:
        st.subheader(config.subtitle)
    
    # 2. フィルタ条件表示
    display_filter_conditions(config.filter_conditions)
    
    # 3. 情報メッセージ（オプション）
    if config.info_message:
        st.info(config.info_message)
    
    # 3.5. 処理時間の警告（オプション）
    if config.processing_time_message:
        st.warning(config.processing_time_message)
    
    # 4. 支払期限入力（SMS画面用、オプション）
    payment_deadline_values = {}
    if config.payment_deadline_input:
        payment_deadline_values = config.payment_deadline_input(key_prefix)
    
    # 5. ファイルアップローダー
    uploaded_files = []

    # ファイルタイプ名の表示用文字列
    file_type_display = "/".join([ft.upper() for ft in config.file_types])

    if config.file_count == 1:
        # 単一ファイル
        uploaded_file = st.file_uploader(
            f"{file_type_display}ファイルをアップロードしてください",
            type=config.file_types,
            key=f"{key_prefix}_file"
        )
        if uploaded_file:
            uploaded_files = [uploaded_file]
    else:
        # 複数ファイル（2列レイアウト）
        cols = st.columns(config.file_count)
        for i, (col, label) in enumerate(zip(cols, config.file_labels)):
            with col:
                st.markdown(f"**📄 {label}**")
                file = st.file_uploader(
                    f"{label.split(': ')[1] if ': ' in label else label}をアップロード",
                    type=config.file_types,
                    key=f"{key_prefix}_file{i+1}"
                )
                if file:
                    uploaded_files.append(file)
    
    # 6. ファイルアップロード後の処理
    if len(uploaded_files) == config.file_count:
        try:
            # ファイル読み込み成功メッセージ
            for file in uploaded_files:
                st.success(f"✅ {file.name}: 読み込み完了")
            
            # 7. 処理実行ボタン
            if st.button("処理を実行", type="primary", key=f"{key_prefix}_process"):
                with st.spinner("処理中..."):
                    # ファイルデータの準備
                    if config.file_count == 1:
                        file_data = uploaded_files[0].read()
                    else:
                        file_data = [f.read() for f in uploaded_files]
                    
                    # 処理実行
                    if payment_deadline_values:
                        result = config.process_function(file_data, **payment_deadline_values)
                    else:
                        result = config.process_function(file_data)
                    
                    # 8. 結果表示
                    _display_result(result, config, key_prefix)
                    
        except Exception as e:
            display_error_result(f"エラーが発生しました: {str(e)}")

    elif 0 < len(uploaded_files) < config.file_count:
        st.warning(f"{config.file_count}つのファイルをアップロードしてください。")


def _display_result(result: Any, config: ScreenConfig, key_prefix: str):
    """結果表示の共通処理"""
    # 結果の形式に応じて処理を分岐
    if isinstance(result, tuple):
        if len(result) == 2:  # (df, filename) パターン
            result_df, filename = result
            if result_df is not None and not result_df.empty:
                display_processing_result(result_df, [], filename)
            else:
                st.warning(config.no_data_message)
                
        elif len(result) == 3:  # (df, logs, filename) パターン
            result_df, logs, filename = result
            if not result_df.empty:
                display_processing_result(result_df, logs, filename)
            else:
                st.warning(config.no_data_message)
                if logs:
                    display_processing_logs(logs, expanded=True)
                            
        elif len(result) == 4:  # (df, logs, filename, stats) パターン
            result_df, logs, filename, stats = result
            if not result_df.empty:
                # 成功メッセージ
                if 'processed_rows' in stats and 'initial_rows' in stats:
                    st.success(
                        config.success_message_template.format(**stats) + 
                        f"（元データ: {stats['initial_rows']}件）"
                    )
                else:
                    st.success("処理が完了しました。")
                
                # ダウンロードボタン
                safe_csv_download(result_df, filename)
                
                # ログ表示（成功時は折りたたみ）
                if logs:
                    with st.expander("📊 処理ログ", expanded=False):
                        for log in logs:
                            st.write(f"• {log}")
            else:
                st.warning(config.no_data_message)
                if logs:
                    # エラー時はログを展開
                    display_processing_logs(logs, expanded=True)


def create_payment_deadline_input(key_prefix: str) -> dict:
    """SMS画面用の支払期限日付入力の生成"""
    st.subheader("支払期限の設定")
    payment_deadline_date = st.date_input(
        "クリックして支払期限を選択してください",
        value=date.today(),
        help="この日付がBG列「支払期限」に設定されます（例：2025年6月30日）",
        key=f"{key_prefix}_payment_deadline",
        disabled=False,
        format="YYYY/MM/DD"
    )
    st.write(f"設定される支払期限: **{payment_deadline_date.strftime('%Y年%m月%d日')}**")
    return {'payment_deadline_date': payment_deadline_date}