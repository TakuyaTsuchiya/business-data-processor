"""
処理結果表示モジュール
Business Data Processor

処理結果の表示、ログ出力、ダウンロード機能を統一的に提供
"""

import streamlit as st
import pandas as pd
from typing import List, Optional, Dict
from components.common_ui import (
    safe_dataframe_display,
    safe_csv_download,
    display_processing_logs
)
from domain.exceptions import BusinessDataProcessorError, create_user_friendly_message


def display_processing_result(
    result_df: pd.DataFrame,
    logs: List[str],
    filename: str,
    stats: Optional[Dict[str, int]] = None,
    preview_rows: int = 10,
    show_preview: bool = True,
    log_expander: bool = False,
    empty_message: str = "条件に合致するデータがありませんでした。",
    success_message_template: Optional[str] = None,
    download_label: Optional[str] = None
) -> None:
    """
    処理結果の表示を統一的に行う
    
    Args:
        result_df: 処理結果のDataFrame
        logs: 処理ログのリスト
        filename: ダウンロード用ファイル名
        stats: 統計情報（processed_rows, initial_rows等）
        preview_rows: プレビュー表示行数
        show_preview: プレビュー表示の有無
        log_expander: ログをexpanderで表示するか
        empty_message: データが空の場合のメッセージ
        success_message_template: 成功メッセージのテンプレート
        download_label: ダウンロードボタンのカスタムラベル
    """
    
    if not result_df.empty:
        # 成功メッセージ表示
        if success_message_template and stats:
            # statsを使ったカスタムメッセージ
            message = success_message_template.format(stats=stats, count=len(result_df))
            st.success(message)
        elif stats and 'processed_rows' in stats and 'initial_rows' in stats:
            # SMS系の標準フォーマット
            st.success(f"処理完了: {stats['processed_rows']}件のデータを出力（元データ: {stats['initial_rows']}件）")
        else:
            # 通常の標準フォーマット
            st.success(f"処理完了: {len(result_df)}件のデータを出力")
        
        # ダウンロードボタン（SMS系は先に表示することが多い）
        if not show_preview or (stats and 'processed_rows' in stats):
            if download_label:
                safe_csv_download(result_df, filename, download_label)
            else:
                safe_csv_download(result_df, filename)
        
        # 処理ログ表示
        if logs:
            if log_expander:
                with st.expander("📊 処理ログ", expanded=False):
                    for log in logs:
                        st.write(f"• {log}")
            else:
                display_processing_logs(logs)
        
        # データプレビュー表示
        if show_preview:
            st.subheader("処理結果プレビュー")
            safe_dataframe_display(result_df.head(preview_rows))
            
            # 通常パターンではプレビュー後にダウンロード
            if not (stats and 'processed_rows' in stats):
                if download_label:
                    safe_csv_download(result_df, filename, download_label)
                else:
                    safe_csv_download(result_df, filename)
        
        # 統計情報表示（SMS系など）
        if stats and show_preview and 'initial_rows' in stats and 'processed_rows' in stats:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("元データ件数", f"{stats.get('initial_rows', 0):,}件")
            with col2:
                st.metric("処理後件数", f"{stats.get('processed_rows', 0):,}件")
    
    else:
        # データが空の場合
        st.warning(empty_message)
        
        # エラー時もログを表示
        if logs:
            if log_expander:
                display_processing_logs(logs, expanded=True)
            else:
                display_processing_logs(logs)


def display_error_result(
    error_message: str,
    logs: Optional[List[str]] = None
) -> None:
    """
    エラー時の表示を統一的に行う
    
    Args:
        error_message: エラーメッセージ
        logs: 処理ログのリスト（オプション）
    """
    st.error(error_message)
    
    if logs:
        display_processing_logs(logs, expanded=True)


def display_exception_error(
    error: Exception,
    logs: Optional[List[str]] = None,
    show_technical: bool = False
) -> None:
    """
    例外オブジェクトからエラー表示を行う（カスタム例外対応版）
    
    Args:
        error: 例外オブジェクト（BusinessDataProcessorErrorまたは通常の例外）
        logs: 処理ログのリスト（オプション）
        show_technical: 技術的な詳細を表示するか
    """
    if isinstance(error, BusinessDataProcessorError):
        # カスタム例外の場合、ユーザーフレンドリーなメッセージを表示
        user_message = create_user_friendly_message(error)
        st.error(user_message)
        
        # 詳細情報がある場合はexpanderで表示
        if error.details or show_technical:
            with st.expander("エラーの詳細情報"):
                st.write(f"**エラーコード**: {error.error_code}")
                if error.details:
                    st.json(error.details)
                if show_technical:
                    st.code(str(error))
    else:
        # 通常の例外の場合
        st.error(f"エラーが発生しました: {str(error)}")
    
    # ログがある場合は表示
    if logs:
        display_processing_logs(logs, expanded=True)