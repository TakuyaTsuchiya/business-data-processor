"""
結果表示サービス - Services Layer

このモジュールは、処理結果の表示、ログ表示、統計情報表示、
エラーハンドリング等のUI表示処理を統一します。

統合対象：
- 成功メッセージ表示（全関数で重複）
- 警告メッセージ表示（全関数で重複）
- 処理ログ表示（ほぼ全関数で重複）
- データプレビュー表示（大部分の関数で重複）
- エラーハンドリング（全関数で重複）
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from .processor_service import ProcessorResult


class ResultDisplayService:
    """
    処理結果表示を統合管理するサービスクラス
    
    プロセッサーの実行結果を一貫した形式で表示します。
    """
    
    @staticmethod
    def show_processing_success(result: ProcessorResult):
        """
        処理成功メッセージを表示
        
        Args:
            result (ProcessorResult): プロセッサー実行結果
            
        Examples:
            >>> ResultDisplayService.show_processing_success(result)
            # "処理完了: 150件のデータを出力"
        """
        if result.has_data:
            st.success(f"処理完了: {result.row_count:,}件のデータを出力")
        else:
            st.warning("条件に合致するデータがありませんでした。")
    
    @staticmethod
    def show_processing_error(result: ProcessorResult):
        """
        処理エラーメッセージを表示
        
        Args:
            result (ProcessorResult): プロセッサー実行結果
        """
        if result.error_message:
            st.error(f"エラーが発生しました: {result.error_message}")
        else:
            st.error("不明なエラーが発生しました。")
    
    @staticmethod
    def show_processing_logs(
        logs: List[str], 
        title: str = "📊 処理ログ",
        expanded: bool = False,
        max_logs: Optional[int] = None
    ):
        """
        処理ログを表示
        
        Args:
            logs (List[str]): 処理ログのリスト
            title (str): ログセクションのタイトル
            expanded (bool): デフォルトで展開するか
            max_logs (int, optional): 表示する最大ログ数
            
        Examples:
            >>> ResultDisplayService.show_processing_logs(result.logs)
            >>> # 最新5件のみ表示
            >>> ResultDisplayService.show_processing_logs(result.logs, max_logs=5)
        """
        if not logs:
            return
        
        display_logs = logs[-max_logs:] if max_logs else logs
        
        with st.expander(title, expanded=expanded):
            for log in display_logs:
                st.write(f"• {log}")
    
    @staticmethod
    def show_statistics(
        stats: Dict[str, Any],
        title: str = "📊 処理統計情報"
    ):
        """
        統計情報を表示
        
        Args:
            stats (Dict[str, Any]): 統計情報の辞書
            title (str): 統計セクションのタイトル
            
        Examples:
            >>> stats = {
            ...     "arrear_unique_before": 15000,
            ...     "arrear_unique_after": 14500,
            ...     "match_success": 12000,
            ...     "match_failed": 2500
            ... }
            >>> ResultDisplayService.show_statistics(stats)
        """
        if not stats:
            return
        
        with st.expander(title, expanded=True):
            st.markdown("**処理統計情報:**")
            st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
            
            # 統計情報をカテゴリ別に整理して表示
            ResultDisplayService._format_statistics(stats)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    @staticmethod
    def _format_statistics(stats: Dict[str, Any]):
        """統計情報をフォーマットして表示"""
        
        # データ抽出統計
        extraction_keys = [k for k in stats.keys() if any(word in k for word in ['before', 'after', 'extracted', 'columns'])]
        if extraction_keys:
            st.markdown("**Step 2: データ抽出**")
            for key in extraction_keys:
                if 'columns' in key:
                    st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]}列")
                elif 'before' in key or 'after' in key:
                    st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]:,} 件")
        
        # フィルタリング統計
        filter_keys = [k for k in stats.keys() if any(word in k for word in ['excluded', 'removed', 'filtered'])]
        if filter_keys:
            st.markdown("**Step 2.5: フィルタリング**")
            for key in filter_keys:
                st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]:,} 件")
        
        # マッチング統計
        match_keys = [k for k in stats.keys() if any(word in k for word in ['match', 'diff', 'increased', 'decreased'])]
        if match_keys:
            st.markdown("**Step 3-4: マッチング**")
            for key in match_keys:
                st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]:,} 件")
        
        # その他の統計
        other_keys = [k for k in stats.keys() if k not in extraction_keys + filter_keys + match_keys]
        if other_keys:
            st.markdown("**その他の統計**")
            for key in other_keys:
                if isinstance(stats[key], (int, float)):
                    st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]:,}")
                else:
                    st.markdown(f"• {key.replace('_', ' ').title()}: {stats[key]}")
    
    @staticmethod
    def show_data_preview(
        df: pd.DataFrame, 
        title: str = "処理結果プレビュー",
        preview_rows: int = 10
    ):
        """
        データプレビューを表示
        
        Args:
            df (pd.DataFrame): 表示するDataFrame
            title (str): プレビューセクションのタイトル
            preview_rows (int): 表示する行数
            
        Examples:
            >>> ResultDisplayService.show_data_preview(result.result_df)
        """
        if df.empty:
            return
        
        st.subheader(title)
        
        # app.pyの safe_dataframe_display を使用
        from app import safe_dataframe_display
        safe_dataframe_display(df.head(preview_rows))
    
    @staticmethod
    def show_download_section(
        result: ProcessorResult,
        download_label: str = "📥 CSVファイルをダウンロード"
    ):
        """
        ダウンロードセクションを表示
        
        Args:
            result (ProcessorResult): プロセッサー実行結果
            download_label (str): ダウンロードボタンのラベル
            
        Examples:
            >>> ResultDisplayService.show_download_section(result)
        """
        if not result.has_data or not result.filename:
            return
        
        # app.pyの safe_csv_download を使用
        from app import safe_csv_download
        safe_csv_download(result.result_df, result.filename, download_label)
    
    @staticmethod
    def show_complete_result(
        result: ProcessorResult,
        show_logs: bool = True,
        show_stats: bool = True,
        show_preview: bool = True,
        show_download: bool = True,
        logs_expanded: bool = False,
        preview_rows: int = 10
    ):
        """
        処理結果を包括的に表示
        
        Args:
            result (ProcessorResult): プロセッサー実行結果
            show_logs (bool): ログを表示するか
            show_stats (bool): 統計情報を表示するか  
            show_preview (bool): データプレビューを表示するか
            show_download (bool): ダウンロードボタンを表示するか
            logs_expanded (bool): ログをデフォルトで展開するか
            preview_rows (int): プレビュー表示行数
            
        Examples:
            >>> # 基本的な結果表示
            >>> ResultDisplayService.show_complete_result(result)
            >>> 
            >>> # ログを展開して表示
            >>> ResultDisplayService.show_complete_result(result, logs_expanded=True)
            >>> 
            >>> # プレビューなしで表示
            >>> ResultDisplayService.show_complete_result(result, show_preview=False)
        """
        if not result.success:
            ResultDisplayService.show_processing_error(result)
            return
        
        # 成功メッセージ
        ResultDisplayService.show_processing_success(result)
        
        # データがない場合はここで終了
        if not result.has_data:
            if show_logs and result.logs:
                ResultDisplayService.show_processing_logs(result.logs, expanded=True)
            return
        
        # ダウンロードボタン（優先表示）
        if show_download:
            ResultDisplayService.show_download_section(result)
        
        # 処理ログ表示
        if show_logs and result.logs:
            ResultDisplayService.show_processing_logs(result.logs, expanded=logs_expanded)
        
        # 統計情報表示
        if show_stats and result.stats:
            ResultDisplayService.show_statistics(result.stats)
        
        # データプレビュー表示
        if show_preview:
            ResultDisplayService.show_data_preview(result.result_df, preview_rows=preview_rows)


class FilterConditionDisplay:
    """
    フィルタ条件表示の統一クラス
    """
    
    @staticmethod
    def show_filter_conditions(conditions: List[str], title: str = "**フィルタ条件:**"):
        """
        フィルタ条件を統一フォーマットで表示
        
        Args:
            conditions (List[str]): フィルタ条件のリスト
            title (str): フィルタ条件セクションのタイトル
            
        Examples:
            >>> conditions = [
            ...     "委託先法人ID → 空白&5",
            ...     "入金予定日 → 前日以前とNaN", 
            ...     "回収ランク → 「弁護士介入」除外"
            ... ]
            >>> FilterConditionDisplay.show_filter_conditions(conditions)
        """
        st.markdown(title)
        st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
        
        for condition in conditions:
            st.markdown(f"• {condition}")
        
        st.markdown('</div>', unsafe_allow_html=True)