"""
結果表示サービス - Services Layer

型安全性確立版：ProcessingResult/ProcessingStatisticsを使用した
統一的な結果表示サービスを提供します。
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from domain.models.processing_models import ProcessingResult, ProcessingStatistics
from domain.models.enums import ProcessingStatus, MessageType
from domain.validators.type_validator import TypeValidator, ValidationError
from services.adapters.legacy_adapter import LegacyProcessorAdapter
from infra.logging.logger import create_logger
import time


logger = create_logger(__name__)


class ResultDisplayService:
    """
    処理結果表示を統合管理するサービスクラス（型安全版）
    
    ProcessingResult形式の結果を一貫した形式で表示します。
    """
    
    @staticmethod
    def show_complete_result(
        result: Union[ProcessingResult, Any],
        processor_name: str = "プロセッサー",
        start_time: Optional[float] = None
    ):
        """
        処理結果を完全表示（型安全対応）
        
        Args:
            result: 処理結果（ProcessingResultまたはレガシー形式）
            processor_name: プロセッサー名
            start_time: 処理開始時刻
        """
        try:
            # 型検証・変換
            if not isinstance(result, ProcessingResult):
                logger.info(f"レガシー形式の結果を変換: {type(result)}")
                result = LegacyProcessorAdapter.adapt_result(
                    result, processor_name, start_time
                )
            
            # ステータスに応じた表示
            if result.is_success():
                ResultDisplayService._show_success_result(result)
            elif result.is_error():
                ResultDisplayService._show_error_result(result)
            else:
                ResultDisplayService._show_warning_result(result)
            
            # 統計情報表示
            ResultDisplayService.show_statistics(result.statistics)
            
            # データプレビュー表示
            if not result.data.empty:
                ResultDisplayService.show_data_preview(result.data)
            
            # メッセージ表示
            ResultDisplayService._show_messages(result)
            
        except Exception as e:
            logger.error(f"結果表示中にエラー: {str(e)}")
            st.error(f"結果表示中にエラーが発生しました: {str(e)}")
    
    @staticmethod
    def _show_success_result(result: ProcessingResult):
        """成功結果の表示"""
        st.success(result.statistics.to_summary())
        
        # 成功率が低い場合は警告
        success_rate = result.statistics.success_rate()
        if success_rate < 90:
            st.warning(f"成功率: {success_rate:.1f}%")
    
    @staticmethod
    def _show_error_result(result: ProcessingResult):
        """エラー結果の表示"""
        st.error("処理中にエラーが発生しました")
        
        for error in result.errors:
            st.error(f"❌ {error}")
    
    @staticmethod
    def _show_warning_result(result: ProcessingResult):
        """警告結果の表示"""
        st.warning("処理は完了しましたが、警告があります")
        
        for warning in result.warnings:
            st.warning(f"⚠️ {warning}")
    
    @staticmethod
    def show_processing_success(result: ProcessingResult):
        """処理成功メッセージを表示（型安全版）"""
        if result.statistics.processed_records > 0:
            st.success(result.statistics.to_summary())
        else:
            st.warning("条件に合致するデータがありませんでした。")
    
    @staticmethod
    def show_processing_error(error_message: str):
        """処理エラーメッセージを表示"""
        st.error(f"エラーが発生しました: {error_message}")
    
    @staticmethod
    def show_processing_logs(
        logs: List[str], 
        title: str = "📊 処理ログ",
        expanded: bool = False,
        max_logs: Optional[int] = None
    ):
        """処理ログを表示"""
        if not logs:
            return
        
        display_logs = logs[-max_logs:] if max_logs else logs
        
        with st.expander(title, expanded=expanded):
            for log in display_logs:
                st.write(f"• {log}")
    
    @staticmethod
    def show_statistics(
        stats: Union[ProcessingStatistics, Dict[str, Any], str],
        title: str = "📊 処理統計情報"
    ):
        """
        統計情報を表示（型安全対応）
        
        Args:
            stats: ProcessingStatistics、辞書、または文字列形式の統計情報
            title: 統計セクションのタイトル
        """
        try:
            # 型検証・変換
            if isinstance(stats, str):
                # 文字列の場合はシンプル表示
                st.info(f"📊 {stats}")
                return
            
            if not isinstance(stats, ProcessingStatistics):
                # ProcessingStatistics型への変換試行
                stats = TypeValidator.validate_processing_statistics(stats)
            
            # 統計情報の表示
            with st.expander(title, expanded=True):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("総レコード数", f"{stats.total_records:,}")
                
                with col2:
                    st.metric("処理済み", f"{stats.processed_records:,}")
                
                with col3:
                    st.metric("処理時間", f"{stats.processing_time:.2f}秒")
                
                # フィルタ条件の表示
                if stats.filter_conditions:
                    st.markdown("**適用フィルタ条件:**")
                    ResultDisplayService._show_filter_conditions(stats.filter_conditions)
                
                # エラー情報
                if stats.error_count > 0:
                    st.error(f"エラー件数: {stats.error_count:,}件")
                
                # 成功率
                success_rate = stats.success_rate()
                if success_rate < 100:
                    st.info(f"成功率: {success_rate:.1f}%")
        
        except ValidationError as e:
            logger.warning(f"統計情報の型変換失敗: {str(e)}")
            # フォールバック：生データをそのまま表示
            st.info(f"統計情報: {stats}")
        except Exception as e:
            logger.error(f"統計情報表示エラー: {str(e)}")
            st.error(f"統計情報の表示中にエラーが発生しました")
    
    @staticmethod
    def _show_filter_conditions(conditions: Dict[str, Any]):
        """フィルタ条件を整形して表示"""
        for key, value in conditions.items():
            # HTMLエスケープとレンダリング
            if isinstance(value, str) and '<' in value and '>' in value:
                # HTMLタグを含む場合
                st.markdown(f"• **{key}**: ", unsafe_allow_html=True)
                st.markdown(value, unsafe_allow_html=True)
            else:
                st.markdown(f"• **{key}**: {value}")
    
    @staticmethod
    def show_data_preview(
        df: pd.DataFrame, 
        title: str = "処理結果プレビュー",
        preview_rows: int = 10
    ):
        """データプレビューを表示"""
        if df.empty:
            st.info("表示するデータがありません。")
            return
        
        with st.expander(f"📋 {title} （{len(df):,}件）", expanded=False):
            # データ形状情報
            st.markdown(f"**データ形状**: {df.shape[0]:,}行 × {df.shape[1]}列")
            
            # プレビュー表示
            display_df = df.head(preview_rows)
            st.dataframe(display_df)
            
            if len(df) > preview_rows:
                st.info(f"※ 最初の{preview_rows}件を表示しています")
    
    @staticmethod
    def show_download_section(result: ProcessingResult, filename: str = "output.csv"):
        """ダウンロードセクションを表示"""
        if result.data.empty:
            return
        
        # CSV生成（インフラ層の使用）
        from infra.csv.writer import safe_csv_download_button
        safe_csv_download_button(result.data, filename)
    
    @staticmethod
    def _show_messages(result: ProcessingResult):
        """各種メッセージを表示"""
        # 情報メッセージ
        for message in result.messages:
            st.info(f"ℹ️ {message}")
        
        # 警告メッセージ
        for warning in result.warnings:
            st.warning(f"⚠️ {warning}")
        
        # エラーメッセージ
        for error in result.errors:
            st.error(f"❌ {error}")
    
    @staticmethod
    def show_result_with_options(
        result: ProcessingResult,
        show_stats: bool = True,
        show_preview: bool = True,
        show_download: bool = True,
        preview_rows: int = 10
    ):
        """オプション付きで結果を表示"""
        # メイン結果表示
        ResultDisplayService._show_success_result(result)
        
        # オプション表示
        if show_download and not result.data.empty:
            ResultDisplayService.show_download_section(result)
        
        if show_stats:
            ResultDisplayService.show_statistics(result.statistics)
        
        if show_preview and not result.data.empty:
            ResultDisplayService.show_data_preview(result.data, preview_rows=preview_rows)
        
        # メッセージ表示
        ResultDisplayService._show_messages(result)


class FilterConditionDisplay:
    """
    フィルタ条件表示の統一クラス（型安全版）
    """
    
    @staticmethod
    def show_filter_conditions(
        conditions: Union[List[str], Dict[str, Any]], 
        title: str = "**フィルタ条件:**"
    ):
        """
        フィルタ条件を統一フォーマットで表示
        
        Args:
            conditions: フィルタ条件（リストまたは辞書）
            title: フィルタ条件セクションのタイトル
        """
        st.markdown(title)
        st.markdown('<div class="filter-condition">', unsafe_allow_html=True)
        
        if isinstance(conditions, list):
            # リスト形式の場合
            for condition in conditions:
                FilterConditionDisplay._render_condition_item(condition)
        
        elif isinstance(conditions, dict):
            # 辞書形式の場合
            for key, value in conditions.items():
                FilterConditionDisplay._render_condition_item(f"{key} → {value}")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    @staticmethod
    def _render_condition_item(condition: str):
        """個別のフィルタ条件を表示"""
        # HTMLタグのチェックと安全なレンダリング
        if '<span' in condition and '</span>' in condition:
            # HTMLを含む場合は unsafe_allow_html を使用
            st.markdown(f"• {condition}", unsafe_allow_html=True)
        else:
            # 通常のテキスト
            st.markdown(f"• {condition}")


# 後方互換性のためのエイリアス
class ProcessorResult:
    """レガシー互換性のためのラッパークラス"""
    def __init__(self, result_df=None, logs=None, stats=None, error_message=None):
        self.result_df = result_df if result_df is not None else pd.DataFrame()
        self.logs = logs if logs is not None else []
        self.stats = stats if stats is not None else {}
        self.error_message = error_message
        self.has_data = not self.result_df.empty
        self.row_count = len(self.result_df)
        
    def to_processing_result(self) -> ProcessingResult:
        """ProcessingResult形式に変換"""
        return LegacyProcessorAdapter.adapt_result(
            (self.result_df, self.stats) if self.stats else self.result_df,
            "LegacyProcessor"
        )