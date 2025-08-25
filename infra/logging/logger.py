"""
ログ処理の統合 - Infrastructure Layer

このモジュールは、各プロセッサーで個別に実装されていたログ処理を統合し、
一貫したログフォーマットとレベル管理を提供します。

v2.5.0からViveLogger対応：
- AI向けの構造化ログ出力
- 従来のログとの併用
- 後方互換性を完全維持

統合対象：
- 処理ログの収集・フォーマット
- 統計情報のログ出力
- エラーログの標準化
- ViveLoggerとの統合
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import os
import traceback

# ViveLogger import（オプショナル）
try:
    from vibelogger import create_file_logger
    VIBELOGGER_AVAILABLE = True
except ImportError:
    VIBELOGGER_AVAILABLE = False


class ProcessLogger:
    """処理ログを管理するクラス（ViveLogger統合版）"""
    
    def __init__(self, processor_name: str):
        """
        ProcessLoggerを初期化
        
        Args:
            processor_name (str): プロセッサー名（例: "ミライル契約者without10k"）
        """
        self.processor_name = processor_name
        self.logs: List[str] = []
        self.stats: Dict[str, Any] = {}
        self.start_time = datetime.now()
        
        # ViveLoggerの初期化（利用可能な場合）
        self.vibe_logger = None
        if VIBELOGGER_AVAILABLE:
            try:
                # ログディレクトリの作成
                log_dir = os.path.join("logs", "vibe")
                os.makedirs(log_dir, exist_ok=True)
                
                # ViveLoggerインスタンス作成
                self.vibe_logger = create_file_logger(f"business-data-processor/{processor_name}")
                print(f"[{processor_name}] ViveLogger初期化完了")
            except Exception as e:
                print(f"[{processor_name}] ViveLogger初期化失敗: {e}")
                self.vibe_logger = None
    
    def info(self, message: str, operation: Optional[str] = None, context: Optional[Dict[str, Any]] = None, 
             human_note: Optional[str] = None, **kwargs):
        """
        情報ログを記録（ViveLogger対応版）
        
        Args:
            message (str): ログメッセージ
            operation (str, optional): 操作名（ViveLogger用）
            context (Dict[str, Any], optional): コンテキスト情報（ViveLogger用）
            human_note (str, optional): AI向けメモ（ViveLogger用）
            **kwargs: 追加情報（従来との互換性用）
        """
        # 従来のログ処理（後方互換性）
        log_entry = self._format_log("INFO", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] {log_entry}")
        
        # ViveLoggerへの出力（利用可能な場合）
        if self.vibe_logger:
            try:
                vibe_context = context or {}
                # kwargsもcontextに統合
                if kwargs:
                    vibe_context.update(kwargs)
                
                self.vibe_logger.info(
                    operation=operation or "general_info",
                    message=message,
                    context=vibe_context,
                    human_note=human_note or "AI-INFO: 一般的な情報ログ"
                )
            except Exception as e:
                print(f"[{self.processor_name}] ViveLogger出力エラー: {e}")
    
    def warning(self, message: str, operation: Optional[str] = None, context: Optional[Dict[str, Any]] = None,
                human_note: Optional[str] = None, **kwargs):
        """
        警告ログを記録（ViveLogger対応版）
        
        Args:
            message (str): ログメッセージ
            operation (str, optional): 操作名
            context (Dict[str, Any], optional): コンテキスト情報
            human_note (str, optional): AI向けメモ
            **kwargs: 追加情報
        """
        # 従来のログ処理
        log_entry = self._format_log("WARNING", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] ⚠️ {log_entry}")
        
        # ViveLoggerへの出力
        if self.vibe_logger:
            try:
                vibe_context = context or {}
                if kwargs:
                    vibe_context.update(kwargs)
                
                self.vibe_logger.warning(
                    operation=operation or "general_warning",
                    message=message,
                    context=vibe_context,
                    human_note=human_note or "AI-WARNING: 注意が必要な状況"
                )
            except Exception as e:
                print(f"[{self.processor_name}] ViveLogger警告出力エラー: {e}")
    
    def error(self, message: str, operation: Optional[str] = None, context: Optional[Dict[str, Any]] = None,
              human_note: Optional[str] = None, exception: Optional[Exception] = None, **kwargs):
        """
        エラーログを記録（ViveLogger対応版）
        
        Args:
            message (str): ログメッセージ
            operation (str, optional): 操作名
            context (Dict[str, Any], optional): コンテキスト情報
            human_note (str, optional): AI向けメモ
            exception (Exception, optional): 例外オブジェクト
            **kwargs: 追加情報
        """
        # 従来のログ処理
        log_entry = self._format_log("ERROR", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] ❌ {log_entry}")
        
        # ViveLoggerへの出力
        if self.vibe_logger:
            try:
                vibe_context = context or {}
                if kwargs:
                    vibe_context.update(kwargs)
                
                # 例外情報を追加
                if exception:
                    vibe_context["exception_type"] = type(exception).__name__
                    vibe_context["exception_message"] = str(exception)
                    vibe_context["traceback"] = traceback.format_exc()
                
                self.vibe_logger.error(
                    operation=operation or "general_error",
                    message=message,
                    context=vibe_context,
                    human_note=human_note or "AI-ERROR: エラー分析と対処法の提案が必要"
                )
            except Exception as e:
                print(f"[{self.processor_name}] ViveLoggerエラー出力エラー: {e}")
    
    def debug(self, message: str, **kwargs):
        """
        デバッグログを記録
        
        Args:
            message (str): ログメッセージ
            **kwargs: 追加情報
        """
        log_entry = self._format_log("DEBUG", message, **kwargs)
        self.logs.append(log_entry)
        # デバッグログはコンソールにのみ出力
        print(f"[{self.processor_name}] 🔍 {log_entry}")
    
    def log_data_processing(self, step: str, before_count: int, after_count: int, details: str = "", 
                           conditions: Optional[Dict[str, Any]] = None, human_note: Optional[str] = None):
        """
        データ処理ステップのログを記録（ViveLogger対応版）
        
        Args:
            step (str): 処理ステップ名
            before_count (int): 処理前データ件数
            after_count (int): 処理後データ件数
            details (str): 詳細情報
            conditions (Dict[str, Any], optional): 処理条件
            human_note (str, optional): AI向けメモ
        """
        excluded_count = before_count - after_count
        
        if excluded_count > 0:
            message = f"{step}: {before_count:,}件 → {after_count:,}件 (除外: {excluded_count:,}件)"
        else:
            message = f"{step}: {after_count:,}件"
        
        if details:
            message += f" - {details}"
        
        # ViveLogger用のコンテキスト情報
        context = {
            "step_name": step,
            "before_count": before_count,
            "after_count": after_count,
            "excluded_count": excluded_count,
            "success_rate": (after_count / before_count * 100) if before_count > 0 else 0
        }
        
        if conditions:
            context["conditions"] = conditions
        
        if details:
            context["details"] = details
        
        # AI向けのデフォルトメモ
        if not human_note:
            if excluded_count > 0:
                exclusion_rate = excluded_count / before_count * 100 if before_count > 0 else 0
                if exclusion_rate > 50:
                    human_note = f"AI-WARNING: 除外率{exclusion_rate:.1f}%が高い。データ品質または条件設定を確認"
                elif exclusion_rate > 20:
                    human_note = f"AI-INFO: 除外率{exclusion_rate:.1f}%は正常範囲だが監視が必要"
                else:
                    human_note = f"AI-INFO: 除外率{exclusion_rate:.1f}%は正常範囲"
            else:
                human_note = "AI-INFO: 全データが条件を満たしている"
        
        # 従来のログ記録とViveLogger統合
        self.info(message, operation=f"data_processing_{step.lower().replace(' ', '_')}", 
                  context=context, human_note=human_note)
        
        # 統計情報に記録
        step_key = step.lower().replace(' ', '_').replace(':', '')
        self.stats[f"{step_key}_before"] = before_count
        self.stats[f"{step_key}_after"] = after_count
        self.stats[f"{step_key}_excluded"] = excluded_count
    
    def log_filter_result(self, filter_name: str, before_count: int, after_count: int, 
                         condition: str = "", filter_conditions: Optional[Dict[str, Any]] = None,
                         human_note: Optional[str] = None):
        """
        フィルタリング結果のログを記録（ViveLogger対応版）
        
        Args:
            filter_name (str): フィルター名
            before_count (int): フィルタリング前データ件数
            after_count (int): フィルタリング後データ件数
            condition (str): フィルタリング条件（テキスト）
            filter_conditions (Dict[str, Any], optional): 詳細なフィルタ条件
            human_note (str, optional): AI向けメモ
        """
        excluded_count = before_count - after_count
        message = f"フィルター[{filter_name}]: {before_count:,}件 → {after_count:,}件"
        
        if excluded_count > 0:
            message += f" (除外: {excluded_count:,}件)"
        
        if condition:
            message += f" - 条件: {condition}"
        
        # ViveLogger用のコンテキスト
        context = {
            "filter_name": filter_name,
            "before_count": before_count,
            "after_count": after_count,
            "excluded_count": excluded_count,
            "filter_efficiency": (excluded_count / before_count * 100) if before_count > 0 else 0
        }
        
        if condition:
            context["condition_description"] = condition
            
        if filter_conditions:
            context["filter_conditions"] = filter_conditions
        
        # AI向けメモの自動生成
        if not human_note:
            filter_rate = excluded_count / before_count * 100 if before_count > 0 else 0
            if filter_rate > 80:
                human_note = f"AI-WARNING: フィルター除外率{filter_rate:.1f}%が非常に高い。条件が厳しすぎる可能性"
            elif filter_rate > 50:
                human_note = f"AI-INFO: フィルター除外率{filter_rate:.1f}%は適切な絞り込み"
            elif filter_rate < 5:
                human_note = f"AI-INFO: フィルター除外率{filter_rate:.1f}%は緩い条件。必要に応じて見直し"
            else:
                human_note = f"AI-INFO: フィルター除外率{filter_rate:.1f}%は適切"
        
        self.info(message, operation=f"filter_{filter_name.lower().replace(' ', '_')}", 
                  context=context, human_note=human_note)
    
    def log_mapping_result(self, mapping_name: str, processed_count: int, details: str = ""):
        """
        マッピング結果のログを記録
        
        Args:
            mapping_name (str): マッピング名
            processed_count (int): 処理件数
            details (str): 詳細情報
        """
        message = f"マッピング[{mapping_name}]: {processed_count:,}件処理完了"
        
        if details:
            message += f" - {details}"
        
        self.info(message)
    
    def add_stat(self, key: str, value: Any):
        """
        統計情報を追加
        
        Args:
            key (str): 統計キー
            value (Any): 統計値
        """
        self.stats[key] = value
    
    def get_logs(self) -> List[str]:
        """
        収集したログのリストを取得
        
        Returns:
            List[str]: ログメッセージのリスト
        """
        return self.logs.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        収集した統計情報を取得
        
        Returns:
            Dict[str, Any]: 統計情報の辞書
        """
        # 処理時間を追加
        end_time = datetime.now()
        processing_time = (end_time - self.start_time).total_seconds()
        
        stats_copy = self.stats.copy()
        stats_copy['processing_time_seconds'] = round(processing_time, 2)
        stats_copy['processor_name'] = self.processor_name
        
        return stats_copy
    
    def _format_log(self, level: str, message: str, **kwargs) -> str:
        """
        ログメッセージをフォーマット
        
        Args:
            level (str): ログレベル
            message (str): メッセージ
            **kwargs: 追加情報
        
        Returns:
            str: フォーマットされたログメッセージ
        """
        if kwargs:
            additional_info = " | " + " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            return f"{message}{additional_info}"
        return message


def create_logger(processor_name: str) -> ProcessLogger:
    """
    プロセッサー用のロガーを作成
    
    Args:
        processor_name (str): プロセッサー名
        
    Returns:
        ProcessLogger: 初期化されたロガーインスタンス
        
    Examples:
        >>> logger = create_logger("ミライル契約者without10k")
        >>> logger.info("処理開始")
        >>> logger.log_data_processing("フィルタリング", 1000, 800)
        >>> logs = logger.get_logs()
    """
    return ProcessLogger(processor_name)


def format_processing_summary(
    processor_name: str, 
    input_count: int, 
    output_count: int, 
    processing_time: float,
    additional_stats: Optional[Dict[str, Any]] = None
) -> str:
    """
    処理結果サマリーをフォーマット
    
    Args:
        processor_name (str): プロセッサー名
        input_count (int): 入力データ件数
        output_count (int): 出力データ件数
        processing_time (float): 処理時間（秒）
        additional_stats (Dict[str, Any], optional): 追加統計情報
        
    Returns:
        str: フォーマットされたサマリー文字列
    """
    summary_lines = [
        f"【{processor_name} 処理完了】",
        f"📊 入力: {input_count:,}件",
        f"📊 出力: {output_count:,}件",
        f"⏱️ 処理時間: {processing_time:.2f}秒"
    ]
    
    if input_count > 0:
        success_rate = (output_count / input_count) * 100
        summary_lines.append(f"📈 成功率: {success_rate:.1f}%")
    
    if additional_stats:
        for key, value in additional_stats.items():
            if isinstance(value, (int, float)):
                summary_lines.append(f"📋 {key}: {value:,}")
            else:
                summary_lines.append(f"📋 {key}: {value}")
    
    return "\n".join(summary_lines)