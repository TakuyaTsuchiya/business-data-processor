"""
ログ処理の統合 - Infrastructure Layer

このモジュールは、各プロセッサーで個別に実装されていたログ処理を統合し、
一貫したログフォーマットとレベル管理を提供します。

統合対象：
- 処理ログの収集・フォーマット
- 統計情報のログ出力
- エラーログの標準化
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class ProcessLogger:
    """処理ログを管理するクラス"""
    
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
    
    def info(self, message: str, **kwargs):
        """
        情報ログを記録
        
        Args:
            message (str): ログメッセージ
            **kwargs: 追加情報
        """
        log_entry = self._format_log("INFO", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] {log_entry}")
    
    def warning(self, message: str, **kwargs):
        """
        警告ログを記録
        
        Args:
            message (str): ログメッセージ
            **kwargs: 追加情報
        """
        log_entry = self._format_log("WARNING", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] ⚠️ {log_entry}")
    
    def error(self, message: str, **kwargs):
        """
        エラーログを記録
        
        Args:
            message (str): ログメッセージ
            **kwargs: 追加情報
        """
        log_entry = self._format_log("ERROR", message, **kwargs)
        self.logs.append(log_entry)
        print(f"[{self.processor_name}] ❌ {log_entry}")
    
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
    
    def log_data_processing(self, step: str, before_count: int, after_count: int, details: str = ""):
        """
        データ処理ステップのログを記録
        
        Args:
            step (str): 処理ステップ名
            before_count (int): 処理前データ件数
            after_count (int): 処理後データ件数
            details (str): 詳細情報
        """
        excluded_count = before_count - after_count
        
        if excluded_count > 0:
            message = f"{step}: {before_count:,}件 → {after_count:,}件 (除外: {excluded_count:,}件)"
        else:
            message = f"{step}: {after_count:,}件"
        
        if details:
            message += f" - {details}"
        
        self.info(message)
        
        # 統計情報に記録
        step_key = step.lower().replace(' ', '_').replace(':', '')
        self.stats[f"{step_key}_before"] = before_count
        self.stats[f"{step_key}_after"] = after_count
        self.stats[f"{step_key}_excluded"] = excluded_count
    
    def log_filter_result(self, filter_name: str, before_count: int, after_count: int, condition: str = ""):
        """
        フィルタリング結果のログを記録
        
        Args:
            filter_name (str): フィルター名
            before_count (int): フィルタリング前データ件数
            after_count (int): フィルタリング後データ件数
            condition (str): フィルタリング条件
        """
        excluded_count = before_count - after_count
        message = f"フィルター[{filter_name}]: {before_count:,}件 → {after_count:,}件"
        
        if excluded_count > 0:
            message += f" (除外: {excluded_count:,}件)"
        
        if condition:
            message += f" - 条件: {condition}"
        
        self.info(message)
    
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