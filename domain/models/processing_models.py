"""
処理結果・統計情報の標準データモデル

三層アーキテクチャにおける型安全な処理結果を定義します。
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
from .enums import ProcessingStatus, MessageType


@dataclass
class ProcessingStatistics:
    """処理統計データの標準形式
    
    全てのプロセッサーが返す統計情報の統一形式です。
    """
    # 基本統計（必須）
    total_records: int              # 入力データ総件数
    processed_records: int          # 実際に処理された件数
    filtered_records: int           # フィルタ適用後件数
    error_count: int               # エラー発生件数
    processing_time: float         # 処理時間（秒）
    
    # 詳細情報（任意）
    filter_conditions: Dict[str, Any] = field(default_factory=dict)  # 適用フィルタ条件
    memory_usage: float = 0.0           # メモリ使用量（MB）
    timestamp: datetime = field(default_factory=datetime.now)  # 処理実行時刻
    
    def __post_init__(self):
        """データ整合性チェック"""
        if self.processed_records > self.total_records:
            raise ValueError("処理件数が総件数を超えています")
        if self.filtered_records > self.total_records:
            raise ValueError("フィルタ後件数が総件数を超えています")
        if self.processing_time < 0:
            raise ValueError("処理時間は正の値である必要があります")
        if self.error_count < 0:
            raise ValueError("エラー件数は0以上である必要があります")
    
    def to_summary(self) -> str:
        """ユーザー向けサマリー文字列を生成"""
        if self.error_count > 0:
            return f"処理完了: {self.processed_records}件のデータを出力（{self.error_count}件のエラー）"
        else:
            return f"処理完了: {self.processed_records}件のデータを出力"
    
    def to_detailed_dict(self) -> Dict[str, Any]:
        """詳細統計辞書形式に変換"""
        return asdict(self)
    
    def success_rate(self) -> float:
        """処理成功率を計算"""
        if self.total_records == 0:
            return 0.0
        return (self.processed_records - self.error_count) / self.total_records * 100


@dataclass
class ProcessingResult:
    """処理結果の標準形式
    
    全てのプロセッサーが返す処理結果の統一形式です。
    """
    # 必須フィールド
    data: pd.DataFrame              # 処理済みデータ
    statistics: ProcessingStatistics # 統計情報（型保証）
    status: ProcessingStatus        # 処理結果ステータス
    
    # 任意フィールド（メッセージ）
    messages: List[str] = field(default_factory=list)    # 情報メッセージ
    warnings: List[str] = field(default_factory=list)    # 警告メッセージ
    errors: List[str] = field(default_factory=list)      # エラーメッセージ
    
    # メタデータ
    processor_name: Optional[str] = None    # 処理プロセッサー名
    processor_version: Optional[str] = None # プロセッサーバージョン
    
    def __post_init__(self):
        """データ整合性チェック"""
        if not isinstance(self.data, pd.DataFrame):
            raise TypeError(f"dataはpd.DataFrame型である必要があります: {type(self.data)}")
        if not isinstance(self.statistics, ProcessingStatistics):
            raise TypeError(f"statisticsはProcessingStatistics型である必要があります: {type(self.statistics)}")
        
        # ステータスとエラーの整合性チェック
        if self.status == ProcessingStatus.ERROR and len(self.errors) == 0:
            self.errors.append("エラーが発生しましたが、詳細情報がありません")
        if self.status == ProcessingStatus.SUCCESS and len(self.errors) > 0:
            self.status = ProcessingStatus.WARNING
    
    def is_success(self) -> bool:
        """処理成功判定"""
        return self.status == ProcessingStatus.SUCCESS
    
    def is_error(self) -> bool:
        """エラー判定"""
        return self.status == ProcessingStatus.ERROR
    
    def has_warnings(self) -> bool:
        """警告有無判定"""
        return len(self.warnings) > 0 or self.status == ProcessingStatus.WARNING
    
    def add_message(self, message: str, message_type: MessageType = MessageType.INFO):
        """メッセージを追加"""
        if message_type == MessageType.INFO:
            self.messages.append(message)
        elif message_type == MessageType.WARNING:
            self.warnings.append(message)
        elif message_type == MessageType.ERROR:
            self.errors.append(message)
            if self.status == ProcessingStatus.SUCCESS:
                self.status = ProcessingStatus.WARNING
    
    def get_all_messages(self) -> Dict[str, List[str]]:
        """全メッセージを取得"""
        return {
            "info": self.messages,
            "warning": self.warnings,
            "error": self.errors
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換（シリアライズ用）"""
        return {
            "data_shape": self.data.shape,
            "statistics": self.statistics.to_detailed_dict(),
            "status": self.status.value,
            "messages": self.messages,
            "warnings": self.warnings,
            "errors": self.errors,
            "processor_name": self.processor_name,
            "processor_version": self.processor_version
        }