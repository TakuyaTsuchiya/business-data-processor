"""
型検証ユーティリティ

ランタイム型検証・変換機能を提供します。
"""

from typing import Any, Optional, Type, TypeVar, Dict, List
import pandas as pd
from domain.models.processing_models import ProcessingResult, ProcessingStatistics
from domain.models.enums import ProcessingStatus, ErrorType
from infra.logging.logger import create_logger


T = TypeVar('T')
logger = create_logger(__name__)


class ValidationError(Exception):
    """検証エラー"""
    def __init__(self, message: str, error_type: ErrorType = ErrorType.VALIDATION_ERROR):
        super().__init__(message)
        self.error_type = error_type


class TypeValidator:
    """ランタイム型検証クラス"""
    
    @staticmethod
    def validate_processing_result(result: Any) -> ProcessingResult:
        """ProcessingResult型の検証と変換
        
        Args:
            result: 検証対象オブジェクト
            
        Returns:
            ProcessingResult: 検証済みオブジェクト
            
        Raises:
            ValidationError: 型変換失敗時
        """
        # すでにProcessingResult型の場合
        if isinstance(result, ProcessingResult):
            logger.debug("ProcessingResult型として検証成功")
            return result
        
        # 辞書型からの変換試行
        if isinstance(result, dict):
            try:
                return TypeValidator._convert_dict_to_result(result)
            except Exception as e:
                raise ValidationError(
                    f"辞書からProcessingResultへの変換失敗: {str(e)}", 
                    ErrorType.TYPE_ERROR
                )
        
        # DataFrameのみの場合
        if isinstance(result, pd.DataFrame):
            logger.info("DataFrameのみの結果をProcessingResultに変換")
            return TypeValidator._create_result_from_dataframe(result)
        
        # タプル形式の場合
        if isinstance(result, tuple):
            return TypeValidator._convert_tuple_to_result(result)
        
        # 文字列の場合（エラーメッセージ）
        if isinstance(result, str):
            logger.warning(f"文字列結果をエラーとして処理: {result}")
            return TypeValidator._create_error_result(result)
        
        # その他の型
        raise ValidationError(
            f"ProcessingResult型が期待されましたが、{type(result).__name__}型が渡されました",
            ErrorType.TYPE_ERROR
        )
    
    @staticmethod
    def validate_processing_statistics(stats: Any) -> ProcessingStatistics:
        """ProcessingStatistics型の検証と変換
        
        Args:
            stats: 検証対象オブジェクト
            
        Returns:
            ProcessingStatistics: 検証済みオブジェクト
            
        Raises:
            ValidationError: 型変換失敗時
        """
        # すでにProcessingStatistics型の場合
        if isinstance(stats, ProcessingStatistics):
            return stats
        
        # 辞書型からの変換
        if isinstance(stats, dict):
            try:
                # 必須フィールドの確認
                required_fields = ['total_records', 'processed_records', 'filtered_records', 
                                 'error_count', 'processing_time']
                missing_fields = [f for f in required_fields if f not in stats]
                
                if missing_fields:
                    # デフォルト値で補完
                    logger.warning(f"統計情報の不足フィールドを補完: {missing_fields}")
                    stats = TypeValidator._fill_missing_stats_fields(stats)
                
                return ProcessingStatistics(**stats)
            except Exception as e:
                raise ValidationError(
                    f"辞書からProcessingStatisticsへの変換失敗: {str(e)}",
                    ErrorType.TYPE_ERROR
                )
        
        # 文字列の場合（レガシー形式）
        if isinstance(stats, str):
            logger.warning(f"文字列統計情報を基本形式に変換: {stats}")
            return TypeValidator._create_stats_from_string(stats)
        
        raise ValidationError(
            f"ProcessingStatistics型が期待されましたが、{type(stats).__name__}型が渡されました",
            ErrorType.TYPE_ERROR
        )
    
    @staticmethod
    def validate_dataframe(df: Any) -> pd.DataFrame:
        """DataFrame型の検証
        
        Args:
            df: 検証対象オブジェクト
            
        Returns:
            pd.DataFrame: 検証済みDataFrame
            
        Raises:
            ValidationError: DataFrame以外の場合
        """
        if not isinstance(df, pd.DataFrame):
            raise ValidationError(
                f"DataFrame型が期待されましたが、{type(df).__name__}型が渡されました",
                ErrorType.TYPE_ERROR
            )
        return df
    
    @staticmethod
    def _convert_dict_to_result(data: Dict[str, Any]) -> ProcessingResult:
        """辞書をProcessingResultに変換"""
        # 必須フィールドの確認
        if 'data' not in data:
            raise ValueError("'data'フィールドが必要です")
        
        # DataFrameの検証
        df = TypeValidator.validate_dataframe(data['data'])
        
        # 統計情報の処理
        if 'statistics' in data:
            stats = TypeValidator.validate_processing_statistics(data['statistics'])
        else:
            stats = TypeValidator._create_default_statistics(df)
        
        # ステータスの処理
        status = ProcessingStatus.SUCCESS
        if 'status' in data:
            if isinstance(data['status'], ProcessingStatus):
                status = data['status']
            elif isinstance(data['status'], str):
                try:
                    status = ProcessingStatus(data['status'])
                except ValueError:
                    logger.warning(f"不明なステータス: {data['status']}")
        
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=status,
            messages=data.get('messages', []),
            warnings=data.get('warnings', []),
            errors=data.get('errors', [])
        )
    
    @staticmethod
    def _convert_tuple_to_result(data: tuple) -> ProcessingResult:
        """タプルをProcessingResultに変換"""
        if len(data) != 2:
            raise ValueError(f"2要素のタプルが期待されましたが、{len(data)}要素でした")
        
        df = TypeValidator.validate_dataframe(data[0])
        
        # 2番目の要素に応じて処理
        if isinstance(data[1], dict):
            stats = TypeValidator.validate_processing_statistics(data[1])
        elif isinstance(data[1], str):
            stats = TypeValidator._create_stats_from_string(data[1])
        else:
            stats = TypeValidator._create_default_statistics(df)
        
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=ProcessingStatus.SUCCESS
        )
    
    @staticmethod
    def _create_result_from_dataframe(df: pd.DataFrame) -> ProcessingResult:
        """DataFrameからProcessingResultを作成"""
        stats = TypeValidator._create_default_statistics(df)
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=ProcessingStatus.SUCCESS,
            messages=[f"{len(df)}件のデータを処理しました"]
        )
    
    @staticmethod
    def _create_error_result(error_message: str) -> ProcessingResult:
        """エラーメッセージからProcessingResultを作成"""
        return ProcessingResult(
            data=pd.DataFrame(),
            statistics=ProcessingStatistics(
                total_records=0,
                processed_records=0,
                filtered_records=0,
                error_count=1,
                processing_time=0.0
            ),
            status=ProcessingStatus.ERROR,
            errors=[error_message]
        )
    
    @staticmethod
    def _create_default_statistics(df: pd.DataFrame) -> ProcessingStatistics:
        """デフォルト統計情報を作成"""
        record_count = len(df)
        return ProcessingStatistics(
            total_records=record_count,
            processed_records=record_count,
            filtered_records=record_count,
            error_count=0,
            processing_time=0.0
        )
    
    @staticmethod
    def _create_stats_from_string(stats_str: str) -> ProcessingStatistics:
        """文字列から統計情報を作成（レガシー対応）"""
        # "処理完了: 242件のデータを出力" のようなパターンを解析
        import re
        match = re.search(r'(\d+)件', stats_str)
        count = int(match.group(1)) if match else 0
        
        return ProcessingStatistics(
            total_records=count,
            processed_records=count,
            filtered_records=count,
            error_count=0,
            processing_time=0.0
        )
    
    @staticmethod
    def _fill_missing_stats_fields(stats: Dict[str, Any]) -> Dict[str, Any]:
        """不足している統計フィールドを補完"""
        defaults = {
            'total_records': 0,
            'processed_records': 0,
            'filtered_records': 0,
            'error_count': 0,
            'processing_time': 0.0
        }
        
        for field, default_value in defaults.items():
            if field not in stats:
                stats[field] = default_value
        
        return stats