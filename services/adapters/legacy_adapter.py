"""
レガシープロセッサー適応層

既存プロセッサーの出力を標準形式に変換します。
"""

from typing import Any, Dict, List, Optional, Union, Tuple
import pandas as pd
import re
import time
from domain.models.processing_models import ProcessingResult, ProcessingStatistics
from domain.models.enums import ProcessingStatus, ErrorType
from infra.logging.logger import create_logger


logger = create_logger(__name__)


class LegacyProcessorAdapter:
    """既存プロセッサー用適応層
    
    様々な形式の処理結果を標準のProcessingResult形式に変換します。
    """
    
    # 既存形式のパターン定義
    LEGACY_PATTERNS = {
        'tuple_with_stats': lambda result: isinstance(result, tuple) and len(result) == 2,
        'dataframe_only': lambda result: isinstance(result, pd.DataFrame),
        'dict_result': lambda result: isinstance(result, dict) and 'data' in result,
        'string_result': lambda result: isinstance(result, str),
        'none_result': lambda result: result is None
    }
    
    @classmethod
    def adapt_result(cls, legacy_result: Any, processor_name: str = "Unknown", 
                    start_time: Optional[float] = None) -> ProcessingResult:
        """レガシー結果を標準形式に変換
        
        Args:
            legacy_result: 既存プロセッサーの返却値
            processor_name: プロセッサー名
            start_time: 処理開始時刻（処理時間計算用）
            
        Returns:
            ProcessingResult: 標準形式の処理結果
        """
        try:
            # 処理時間の計算
            processing_time = time.time() - start_time if start_time else 0.0
            
            # パターンマッチング
            if cls.LEGACY_PATTERNS['tuple_with_stats'](legacy_result):
                return cls._adapt_tuple_result(legacy_result, processor_name, processing_time)
            
            elif cls.LEGACY_PATTERNS['dataframe_only'](legacy_result):
                return cls._adapt_dataframe_result(legacy_result, processor_name, processing_time)
            
            elif cls.LEGACY_PATTERNS['dict_result'](legacy_result):
                return cls._adapt_dict_result(legacy_result, processor_name, processing_time)
            
            elif cls.LEGACY_PATTERNS['string_result'](legacy_result):
                return cls._adapt_string_result(legacy_result, processor_name, processing_time)
            
            elif cls.LEGACY_PATTERNS['none_result'](legacy_result):
                return cls._create_empty_result(processor_name, processing_time, 
                                              "処理結果がありません")
            
            else:
                # 未知の形式
                logger.warning(f"未知の結果形式: {type(legacy_result)}")
                return cls._create_minimal_result(legacy_result, processor_name, processing_time)
                
        except Exception as e:
            logger.error(f"適応層でエラー発生: {str(e)}")
            return cls._create_error_result(str(e), processor_name, processing_time)
    
    @classmethod
    def _adapt_tuple_result(cls, result: Tuple, processor_name: str, 
                          processing_time: float) -> ProcessingResult:
        """タプル形式の結果を変換"""
        df, stats_or_msg = result
        
        if not isinstance(df, pd.DataFrame):
            logger.warning(f"タプルの最初の要素がDataFrameではありません: {type(df)}")
            df = pd.DataFrame()
        
        # 統計情報の処理
        if isinstance(stats_or_msg, dict):
            stats = cls._convert_dict_to_statistics(stats_or_msg, df, processing_time)
        elif isinstance(stats_or_msg, str):
            stats = cls._create_stats_from_string(stats_or_msg, df, processing_time)
        else:
            stats = cls._create_default_statistics(df, processing_time)
        
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=ProcessingStatus.SUCCESS,
            messages=[f"{processor_name}の処理が完了しました"],
            processor_name=processor_name
        )
    
    @classmethod
    def _adapt_dataframe_result(cls, df: pd.DataFrame, processor_name: str,
                               processing_time: float) -> ProcessingResult:
        """DataFrame単体の結果を変換"""
        stats = cls._create_default_statistics(df, processing_time)
        
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=ProcessingStatus.SUCCESS,
            messages=[f"{len(df)}件のデータを処理しました"],
            processor_name=processor_name
        )
    
    @classmethod
    def _adapt_dict_result(cls, result: Dict[str, Any], processor_name: str,
                          processing_time: float) -> ProcessingResult:
        """辞書形式の結果を変換"""
        # データ部分の取得
        df = result.get('data', pd.DataFrame())
        if not isinstance(df, pd.DataFrame):
            logger.warning("辞書の'data'キーがDataFrameではありません")
            df = pd.DataFrame()
        
        # 統計情報の取得
        if 'stats' in result or 'statistics' in result:
            stats_dict = result.get('stats', result.get('statistics', {}))
            stats = cls._convert_dict_to_statistics(stats_dict, df, processing_time)
        else:
            stats = cls._create_default_statistics(df, processing_time)
        
        # ステータスの取得
        status = ProcessingStatus.SUCCESS
        if 'status' in result:
            try:
                status = ProcessingStatus(result['status'])
            except (ValueError, TypeError):
                logger.warning(f"不明なステータス: {result['status']}")
        
        return ProcessingResult(
            data=df,
            statistics=stats,
            status=status,
            messages=result.get('messages', []),
            warnings=result.get('warnings', []),
            errors=result.get('errors', []),
            processor_name=processor_name
        )
    
    @classmethod
    def _adapt_string_result(cls, result: str, processor_name: str,
                           processing_time: float) -> ProcessingResult:
        """文字列結果を変換（通常はエラーメッセージ）"""
        # 文字列から情報を抽出
        stats = cls._create_stats_from_string(result, pd.DataFrame(), processing_time)
        
        # 成功パターンの判定
        if any(word in result for word in ['完了', '成功', '処理済み']):
            return ProcessingResult(
                data=pd.DataFrame(),
                statistics=stats,
                status=ProcessingStatus.SUCCESS,
                messages=[result],
                processor_name=processor_name
            )
        else:
            # エラーとして扱う
            return ProcessingResult(
                data=pd.DataFrame(),
                statistics=stats,
                status=ProcessingStatus.ERROR,
                errors=[result],
                processor_name=processor_name
            )
    
    @classmethod
    def _convert_dict_to_statistics(cls, stats_dict: Dict[str, Any], 
                                  df: pd.DataFrame, processing_time: float) -> ProcessingStatistics:
        """辞書を統計情報に変換"""
        # 必須フィールドの設定
        total_records = stats_dict.get('total_records', 
                                     stats_dict.get('total', len(df)))
        processed_records = stats_dict.get('processed_records', 
                                         stats_dict.get('processed', len(df)))
        filtered_records = stats_dict.get('filtered_records', 
                                        stats_dict.get('filtered', processed_records))
        error_count = stats_dict.get('error_count', 
                                   stats_dict.get('errors', 0))
        
        # フィルタ条件の取得
        filter_conditions = stats_dict.get('filter_conditions', {})
        if not filter_conditions and 'filters' in stats_dict:
            filter_conditions = stats_dict['filters']
        
        return ProcessingStatistics(
            total_records=total_records,
            processed_records=processed_records,
            filtered_records=filtered_records,
            error_count=error_count,
            processing_time=processing_time,
            filter_conditions=filter_conditions
        )
    
    @classmethod
    def _create_stats_from_string(cls, stats_str: str, df: pd.DataFrame,
                                processing_time: float) -> ProcessingStatistics:
        """文字列から統計情報を作成"""
        # 数値パターンの抽出
        numbers = re.findall(r'\d+', stats_str)
        
        # "処理完了: 242件のデータを出力" のようなパターン
        if '件' in stats_str and numbers:
            count = int(numbers[0])
        else:
            count = len(df) if not df.empty else 0
        
        # エラー件数の推定
        error_count = 0
        if 'エラー' in stats_str and len(numbers) > 1:
            error_count = int(numbers[1])
        
        return ProcessingStatistics(
            total_records=count,
            processed_records=count,
            filtered_records=count,
            error_count=error_count,
            processing_time=processing_time
        )
    
    @classmethod
    def _create_default_statistics(cls, df: pd.DataFrame, 
                                 processing_time: float) -> ProcessingStatistics:
        """デフォルト統計情報を作成"""
        record_count = len(df)
        
        return ProcessingStatistics(
            total_records=record_count,
            processed_records=record_count,
            filtered_records=record_count,
            error_count=0,
            processing_time=processing_time
        )
    
    @classmethod
    def _create_minimal_result(cls, result: Any, processor_name: str,
                             processing_time: float) -> ProcessingResult:
        """最小限の結果を作成"""
        logger.warning(f"最小限の結果を作成: {type(result)}")
        
        return ProcessingResult(
            data=pd.DataFrame(),
            statistics=ProcessingStatistics(
                total_records=0,
                processed_records=0,
                filtered_records=0,
                error_count=0,
                processing_time=processing_time
            ),
            status=ProcessingStatus.WARNING,
            warnings=[f"非標準形式の結果: {type(result).__name__}"],
            processor_name=processor_name
        )
    
    @classmethod
    def _create_empty_result(cls, processor_name: str, processing_time: float,
                           message: str) -> ProcessingResult:
        """空の結果を作成"""
        return ProcessingResult(
            data=pd.DataFrame(),
            statistics=ProcessingStatistics(
                total_records=0,
                processed_records=0,
                filtered_records=0,
                error_count=0,
                processing_time=processing_time
            ),
            status=ProcessingStatus.WARNING,
            warnings=[message],
            processor_name=processor_name
        )
    
    @classmethod
    def _create_error_result(cls, error_message: str, processor_name: str,
                           processing_time: float) -> ProcessingResult:
        """エラー結果を作成"""
        return ProcessingResult(
            data=pd.DataFrame(),
            statistics=ProcessingStatistics(
                total_records=0,
                processed_records=0,
                filtered_records=0,
                error_count=1,
                processing_time=processing_time
            ),
            status=ProcessingStatus.ERROR,
            errors=[error_message],
            processor_name=processor_name
        )