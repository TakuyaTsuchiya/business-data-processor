"""
プロセッサー実行サービス - Services Layer

このモジュールは、UI層とプロセッサー層の間の調整を行い、
共通的なプロセッサー実行パターンを統一したインターフェースで提供します。

統合対象：
- 単一ファイル処理パターン（13関数）
- 複数ファイル処理パターン（8関数）  
- プログレス表示付き処理パターン（1関数）
- エラーハンドリングの標準化
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Callable, Tuple, Union
from datetime import datetime
import traceback

from infra.logging.logger import create_logger


class ProcessorResult:
    """プロセッサー実行結果を格納するデータクラス"""
    
    def __init__(
        self,
        success: bool,
        result_df: Optional[pd.DataFrame] = None,
        filtered_df: Optional[pd.DataFrame] = None,
        logs: Optional[List[str]] = None,
        filename: Optional[str] = None,
        stats: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ):
        self.success = success
        self.result_df = result_df if result_df is not None else pd.DataFrame()
        self.filtered_df = filtered_df if filtered_df is not None else pd.DataFrame()
        self.logs = logs or []
        self.filename = filename or ""
        self.stats = stats or {}
        self.error_message = error_message
    
    @property
    def has_data(self) -> bool:
        """処理結果にデータが存在するか"""
        return not self.result_df.empty
    
    @property
    def row_count(self) -> int:
        """処理結果の行数"""
        return len(self.result_df)


class ProcessorExecutionService:
    """
    プロセッサー実行を統合管理するサービスクラス
    
    各プロセッサーの実行パターンを統一し、共通のエラーハンドリングと
    結果管理を提供します。
    """
    
    @staticmethod
    def execute_single_file_processor(
        processor_func: Callable,
        file_content: bytes,
        processor_name: str,
        **kwargs
    ) -> ProcessorResult:
        """
        単一ファイル処理プロセッサーを実行
        
        Args:
            processor_func (Callable): プロセッサー関数
            file_content (bytes): CSVファイル内容
            processor_name (str): プロセッサー名（ログ用）
            **kwargs: プロセッサーに渡す追加パラメータ
            
        Returns:
            ProcessorResult: 実行結果
            
        Examples:
            >>> from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
            >>> result = ProcessorExecutionService.execute_single_file_processor(
            ...     process_mirail_contract_without10k_data,
            ...     file_content,
            ...     "ミライル契約者without10k"
            ... )
            >>> if result.success and result.has_data:
            ...     print(f"処理完了: {result.row_count}件")
        """
        service_logger = create_logger(f"ProcessorService-{processor_name}")
        
        try:
            service_logger.info(f"プロセッサー実行開始: {processor_name}")
            
            # プロセッサー関数を実行
            processor_result = processor_func(file_content, **kwargs)
            
            # 戻り値の形式を正規化（プロセッサーによって異なる可能性がある）
            result_df, filtered_df, logs, filename = ProcessorExecutionService._normalize_processor_result(
                processor_result, processor_name
            )
            
            service_logger.info(f"プロセッサー実行完了: {len(result_df)}件出力")
            
            return ProcessorResult(
                success=True,
                result_df=result_df,
                filtered_df=filtered_df,
                logs=logs,
                filename=filename
            )
            
        except Exception as e:
            error_msg = f"{processor_name}処理エラー: {str(e)}"
            service_logger.error(error_msg)
            service_logger.error(f"スタックトレース: {traceback.format_exc()}")
            
            return ProcessorResult(
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def execute_multiple_file_processor(
        processor_func: Callable,
        file_contents: List[bytes],
        processor_name: str,
        **kwargs
    ) -> ProcessorResult:
        """
        複数ファイル処理プロセッサーを実行
        
        Args:
            processor_func (Callable): プロセッサー関数
            file_contents (List[bytes]): CSVファイル内容のリスト
            processor_name (str): プロセッサー名（ログ用）
            **kwargs: プロセッサーに渡す追加パラメータ
            
        Returns:
            ProcessorResult: 実行結果
            
        Examples:
            >>> from processors.ark_registration import process_ark_data
            >>> result = ProcessorExecutionService.execute_multiple_file_processor(
            ...     process_ark_data,
            ...     [file1_content, file2_content],
            ...     "アーク新規登録",
            ...     region_code=1
            ... )
        """
        service_logger = create_logger(f"ProcessorService-{processor_name}")
        
        try:
            service_logger.info(f"複数ファイルプロセッサー実行開始: {processor_name}")
            service_logger.info(f"ファイル数: {len(file_contents)}")
            
            # プロセッサー関数を実行（複数ファイルを引数として渡す）
            processor_result = processor_func(*file_contents, **kwargs)
            
            # 戻り値の形式を正規化
            result_df, logs, stats = ProcessorExecutionService._normalize_multiple_file_result(
                processor_result, processor_name
            )
            
            # ファイル名生成
            filename = ProcessorExecutionService._generate_filename(processor_name, **kwargs)
            
            service_logger.info(f"複数ファイルプロセッサー実行完了: {len(result_df)}件出力")
            
            return ProcessorResult(
                success=True,
                result_df=result_df,
                logs=logs,
                filename=filename,
                stats=stats
            )
            
        except Exception as e:
            error_msg = f"{processor_name}処理エラー: {str(e)}"
            service_logger.error(error_msg)
            service_logger.error(f"スタックトレース: {traceback.format_exc()}")
            
            return ProcessorResult(
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def execute_with_progress(
        processor_func: Callable,
        file_contents: List[bytes],
        processor_name: str,
        progress_callback: Optional[Callable[[float, str], None]] = None,
        **kwargs
    ) -> ProcessorResult:
        """
        プログレス表示付きプロセッサーを実行
        
        Args:
            processor_func (Callable): プロセッサー関数
            file_contents (List[bytes]): CSVファイル内容のリスト
            processor_name (str): プロセッサー名（ログ用）
            progress_callback (Callable): プログレスコールバック関数
            **kwargs: プロセッサーに渡す追加パラメータ
            
        Returns:
            ProcessorResult: 実行結果
            
        Examples:
            >>> def update_progress(progress: float, message: str):
            ...     st.progress(progress)
            ...     st.text(message)
            >>> 
            >>> result = ProcessorExecutionService.execute_with_progress(
            ...     process_capco_debt_update,
            ...     [file1_content, file2_content],
            ...     "カプコ残債更新",
            ...     progress_callback=update_progress
            ... )
        """
        service_logger = create_logger(f"ProcessorService-{processor_name}")
        
        try:
            service_logger.info(f"プログレス付きプロセッサー実行開始: {processor_name}")
            
            # プログレスコールバックを含めてプロセッサーを実行
            if progress_callback:
                processor_result = processor_func(*file_contents, progress_callback=progress_callback, **kwargs)
            else:
                processor_result = processor_func(*file_contents, **kwargs)
            
            # 戻り値の形式を正規化（プログレス付きプロセッサー用）
            result_df, filename, stats = ProcessorExecutionService._normalize_progress_result(
                processor_result, processor_name
            )
            
            service_logger.info(f"プログレス付きプロセッサー実行完了: {len(result_df)}件出力")
            
            return ProcessorResult(
                success=True,
                result_df=result_df,
                filename=filename,
                stats=stats
            )
            
        except Exception as e:
            error_msg = f"{processor_name}処理エラー: {str(e)}"
            service_logger.error(error_msg)
            service_logger.error(f"スタックトレース: {traceback.format_exc()}")
            
            return ProcessorResult(
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _normalize_processor_result(
        result: Tuple,
        processor_name: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
        """
        単一ファイルプロセッサーの戻り値を正規化
        
        Args:
            result (Tuple): プロセッサーの戻り値
            processor_name (str): プロセッサー名
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, List[str], str]: 正規化された結果
        """
        try:
            if len(result) == 4:
                # 標準形式: (result_df, filtered_df, logs, filename)
                return result
            elif len(result) == 3:
                # ログなし形式: (result_df, filtered_df, filename)
                result_df, filtered_df, filename = result
                return result_df, filtered_df, [], filename
            else:
                raise ValueError(f"未対応の戻り値形式: {len(result)}要素")
        except Exception as e:
            raise ValueError(f"{processor_name}の戻り値正規化エラー: {str(e)}")
    
    @staticmethod
    def _normalize_multiple_file_result(
        result: Tuple,
        processor_name: str
    ) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
        """
        複数ファイルプロセッサーの戻り値を正規化
        
        Args:
            result (Tuple): プロセッサーの戻り値
            processor_name (str): プロセッサー名
            
        Returns:
            Tuple[pd.DataFrame, List[str], Dict[str, Any]]: 正規化された結果
        """
        try:
            if len(result) == 3:
                # 標準形式: (result_df, logs, stats)
                return result
            elif len(result) == 2:
                # 統計なし形式: (result_df, logs)
                result_df, logs = result
                return result_df, logs, {}
            else:
                raise ValueError(f"未対応の戻り値形式: {len(result)}要素")
        except Exception as e:
            raise ValueError(f"{processor_name}の戻り値正規化エラー: {str(e)}")
    
    @staticmethod
    def _normalize_progress_result(
        result: Tuple,
        processor_name: str
    ) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
        """
        プログレス付きプロセッサーの戻り値を正規化
        
        Args:
            result (Tuple): プロセッサーの戻り値
            processor_name (str): プロセッサー名
            
        Returns:
            Tuple[pd.DataFrame, str, Dict[str, Any]]: 正規化された結果
        """
        try:
            if len(result) == 3:
                # 標準形式: (result_df, filename, stats)
                return result
            elif len(result) == 2:
                # 統計なし形式: (result_df, filename)
                result_df, filename = result
                return result_df, filename, {}
            else:
                raise ValueError(f"未対応の戻り値形式: {len(result)}要素")
        except Exception as e:
            raise ValueError(f"{processor_name}の戻り値正規化エラー: {str(e)}")
    
    @staticmethod
    def _generate_filename(processor_name: str, **kwargs) -> str:
        """
        プロセッサー名とパラメータからファイル名を生成
        
        Args:
            processor_name (str): プロセッサー名
            **kwargs: ファイル名生成に使用する追加パラメータ
            
        Returns:
            str: 生成されたファイル名
        """
        timestamp = datetime.now().strftime("%m%d")
        
        # パラメータからファイル名要素を抽出
        region_mapping = {1: "東京", 2: "大阪", 3: "北海道", 4: "北関東"}
        
        if "region_code" in kwargs:
            region = region_mapping.get(kwargs["region_code"], "不明")
            return f"{timestamp}{processor_name}_{region}.csv"
        
        return f"{timestamp}{processor_name}.csv"