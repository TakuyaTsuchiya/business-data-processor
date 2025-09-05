"""
プラザオートコール統合プロセッサーのラッパー関数
既存のインターフェースとの互換性を保つために、個別の関数を提供
"""

from processors.plaza_autocall_unified import PlazaAutocallUnifiedProcessor
from typing import Tuple, List
import pandas as pd


# プロセッサーのシングルトンインスタンス
_processor = PlazaAutocallUnifiedProcessor()


# 契約者処理
def process_plaza_main_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ契約者データ処理"""
    return _processor.process_plaza_autocall(file_content, "main")


# 保証人処理
def process_plaza_guarantor_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ保証人データ処理"""
    return _processor.process_plaza_autocall(file_content, "guarantor")


# 緊急連絡人処理
def process_plaza_contact_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ緊急連絡人データ処理"""
    return _processor.process_plaza_autocall(file_content, "contact")