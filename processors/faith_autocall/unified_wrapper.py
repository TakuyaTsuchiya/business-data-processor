"""
フェイスオートコール統合プロセッサーのラッパー関数
既存のインターフェースとの互換性を保つ
"""

from processors.faith_autocall_unified import FaithAutocallUnifiedProcessor
from typing import Tuple, List
import pandas as pd


# プロセッサーのシングルトンインスタンス
_processor = FaithAutocallUnifiedProcessor()


def process_faith_contract_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス契約者データ処理"""
    return _processor.process_faith_autocall(file_content, "contract")


def process_faith_guarantor_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス保証人データ処理"""
    return _processor.process_faith_autocall(file_content, "guarantor")


def process_faith_emergencycontact_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス緊急連絡人データ処理"""
    return _processor.process_faith_autocall(file_content, "emergency_contact")