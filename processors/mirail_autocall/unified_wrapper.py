"""
ミライルオートコール統合プロセッサーのラッパー関数
既存のインターフェースとの互換性を保つために、個別の関数を提供
"""

from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor
from typing import Tuple, List
import pandas as pd


# プロセッサーのシングルトンインスタンス
_processor = MirailAutocallUnifiedProcessor()


# 契約者処理
def process_mirail_contract_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル契約者（10,000円除外）データ処理"""
    return _processor.process_mirail_autocall(file_content, "contract", with_10k=False)


def process_mirail_contract_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル契約者（10,000円含む）データ処理"""
    return _processor.process_mirail_autocall(file_content, "contract", with_10k=True)


# 保証人処理
def process_mirail_guarantor_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル保証人（10,000円除外）データ処理"""
    return _processor.process_mirail_autocall(file_content, "guarantor", with_10k=False)


def process_mirail_guarantor_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル保証人（10,000円含む）データ処理"""
    return _processor.process_mirail_autocall(file_content, "guarantor", with_10k=True)


# 緊急連絡先処理
def process_mirail_emergency_contact_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル緊急連絡先（10,000円除外）データ処理"""
    return _processor.process_mirail_autocall(file_content, "emergency_contact", with_10k=False)


def process_mirail_emergency_contact_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル緊急連絡先（10,000円含む）データ処理"""
    return _processor.process_mirail_autocall(file_content, "emergency_contact", with_10k=True)