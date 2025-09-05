"""
SMS統合プロセッサーのラッパー関数
既存のインターフェースとの互換性を保つために、個別の関数を提供
"""

from processors.sms_unified import SmsUnifiedProcessor
from typing import Tuple, List
from datetime import date
import pandas as pd


# プロセッサーのシングルトンインスタンス
_processor = SmsUnifiedProcessor()


# ミライル系
def process_mirail_sms_contract(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル契約者SMS処理"""
    results = _processor.process_sms(file_content, "mirail", "contract", payment_deadline)
    # 単一ファイル出力なので、リストから取り出す
    return results[0][0], results[1], results[2][0]


def process_mirail_sms_guarantor(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル保証人SMS処理"""
    results = _processor.process_sms(file_content, "mirail", "guarantor", payment_deadline)
    return results[0][0], results[1], results[2][0]


def process_mirail_sms_emergency(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライル緊急連絡先SMS処理"""
    results = _processor.process_sms(file_content, "mirail", "emergency", payment_deadline)
    return results[0][0], results[1], results[2][0]


# フェイス系
def process_faith_sms_contract(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス契約者SMS処理"""
    results = _processor.process_sms(file_content, "faith", "contract", payment_deadline)
    return results[0][0], results[1], results[2][0]


def process_faith_sms_guarantor(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス保証人SMS処理"""
    results = _processor.process_sms(file_content, "faith", "guarantor", payment_deadline)
    return results[0][0], results[1], results[2][0]


def process_faith_sms_emergency(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイス緊急連絡人SMS処理"""
    results = _processor.process_sms(file_content, "faith", "emergency", payment_deadline)
    return results[0][0], results[1], results[2][0]


# プラザ系（将来的に国籍分離対応）
def process_plaza_sms_contract(
    file_content: bytes,
    payment_deadline: date,
    call_center_file: bytes = None
) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
    """プラザ契約者SMS処理（複数ファイル出力対応）"""
    return _processor.process_sms(
        file_content, "plaza", "contract", payment_deadline, call_center_file
    )


def process_plaza_sms_guarantor(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ保証人SMS処理"""
    results = _processor.process_sms(file_content, "plaza", "guarantor", payment_deadline)
    return results[0][0], results[1], results[2][0]


def process_plaza_sms_contact(
    file_content: bytes,
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ連絡先SMS処理"""
    results = _processor.process_sms(file_content, "plaza", "emergency", payment_deadline)
    return results[0][0], results[1], results[2][0]