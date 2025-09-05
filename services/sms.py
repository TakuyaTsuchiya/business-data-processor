"""
SMS系処理のService層

processors/配下のSMS処理を再エクスポートします。
"""

from datetime import date
from typing import Optional, Tuple, List
import pandas as pd

from services.logger import get_logger

# ロガーの取得
logger = get_logger(__name__)

# 統合プロセッサーをインポート
from processors.sms_unified import SmsUnifiedProcessor


# ミライル系ラッパー
def process_mirail_sms_contract_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライルSMS契約者データ処理"""
    logger.info(f"処理開始 - ミライルSMS契約者 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "mirail", "contract", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - ミライルSMS契約者 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - ミライルSMS契約者: {str(e)}", exc_info=True)
        raise


def process_mirail_sms_guarantor_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライルSMS保証人データ処理"""
    logger.info(f"処理開始 - ミライルSMS保証人 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "mirail", "guarantor", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - ミライルSMS保証人 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - ミライルSMS保証人: {str(e)}", exc_info=True)
        raise


def process_mirail_sms_emergencycontact_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """ミライルSMS緊急連絡先データ処理"""
    logger.info(f"処理開始 - ミライルSMS緊急連絡先 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "mirail", "emergency", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - ミライルSMS緊急連絡先 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - ミライルSMS緊急連絡先: {str(e)}", exc_info=True)
        raise


# フェイス系ラッパー
def process_faith_sms_contract_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイスSMS契約者データ処理"""
    logger.info(f"処理開始 - フェイスSMS契約者 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "faith", "contract", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - フェイスSMS契約者 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - フェイスSMS契約者: {str(e)}", exc_info=True)
        raise


def process_faith_sms_guarantor_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイスSMS保証人データ処理"""
    logger.info(f"処理開始 - フェイスSMS保証人 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "faith", "guarantor", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - フェイスSMS保証人 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - フェイスSMS保証人: {str(e)}", exc_info=True)
        raise


def process_faith_sms_emergencycontact_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """フェイスSMS緊急連絡先データ処理"""
    logger.info(f"処理開始 - フェイスSMS緊急連絡先 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "faith", "emergency", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - フェイスSMS緊急連絡先 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - フェイスSMS緊急連絡先: {str(e)}", exc_info=True)
        raise


# プラザ系ラッパー
def process_plaza_sms_contract_data(
    file_content: bytes, 
    payment_deadline: date,
    call_center_file: Optional[bytes] = None
) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
    """プラザSMS契約者データ処理（複数ファイル対応）"""
    logger.info(f"処理開始 - プラザSMS契約者 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(
            file_content, "plaza", "contract", payment_deadline, call_center_file
        )
        df_outputs = results[0]
        logs = results[1]
        filenames = results[2]
        total_count = sum(len(df) for df in df_outputs)
        logger.info(f"処理完了 - プラザSMS契約者 - {total_count}件 ({len(filenames)}ファイル)")
        return df_outputs, logs, filenames
    except Exception as e:
        logger.error(f"エラー - プラザSMS契約者: {str(e)}", exc_info=True)
        raise


def process_plaza_sms_guarantor_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザSMS保証人データ処理"""
    logger.info(f"処理開始 - プラザSMS保証人 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "plaza", "guarantor", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - プラザSMS保証人 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - プラザSMS保証人: {str(e)}", exc_info=True)
        raise


def process_plaza_sms_contact_data(
    file_content: bytes, 
    payment_deadline: date
) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザSMS連絡人データ処理"""
    logger.info(f"処理開始 - プラザSMS連絡人 - {len(file_content):,} bytes")
    try:
        processor = SmsUnifiedProcessor()
        results = processor.process_sms(file_content, "plaza", "emergency", payment_deadline)
        df_output = results[0][0]
        logs = results[1]
        filename = results[2][0]
        logger.info(f"処理完了 - プラザSMS連絡人 - {len(df_output)}件")
        return df_output, logs, filename
    except Exception as e:
        logger.error(f"エラー - プラザSMS連絡人: {str(e)}", exc_info=True)
        raise


# 公開する関数を明示
__all__ = [
    # ミライル系
    'process_mirail_sms_contract_data',
    'process_mirail_sms_guarantor_data',
    'process_mirail_sms_emergencycontact_data',
    # フェイス系
    'process_faith_sms_contract_data',
    'process_faith_sms_guarantor_data',
    'process_faith_sms_emergencycontact_data',
    # プラザ系
    'process_plaza_sms_contract_data',
    'process_plaza_sms_guarantor_data',
    'process_plaza_sms_contact_data',
]