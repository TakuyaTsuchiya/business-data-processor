"""
オートコール系処理のService層

processors/配下のオートコール処理を再エクスポートします。
"""

from services.logger import get_logger

# ロガーの取得
logger = get_logger(__name__)

# 統合プロセッサーをインポート
from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor
from processors.faith_autocall_unified import FaithAutocallUnifiedProcessor
from processors.plaza_autocall_unified import PlazaAutocallUnifiedProcessor


# ミライル系ラッパー
def process_mirail_contract_without10k_data(file_content: bytes):
    """ミライル契約者（1万円未満）データ処理"""
    logger.info(f"処理開始 - ミライル契約者（1万円未満） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "contract", with_10k=False)
        logger.info(f"処理完了 - ミライル契約者（1万円未満） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル契約者（1万円未満）: {str(e)}", exc_info=True)
        raise


def process_mirail_contract_with10k_data(file_content: bytes):
    """ミライル契約者（1万円以上）データ処理"""
    logger.info(f"処理開始 - ミライル契約者（1万円以上） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "contract", with_10k=True)
        logger.info(f"処理完了 - ミライル契約者（1万円以上） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル契約者（1万円以上）: {str(e)}", exc_info=True)
        raise


def process_mirail_guarantor_without10k_data(file_content: bytes):
    """ミライル保証人（1万円未満）データ処理"""
    logger.info(f"処理開始 - ミライル保証人（1万円未満） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "guarantor", with_10k=False)
        logger.info(f"処理完了 - ミライル保証人（1万円未満） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル保証人（1万円未満）: {str(e)}", exc_info=True)
        raise


def process_mirail_guarantor_with10k_data(file_content: bytes):
    """ミライル保証人（1万円以上）データ処理"""
    logger.info(f"処理開始 - ミライル保証人（1万円以上） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "guarantor", with_10k=True)
        logger.info(f"処理完了 - ミライル保証人（1万円以上） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル保証人（1万円以上）: {str(e)}", exc_info=True)
        raise


def process_mirail_emergency_contact_without10k_data(file_content: bytes):
    """ミライル緊急連絡先（1万円未満）データ処理"""
    logger.info(f"処理開始 - ミライル緊急連絡先（1万円未満） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "emergency_contact", with_10k=False)
        logger.info(f"処理完了 - ミライル緊急連絡先（1万円未満） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル緊急連絡先（1万円未満）: {str(e)}", exc_info=True)
        raise


def process_mirail_emergency_contact_with10k_data(file_content: bytes):
    """ミライル緊急連絡先（1万円以上）データ処理"""
    logger.info(f"処理開始 - ミライル緊急連絡先（1万円以上） - {len(file_content):,} bytes")
    try:
        processor = MirailAutocallUnifiedProcessor()
        result = processor.process_mirail_autocall(file_content, "emergency_contact", with_10k=True)
        logger.info(f"処理完了 - ミライル緊急連絡先（1万円以上） - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - ミライル緊急連絡先（1万円以上）: {str(e)}", exc_info=True)
        raise


# フェイス系ラッパー
def process_faith_contract_data(file_content: bytes):
    """フェイス契約者データ処理"""
    logger.info(f"処理開始 - フェイス契約者 - {len(file_content):,} bytes")
    try:
        processor = FaithAutocallUnifiedProcessor()
        result = processor.process_faith_autocall(file_content, "contract")
        logger.info(f"処理完了 - フェイス契約者 - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - フェイス契約者: {str(e)}", exc_info=True)
        raise


def process_faith_guarantor_data(file_content: bytes):
    """フェイス保証人データ処理"""
    logger.info(f"処理開始 - フェイス保証人 - {len(file_content):,} bytes")
    try:
        processor = FaithAutocallUnifiedProcessor()
        result = processor.process_faith_autocall(file_content, "guarantor")
        logger.info(f"処理完了 - フェイス保証人 - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - フェイス保証人: {str(e)}", exc_info=True)
        raise


def process_faith_emergencycontact_data(file_content: bytes):
    """フェイス緊急連絡先データ処理"""
    logger.info(f"処理開始 - フェイス緊急連絡先 - {len(file_content):,} bytes")
    try:
        processor = FaithAutocallUnifiedProcessor()
        result = processor.process_faith_autocall(file_content, "emergency_contact")
        logger.info(f"処理完了 - フェイス緊急連絡先 - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - フェイス緊急連絡先: {str(e)}", exc_info=True)
        raise


# プラザ系ラッパー
def process_plaza_main_data(file_content: bytes):
    """プラザメインデータ処理"""
    logger.info(f"処理開始 - プラザメイン - {len(file_content):,} bytes")
    try:
        processor = PlazaAutocallUnifiedProcessor()
        result = processor.process_plaza_autocall(file_content, "main")
        logger.info(f"処理完了 - プラザメイン - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - プラザメイン: {str(e)}", exc_info=True)
        raise


def process_plaza_guarantor_data(file_content: bytes):
    """プラザ保証人データ処理"""
    logger.info(f"処理開始 - プラザ保証人 - {len(file_content):,} bytes")
    try:
        processor = PlazaAutocallUnifiedProcessor()
        result = processor.process_plaza_autocall(file_content, "guarantor")
        logger.info(f"処理完了 - プラザ保証人 - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - プラザ保証人: {str(e)}", exc_info=True)
        raise


def process_plaza_contact_data(file_content: bytes):
    """プラザ連絡先データ処理"""
    logger.info(f"処理開始 - プラザ連絡先 - {len(file_content):,} bytes")
    try:
        processor = PlazaAutocallUnifiedProcessor()
        result = processor.process_plaza_autocall(file_content, "contact")
        logger.info(f"処理完了 - プラザ連絡先 - {len(result[0])}件")
        return result
    except Exception as e:
        logger.error(f"エラー - プラザ連絡先: {str(e)}", exc_info=True)
        raise


# 互換性のためのエイリアス（元の関数名を維持）
process_mirail_emergencycontact_without10k_data = process_mirail_emergency_contact_without10k_data
process_mirail_emergencycontact_with10k_data = process_mirail_emergency_contact_with10k_data

# 公開する関数を明示
__all__ = [
    # ミライル系
    'process_mirail_contract_without10k_data',
    'process_mirail_contract_with10k_data',
    'process_mirail_guarantor_without10k_data',
    'process_mirail_guarantor_with10k_data',
    'process_mirail_emergencycontact_without10k_data',
    'process_mirail_emergencycontact_with10k_data',
    # フェイス系
    'process_faith_contract_data',
    'process_faith_guarantor_data',
    'process_faith_emergencycontact_data',
    # プラザ系
    'process_plaza_main_data',
    'process_plaza_guarantor_data',
    'process_plaza_contact_data',
]