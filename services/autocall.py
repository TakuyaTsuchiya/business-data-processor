"""
オートコール系処理のService層

processors/配下のオートコール処理を再エクスポートします。
"""

import os
from services.logger import get_logger

# ロガーの取得
logger = get_logger(__name__)

# 新アーキテクチャへの移行フラグ（環境変数で制御）
USE_NEW_ARCHITECTURE = os.getenv("USE_NEW_AUTOCALL_ARCHITECTURE", "false").lower() == "true"

if USE_NEW_ARCHITECTURE:
    # 新アーキテクチャを使用
    from services.unified_autocall_service import UnifiedAutocallService
    _service = UnifiedAutocallService()
else:
    # 既存プロセッサーを使用
    from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor
    from processors.faith_autocall_unified import FaithAutocallUnifiedProcessor
    from processors.plaza_autocall_unified import PlazaAutocallUnifiedProcessor


# 汎用ラッパー関数
def _process_generic(system: str, target: str, description: str, file_content: bytes, **kwargs):
    """汎用的なオートコール処理"""
    if USE_NEW_ARCHITECTURE:
        return _service.process(system, target, file_content, **kwargs)
    else:
        logger.info(f"処理開始 - {description} - {len(file_content):,} bytes")
        try:
            # 既存プロセッサーの処理
            if system == "mirail":
                processor = MirailAutocallUnifiedProcessor()
                result = processor.process_mirail_autocall(file_content, target, **kwargs)
            elif system == "faith":
                processor = FaithAutocallUnifiedProcessor()
                result = processor.process_faith_autocall(file_content, target)
            elif system == "plaza":
                processor = PlazaAutocallUnifiedProcessor()
                result = processor.process_plaza_autocall(file_content, target)
            else:
                raise ValueError(f"Unknown system: {system}")
            
            logger.info(f"処理完了 - {description} - {len(result[0])}件")
            return result
        except Exception as e:
            logger.error(f"エラー - {description}: {str(e)}", exc_info=True)
            raise


# ミライル系ラッパー
def process_mirail_contract_without10k_data(file_content: bytes):
    """ミライル契約者（1万円未満）データ処理"""
    return _process_generic("mirail", "contract", "ミライル契約者（1万円未満）", file_content, with_10k=False)


def process_mirail_contract_with10k_data(file_content: bytes):
    """ミライル契約者（1万円以上）データ処理"""
    return _process_generic("mirail", "contract", "ミライル契約者（1万円以上）", file_content, with_10k=True)


def process_mirail_guarantor_without10k_data(file_content: bytes):
    """ミライル保証人（1万円未満）データ処理"""
    return _process_generic("mirail", "guarantor", "ミライル保証人（1万円未満）", file_content, with_10k=False)


def process_mirail_guarantor_with10k_data(file_content: bytes):
    """ミライル保証人（1万円以上）データ処理"""
    return _process_generic("mirail", "guarantor", "ミライル保証人（1万円以上）", file_content, with_10k=True)


def process_mirail_emergency_contact_without10k_data(file_content: bytes):
    """ミライル緊急連絡先（1万円未満）データ処理"""
    return _process_generic("mirail", "emergency_contact", "ミライル緊急連絡先（1万円未満）", file_content, with_10k=False)


def process_mirail_emergency_contact_with10k_data(file_content: bytes):
    """ミライル緊急連絡先（1万円以上）データ処理"""
    return _process_generic("mirail", "emergency_contact", "ミライル緊急連絡先（1万円以上）", file_content, with_10k=True)


# フェイス系ラッパー
def process_faith_contract_data(file_content: bytes):
    """フェイス契約者データ処理"""
    return _process_generic("faith", "contract", "フェイス契約者", file_content)


def process_faith_guarantor_data(file_content: bytes):
    """フェイス保証人データ処理"""
    return _process_generic("faith", "guarantor", "フェイス保証人", file_content)


def process_faith_emergencycontact_data(file_content: bytes):
    """フェイス緊急連絡先データ処理"""
    return _process_generic("faith", "emergency_contact", "フェイス緊急連絡先", file_content)


# プラザ系ラッパー
def process_plaza_main_data(file_content: bytes):
    """プラザメインデータ処理"""
    return _process_generic("plaza", "main", "プラザメイン", file_content)


def process_plaza_guarantor_data(file_content: bytes):
    """プラザ保証人データ処理"""
    return _process_generic("plaza", "guarantor", "プラザ保証人", file_content)


def process_plaza_contact_data(file_content: bytes):
    """プラザ連絡先データ処理"""
    return _process_generic("plaza", "contact", "プラザ連絡先", file_content)


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