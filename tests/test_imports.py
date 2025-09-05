"""
全プロセッサーのインポートテスト
実行前にインポートエラーを検出
"""

import pytest


def test_unified_processors_import():
    """統合プロセッサーがインポートできることを確認"""
    try:
        # ミライル（既存）
        from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor
        
        # フェイス（新規）
        from processors.faith_autocall_unified import FaithAutocallUnifiedProcessor
        
        # プラザ（新規）
        from processors.plaza_autocall_unified import PlazaAutocallUnifiedProcessor
        
        # SMS統合（新規）
        from processors.sms_unified import SmsUnifiedProcessor
        
        # 共通モジュール
        from processors.autocall_constants import AUTOCALL_OUTPUT_COLUMNS  # ファイルから
        from processors.autocall_common.filter_engine import apply_filters  # ディレクトリから
        from processors.common.contract_list_columns import ContractListColumns
        from processors.common.detailed_logger import DetailedLogger
        
        # インフラストラクチャ層
        from infrastructure import (
            EncodingHandler, 
            read_csv_auto_encoding,
            FileWriter,
            to_csv_bytes,
            to_csv_cp932_safe,
            to_excel_bytes
        )
        
        assert True  # インポート成功
    except ImportError as e:
        pytest.fail(f"インポートエラー: {e}")


def test_filter_engine_available():
    """FilterEngineが正しく動作することを確認"""
    from processors.autocall_common.filter_engine import apply_filters
    import pandas as pd
    
    # 簡単な動作確認
    assert callable(apply_filters)
    
    # 空のDataFrameでテスト
    df = pd.DataFrame()
    config = {}
    result = apply_filters(df, config)
    assert isinstance(result, tuple)
    assert len(result) == 2  # (DataFrame, logs)


def test_service_layer_imports():
    """サービス層からのインポートを確認"""
    try:
        from services.autocall import (
            # ミライル
            process_mirail_contract_without10k_data,
            process_mirail_contract_with10k_data,
            process_mirail_guarantor_without10k_data,
            process_mirail_guarantor_with10k_data,
            process_mirail_emergencycontact_without10k_data,
            process_mirail_emergencycontact_with10k_data,
            # フェイス
            process_faith_contract_data,
            process_faith_guarantor_data,
            process_faith_emergencycontact_data,
            # プラザ
            process_plaza_main_data,
            process_plaza_guarantor_data,
            process_plaza_contact_data
        )
        
        from services.sms import (
            # ミライル SMS
            process_mirail_sms_contract_data,
            process_mirail_sms_guarantor_data,
            process_mirail_sms_emergencycontact_data,
            # フェイス SMS
            process_faith_sms_contract_data,
            process_faith_sms_guarantor_data,
            process_faith_sms_emergencycontact_data,
            # プラザ SMS
            process_plaza_sms_contract_data,
            process_plaza_sms_guarantor_data,
            process_plaza_sms_contact_data
        )
        
        assert True  # 全てインポート成功
    except ImportError as e:
        pytest.fail(f"サービス層インポートエラー: {e}")


def test_column_definitions():
    """列定義が正しく読み込めることを確認"""
    from processors.common.contract_list_columns import ContractListColumns as COL
    
    # 主要な列が定義されていることを確認
    assert hasattr(COL, 'MANAGEMENT_NO')
    assert hasattr(COL, 'TEL_MOBILE')
    assert hasattr(COL, 'TEL_MOBILE_1')
    assert hasattr(COL, 'TEL_MOBILE_2')
    assert hasattr(COL, 'TRUSTEE_ID')
    assert hasattr(COL, 'COLLECTION_RANK')
    assert hasattr(COL, 'PAYMENT_DATE')
    assert hasattr(COL, 'PAYMENT_AMOUNT')