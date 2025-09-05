"""
SMS系処理のService層

processors/配下のSMS処理を再エクスポートします。
"""

# 統合プロセッサーからインポート
from processors.sms_unified_wrapper import (
    # ミライル系
    process_mirail_sms_contract as process_mirail_sms_contract_data,
    process_mirail_sms_guarantor as process_mirail_sms_guarantor_data,
    process_mirail_sms_emergency as process_mirail_sms_emergencycontact_data,
    # フェイス系
    process_faith_sms_contract as process_faith_sms_contract_data,
    process_faith_sms_guarantor as process_faith_sms_guarantor_data,
    process_faith_sms_emergency as process_faith_sms_emergencycontact_data,
    # プラザ系
    process_plaza_sms_contract as process_plaza_sms_contract_data,
    process_plaza_sms_guarantor as process_plaza_sms_guarantor_data,
    process_plaza_sms_contact as process_plaza_sms_contact_data
)

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