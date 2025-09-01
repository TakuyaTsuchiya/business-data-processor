"""
SMS系処理のService層

processors/配下のSMS処理を再エクスポートします。
"""

# ミライル系
from processors.mirail_sms.contract import process_mirail_sms_contract_data
from processors.mirail_sms.guarantor import process_mirail_sms_guarantor_data
from processors.mirail_sms.emergency_contact import process_mirail_sms_emergency_contract_data

# フェイス系
from processors.faith_sms.contract import process_faith_sms_contract_data
from processors.faith_sms.guarantor import process_faith_sms_guarantor_data
from processors.faith_sms.emergency_contact import process_faith_sms_emergency_contract_data

# プラザ系
from processors.plaza_sms.contract import process_plaza_sms_contract_data
from processors.plaza_sms.guarantor import process_plaza_sms_guarantor_data
from processors.plaza_sms.contact import process_plaza_sms_contact_data

# 公開する関数を明示
__all__ = [
    # ミライル系
    'process_mirail_sms_contract_data',
    'process_mirail_sms_guarantor_data',
    'process_mirail_sms_emergency_contract_data',
    # フェイス系
    'process_faith_sms_contract_data',
    'process_faith_sms_guarantor_data',
    'process_faith_sms_emergency_contract_data',
    # プラザ系
    'process_plaza_sms_contract_data',
    'process_plaza_sms_guarantor_data',
    'process_plaza_sms_contact_data',
]