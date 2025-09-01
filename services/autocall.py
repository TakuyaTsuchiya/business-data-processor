"""
オートコール系処理のService層

processors/配下のオートコール処理を再エクスポートします。
"""

# ミライル系
from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data

# フェイス系
from processors.faith_autocall.contract.standard import process_faith_contract_data
from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data
from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data

# プラザ系
from processors.plaza_autocall.main.standard import process_plaza_main_data
from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data
from processors.plaza_autocall.contact.standard import process_plaza_contact_data

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