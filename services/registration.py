"""
新規登録系処理のService層

processors/配下の新規登録処理を再エクスポートします。
"""

# アーク系
from processors.ark_registration import process_ark_data, process_arktrust_data

# カプコ系
from processors.capco_registration import process_capco_data

# プラザ系
from processors.plaza_registration import process_plaza_data

# IOG系
from processors.iog_registration import process_jid_data

# 公開する関数を明示
__all__ = [
    # アーク系
    'process_ark_data',
    'process_arktrust_data',
    # カプコ系
    'process_capco_data',
    # プラザ系
    'process_plaza_data',
    # IOG系
    'process_jid_data',
]