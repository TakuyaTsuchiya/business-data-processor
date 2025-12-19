"""
ビジネスルール定義
Business Data Processor

このファイルには、システム全体で使用されるビジネスルール（定数）を定義します。
"""

# 委託先法人ID定義
CLIENT_IDS = {
    'faith': [1, 2, 3, 4],          # フェイス
    'faith_contract': [1, 2, 3, 4, 7],  # フェイス契約者（オートコール用）
    'mirail': ['', '5'],            # ミライル（空白と5）
    'plaza': [6]                    # プラザ
}

# 除外金額定義
EXCLUDE_AMOUNTS = {
    'faith': [2, 3, 5],             # フェイス
    'mirail': [2, 3, 5],            # ミライル
    'plaza': [2, 3, 5, 12]          # プラザ
}

# ミライル残債除外金額
MIRAIL_DEBT_EXCLUDE = [10000, 11000]  # 1万円、1万1千円