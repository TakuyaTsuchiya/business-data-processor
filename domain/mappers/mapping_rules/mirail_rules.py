"""
ミライルシステム用マッピングルール定義

ミライルオートコール処理で使用される各種マッピングルールを定義します。
契約者、保証人、緊急連絡人それぞれのルールを管理します。
"""

from typing import Dict, Any, Callable


# 契約者用マッピングルール
CONTRACT_MAPPING_RULES = {
    "電話番号": "TEL携帯",
    "架電番号": "TEL携帯",
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "管理番号": "管理番号",
    "契約者名（カナ）": "契約者カナ",
    "物件名": "物件名",
    "クライアント": "クライアント名",
    "残債": "滞納残債"  # J列「残債」にBT列「滞納残債」を格納
}

# 契約者用デフォルト値
CONTRACT_DEFAULT_VALUES = {
    "連番": "",
    "架電日": "",
    "架電月": "",
    "架電番号カウント": "",
    "通話秒数": "",
    "ステータス": "",
    "架電結果": "",
    "督促状況": "TEL",
    "商品種別": "",
    "約束日": "",
    "入金日": "",
    "約束金額（税込）": "",
    "入金金額（税込）": "",
    "連絡": "",
    "氏名": "",
    "履歴数": "",
    "架電備考": "",
    "入居日": "",
    "法人名": "",
    "施策日": "",
    "遅延日数": 0,
    "架電結果_督促停止": "",
    "督促停止日": "",
    "備考欄": "",
    "担当者": "",
    "最終更新日": "",
    "最終更新担当者": ""
}

# 保証人用マッピングルール
GUARANTOR_MAPPING_RULES = {
    "電話番号": "TEL携帯.1",  # 保証人は.1を使用
    "架電番号": "TEL携帯.1",
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "管理番号": "管理番号",
    "契約者名（カナ）": "契約者カナ",
    "物件名": "物件名",
    "クライアント": "クライアント名",
    "残債": "滞納残債"
}

# 保証人用デフォルト値（契約者と同じ）
GUARANTOR_DEFAULT_VALUES = CONTRACT_DEFAULT_VALUES.copy()

# 緊急連絡人用マッピングルール
EMERGENCY_MAPPING_RULES = {
    "電話番号": "TEL携帯.2",  # 緊急連絡人は.2を使用
    "架電番号": "TEL携帯.2",
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "管理番号": "管理番号",
    "契約者名（カナ）": "契約者カナ",
    "物件名": "物件名",
    "クライアント": "クライアント名",
    "残債": "滞納残債"
}

# 緊急連絡人用デフォルト値（契約者と同じ）
EMERGENCY_DEFAULT_VALUES = CONTRACT_DEFAULT_VALUES.copy()

# カスタム変換関数の定義
def format_phone_number(data: Dict[str, Any]) -> str:
    """
    電話番号のフォーマット処理
    
    Args:
        data: 入力データ
        
    Returns:
        フォーマット済み電話番号
    """
    phone_fields = ["TEL携帯", "TEL携帯.1", "TEL携帯.2"]
    for field in phone_fields:
        if field in data and data[field]:
            phone = str(data[field]).strip()
            # ハイフンを除去して数字のみに
            phone = phone.replace("-", "").replace(" ", "")
            # 11桁の場合はハイフンを挿入
            if len(phone) == 11 and phone.startswith("0"):
                return f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
            return phone
    return ""

# 残債フォーマット関数
def format_debt_amount(data: Dict[str, Any]) -> str:
    """
    残債金額のフォーマット処理
    
    Args:
        data: 入力データ
        
    Returns:
        フォーマット済み金額
    """
    if "滞納残債" in data:
        try:
            amount = float(data["滞納残債"])
            return f"{amount:,.0f}"
        except (ValueError, TypeError):
            return "0"
    return "0"