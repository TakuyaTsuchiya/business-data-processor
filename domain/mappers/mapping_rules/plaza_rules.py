"""
プラザシステム用マッピングルール定義

プラザオートコール処理で使用される各種マッピングルールを定義します。
契約者（メイン）、保証人、緊急連絡人のルールを管理します。
"""

from typing import Dict, Any, Callable


# 契約者（メイン）用マッピングルール
MAIN_MAPPING_RULES = {
    "電話番号": "TEL携帯",
    "架電番号": "TEL携帯",
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "管理番号": "管理番号",
    "契約者名（カナ）": "契約者カナ",
    "物件名": "物件名",
    "クライアント": "クライアント名",  # 入力データからマッピング
    "残債": "滞納残債"  # プラザは残債表示あり
}

# 契約者用デフォルト値
MAIN_DEFAULT_VALUES = {
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
GUARANTOR_DEFAULT_VALUES = MAIN_DEFAULT_VALUES.copy()

# 緊急連絡人（コンタクト）用マッピングルール
# プラザでは独自の列名を使用
CONTACT_MAPPING_RULES = {
    "電話番号": "緊急連絡人１のTEL（携帯）",  # プラザ独自の列名
    "架電番号": "緊急連絡人１のTEL（携帯）",
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "管理番号": "管理番号",
    "契約者名（カナ）": "契約者カナ",
    "物件名": "物件名",
    "クライアント": "クライアント名",
    "残債": "滞納残債"
}

# 緊急連絡人用デフォルト値（契約者と同じ）
CONTACT_DEFAULT_VALUES = MAIN_DEFAULT_VALUES.copy()

# カスタム変換関数
def format_debt_with_comma(data: Dict[str, Any]) -> str:
    """
    残債金額をカンマ区切りでフォーマット
    
    Args:
        data: 入力データ
        
    Returns:
        フォーマット済み金額
    """
    if "滞納残債" in data and data["滞納残債"]:
        try:
            amount = float(data["滞納残債"])
            return f"{amount:,.0f}"
        except (ValueError, TypeError):
            return str(data["滞納残債"])
    return ""

def validate_plaza_phone(data: Dict[str, Any], phone_field: str) -> str:
    """
    プラザ用電話番号バリデーション
    
    Args:
        data: 入力データ
        phone_field: 電話番号フィールド名
        
    Returns:
        有効な電話番号または空文字列
    """
    if phone_field in data and data[phone_field]:
        phone = str(data[phone_field]).strip()
        # ハイフンを除去して数字のみに
        phone_digits = phone.replace("-", "").replace(" ", "")
        
        # 10桁または11桁の数字のみ許可
        if phone_digits.isdigit() and len(phone_digits) in [10, 11]:
            return phone  # 元の形式を保持
        
    return ""