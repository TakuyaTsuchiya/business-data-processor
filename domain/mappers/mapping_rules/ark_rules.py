"""
アークシステム用マッピングルール定義

アーク新規登録・残債更新処理で使用される各種マッピングルールを定義します。
111列テンプレート対応とアーク固有のビジネスルールを管理します。
"""

from typing import Dict, Any, Callable


# アーク新規登録用基本マッピングルール
REGISTRATION_MAPPING_RULES = {
    # 基本契約情報
    "引継番号": "契約番号",
    "契約者氏名": "契約者氏名",
    "契約者カナ": "契約者カナ",
    "契約者生年月日": "契約者生年月日",
    "契約者TEL自宅": "契約者TEL自宅",
    "契約者TEL携帯": "契約者TEL携帯",
    "契約者現住所郵便番号": "契約者現住所郵便番号",
    "契約者現住所1": "契約者現住所1", 
    "契約者現住所2": "契約者現住所2",
    "契約者現住所3": "契約者現住所3",
    
    # 物件情報
    "物件名": "物件名",
    "部屋番号": "部屋番号",
    "物件住所郵便番号": "物件住所郵便番号",
    "物件住所1": "物件住所1",
    "物件住所2": "物件住所2", 
    "物件住所3": "物件住所3",
    
    # ステータス情報
    "入居ステータス": "入居ステータス",
    "滞納ステータス": "滞納ステータス",
    "受託状況": "受託状況",
    
    # 金額情報
    "月額賃料": "月額賃料",
    "管理費": "管理費",
    "共益費": "共益費",
    "水道代": "水道代",
    "駐車場代": "駐車場代",
    "その他費用1": "その他費用1",
    "その他費用2": "その他費用2",
    "敷金": "敷金",
    "礼金": "礼金",
    
    # 管理会社情報（アーク元データのH列「取引先」をマッピング）
    "管理会社": "取引先",
}

# アーク新規登録用デフォルト値
REGISTRATION_DEFAULT_VALUES = {
    # 引継情報
    "引継情報": "",
    
    # 日付情報
    "管理受託日": "",
    "契約確認日": "",
    "保証開始日": "",
    "初回振替月": "",
    "解約日": "",
    
    # 手数料情報
    "退去済手数料": "",
    "入居中滞納手数料": "",
    "入居中正常手数料": "",
    "管理前滞納額": "0",
    "退去手続き（実費）": "",
    
    # 口座情報
    "回収口座金融機関CD": "",
    "回収口座金融機関名": "",
    "回収口座支店CD": "",
    "回収口座支店名": "",
    "回収口座種類": "",
    "回収口座番号": "",
    "回収口座名義": "",
    
    # 契約種類
    "契約種類": "",
    
    # コード情報
    "クライアントCD": "",
    "パートナーCD": "",
    "委託先法人ID": "5",  # ARK固定値
    
    # 勤務先情報
    "契約者勤務先名": "",
    "契約者勤務先カナ": "",
    "契約者勤務先TEL": "",
    "勤務先業種": "",
    "契約者勤務先郵便番号": "",
    "契約者勤務先住所1": "",
    "契約者勤務先住所2": "",
    "契約者勤務先住所3": "",
    
    # 引落口座情報
    "保証入金日": "",
    "保証入金者": "",
    "引落銀行CD": "",
    "引落銀行名": "",
    "引落支店CD": "",
    "引落支店名": "",
    "引落預金種別": "",
    "引落口座番号": "",
    "引落口座名義": "",
    
    # 登録フラグ（111列目）
    "登録フラグ": "1",
    
    # 空列（108-110番目は空文字）
    "": ""  # 空列用デフォルト
}

# 残債更新用マッピングルール
LATE_PAYMENT_MAPPING_RULES = {
    "管理番号": "管理番号",
    "引継番号": "引継番号", 
    "滞納残債": "滞納残債",
    "クライアントCD": "クライアントCD",
    "管理前滞納額": "管理前滞納額"
}

# 残債更新用デフォルト値
LATE_PAYMENT_DEFAULT_VALUES = {
    "更新日": "",
    "更新担当者": ""
}

# 地域別設定
REGION_SETTINGS = {
    1: {"name": "東京", "fee": "1"},      # 東京
    2: {"name": "大阪", "fee": "2"},      # 大阪  
    3: {"name": "北海道", "fee": "3"},    # 北海道
    4: {"name": "北関東", "fee": "4"}     # 北関東
}

# カスタム変換関数
def generate_ark_management_company(data: Dict[str, Any]) -> str:
    """
    アーク元データのH列「取引先」から管理会社を設定
    
    Args:
        data: 入力データ
        
    Returns:
        管理会社名
    """
    return str(data.get("取引先", "")).strip()

def set_ark_client_id(data: Dict[str, Any]) -> str:
    """
    委託先法人IDの固定値設定（ARK固有）
    
    Returns:
        固定値 "5"
    """
    return "5"

def generate_region_fee(data: Dict[str, Any], region_code: int) -> str:
    """
    地域コードから更新契約手数料を生成
    
    Args:
        data: 入力データ
        region_code: 地域コード (1-4)
        
    Returns:
        地域コード文字列
    """
    return str(region_code) if region_code in REGION_SETTINGS else ""

def format_ark_phone_number(data: Dict[str, Any], phone_field: str) -> str:
    """
    アーク用電話番号フォーマット
    
    Args:
        data: 入力データ
        phone_field: 電話番号フィールド名
        
    Returns:
        フォーマット済み電話番号
    """
    if phone_field in data and data[phone_field]:
        phone = str(data[phone_field]).strip()
        # 不要文字を除去（ハイフン、スペース等）
        phone = phone.replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
        return phone
    return ""

def process_ark_amount(data: Dict[str, Any], amount_field: str) -> str:
    """
    アーク用金額フォーマット（小数点以下切り捨て）
    
    Args:
        data: 入力データ
        amount_field: 金額フィールド名
        
    Returns:
        フォーマット済み金額
    """
    if amount_field in data and data[amount_field]:
        try:
            amount = float(data[amount_field])
            return str(int(amount))  # 小数点以下切り捨て
        except (ValueError, TypeError):
            return str(data[amount_field])
    return "0"