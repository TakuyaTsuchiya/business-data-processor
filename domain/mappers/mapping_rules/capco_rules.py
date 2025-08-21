"""
カプコシステム用マッピングルール定義

カプコ新規登録・残債更新処理で使用される各種マッピングルールを定義します。
111列テンプレート対応とカプコ固有の電話番号クリーニング機能を管理します。
"""

from typing import Dict, Any, Callable
import re


# カプコ新規登録用基本マッピングルール
REGISTRATION_MAPPING_RULES = {
    # 基本契約情報
    "引継番号": "契約番号",
    "契約者氏名": "契約者氏名",
    "契約者カナ": "契約者カナ",
    "契約者生年月日": "契約者生年月日",
    "契約者TEL自宅": "契約者TEL自宅",
    "契約者TEL携帯": "契約者：電話番号",  # L列から電話番号をクリーニング
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
    
    # 管理会社情報
    "管理会社": "管理会社名",
}

# カプコ新規登録用デフォルト値
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
    "委託先法人ID": "7",  # CAPCO固定値
    
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
DEBT_UPDATE_MAPPING_RULES = {
    "契約番号": "契約番号",
    "現在残債": "現在残債",
    "更新後残債": "更新後残債",
    "差分金額": "差分金額",
    "更新理由": "更新理由"
}

# 残債更新用デフォルト値
DEBT_UPDATE_DEFAULT_VALUES = {
    "更新日": "",
    "更新担当者": "",
    "処理ステータス": "完了"
}

# カプコ電話番号クリーニングパターン
PHONE_CLEANING_PATTERNS = [
    # 漢字混入パターン
    r'[未娘父母本人代理自宅]',
    # 記号混入パターン
    r'[×○※]',
    # アルファベット混入パターン
    r'[TEL携帯tel]',
    # その他の記号
    r'[-\s\(\)]'
]

# カスタム変換関数
def extract_clean_phone_number(phone_input: str) -> str:
    """
    混入文字を除去して電話番号のみを抽出
    
    カプコ入力データの「契約者：電話番号」列（L列）から
    不要な文字を除去し、適切な電話番号形式に変換します。
    
    Examples:
        '未080-5787-5364' -> '080-5787-5364'
        '×042-361-5460' -> '042-361-5460'
        '娘080-6868-0817' -> '080-6868-0817'
        'TEL03-1234-5678' -> '03-1234-5678'
        '08012345678' -> '080-1234-5678'  # ハイフン自動補完
    
    Args:
        phone_input: 入力電話番号文字列
        
    Returns:
        クリーニング済み電話番号
    """
    if not phone_input or phone_input.strip() == "":
        return ""
    
    phone = str(phone_input).strip()
    
    # 1. 漢字・記号の除去
    for pattern in PHONE_CLEANING_PATTERNS[:-1]:  # ハイフン以外を除去
        phone = re.sub(pattern, '', phone)
    
    # 2. 数字のみを抽出
    digits_only = re.sub(r'[^\d]', '', phone)
    
    if not digits_only:
        return ""
    
    # 3. 桁数による自動フォーマット
    if len(digits_only) == 11:
        # 携帯電話（11桁）: 090-1234-5678
        if digits_only.startswith(('090', '080', '070')):
            return f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:]}"
        # IP電話等（11桁）: 050-1234-5678
        elif digits_only.startswith('050'):
            return f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:]}"
        else:
            return digits_only
    
    elif len(digits_only) == 10:
        # 固定電話（10桁）
        if digits_only.startswith('0'):
            # 03-1234-5678 (東京)
            if digits_only.startswith('03'):
                return f"{digits_only[:2]}-{digits_only[2:6]}-{digits_only[6:]}"
            # 06-1234-5678 (大阪)
            elif digits_only.startswith('06'):
                return f"{digits_only[:2]}-{digits_only[2:6]}-{digits_only[6:]}"
            # その他の市外局番（3桁）
            else:
                return f"{digits_only[:3]}-{digits_only[3:6]}-{digits_only[6:]}"
        else:
            return digits_only
    
    elif len(digits_only) in [12, 13]:
        # 国際番号等の長い番号はそのまま返す
        return digits_only
    
    else:
        # その他の桁数は元の数字をそのまま返す
        return digits_only

def clean_capco_phone_field(data: Dict[str, Any], field_name: str) -> str:
    """
    カプコデータの電話番号フィールドをクリーニング
    
    Args:
        data: 入力データ
        field_name: 電話番号フィールド名
        
    Returns:
        クリーニング済み電話番号
    """
    if field_name in data and data[field_name]:
        return extract_clean_phone_number(data[field_name])
    return ""

def set_capco_client_id(data: Dict[str, Any]) -> str:
    """
    委託先法人IDの固定値設定（CAPCO固有）
    
    Returns:
        固定値 "7"
    """
    return "7"

def process_capco_amount(data: Dict[str, Any], amount_field: str) -> str:
    """
    カプコ用金額フォーマット（小数点以下切り捨て）
    
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

def calculate_debt_difference(data: Dict[str, Any]) -> str:
    """
    残債差分の計算
    
    Args:
        data: 入力データ（現在残債、更新後残債を含む）
        
    Returns:
        差分金額
    """
    try:
        current = float(data.get("現在残債", 0))
        updated = float(data.get("更新後残債", 0))
        return str(updated - current)
    except (ValueError, TypeError):
        return "0"