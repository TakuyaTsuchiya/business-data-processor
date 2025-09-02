"""
ミライル契約者用フィルタリング関数群

ビジネスルールに基づいたフィルタリング関数を個別に定義。
各関数は純粋関数として実装し、テスト可能な構造にする。
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional


def filter_client_id(row: Dict[str, Any]) -> bool:
    """
    委託先法人IDフィルタ
    
    ビジネスルール: 空白と5のみを処理対象とする
    
    Args:
        row: データ行の辞書
        
    Returns:
        bool: True=処理対象, False=除外
    """
    client_id = row.get('委託先法人ID', '')
    
    # 空白、NaN、文字列の空白、"5"のみを処理対象
    if pd.isna(client_id):
        return True
    
    client_id_str = str(client_id).strip()
    return client_id_str == '' or client_id_str == '5'


def filter_payment_date(row: Dict[str, Any], reference_date: Optional[datetime] = None) -> bool:
    """
    入金予定日フィルタ
    
    ビジネスルール: 前日以前またはNaNを処理対象（当日は除外）
    
    Args:
        row: データ行の辞書
        reference_date: 基準日（デフォルト: 実行日）
        
    Returns:
        bool: True=処理対象, False=除外
    """
    payment_date = row.get('入金予定日')
    
    # NaNは処理対象
    if pd.isna(payment_date):
        return True
    
    # 基準日のデフォルトは実行日
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()
    else:
        reference_date = pd.Timestamp(reference_date).normalize()
    
    # 文字列の場合は日付に変換
    if isinstance(payment_date, str):
        try:
            payment_date = pd.to_datetime(payment_date)
        except:
            return True  # 変換できない場合は処理対象
    
    # 前日以前のみ処理対象
    return payment_date < reference_date


def filter_collection_rank(row: Dict[str, Any]) -> bool:
    """
    回収ランクフィルタ
    
    ビジネスルール: 「弁護士介入」を除外
    
    Args:
        row: データ行の辞書
        
    Returns:
        bool: True=処理対象, False=除外
    """
    rank = row.get('回収ランク', '')
    return rank != '弁護士介入'


def filter_mirail_special_debt(row: Dict[str, Any]) -> bool:
    """
    ミライル特殊残債フィルタ
    
    ビジネスルール: クライアントCD=1,4 かつ 滞納残債=10,000円・11,000円を除外
    その他は全て処理対象
    
    Args:
        row: データ行の辞書
        
    Returns:
        bool: True=処理対象, False=除外
    """
    client_cd = row.get('クライアントCD')
    debt_amount = row.get('滞納残債')
    
    # 数値に変換
    try:
        client_cd = int(client_cd) if not pd.isna(client_cd) else None
    except:
        client_cd = None
    
    try:
        # カンマを除去して数値に変換
        if isinstance(debt_amount, str):
            debt_amount = float(debt_amount.replace(',', ''))
        else:
            debt_amount = float(debt_amount) if not pd.isna(debt_amount) else None
    except:
        debt_amount = None
    
    # CD=1,4 かつ 残債=10,000,11,000の場合のみ除外
    if client_cd in [1, 4] and debt_amount in [10000, 11000]:
        return False
    
    return True


def filter_mobile_phone(row: Dict[str, Any]) -> bool:
    """
    携帯電話番号フィルタ
    
    ビジネスルール: TEL携帯が空でない値のみ処理対象
    
    Args:
        row: データ行の辞書
        
    Returns:
        bool: True=処理対象, False=除外
    """
    mobile = row.get('TEL携帯', '')
    
    # NaNチェック
    if pd.isna(mobile):
        return False
    
    # 文字列として空白チェック
    mobile_str = str(mobile).strip()
    return mobile_str not in ['', 'nan', 'NaN']


def filter_exclude_amounts(row: Dict[str, Any]) -> bool:
    """
    除外金額フィルタ
    
    ビジネスルール: 入金予定金額が2,3,5,12円を除外
    
    Args:
        row: データ行の辞書
        
    Returns:
        bool: True=処理対象, False=除外
    """
    amount = row.get('入金予定金額')
    
    # NaNは処理対象
    if pd.isna(amount):
        return True
    
    # 数値に変換
    try:
        amount = float(amount)
    except:
        return True  # 変換できない場合は処理対象
    
    # 2,3,5,12円は除外
    return amount not in [2, 3, 5, 12]


# フィルタ関数のリスト（適用順序）
MIRAIL_CONTRACT_WITHOUT10K_FILTERS = [
    ('委託先法人ID', filter_client_id),
    ('入金予定日', filter_payment_date),
    ('回収ランク', filter_collection_rank),
    ('ミライル特殊残債', filter_mirail_special_debt),
    ('携帯電話', filter_mobile_phone),
    ('除外金額', filter_exclude_amounts),
]