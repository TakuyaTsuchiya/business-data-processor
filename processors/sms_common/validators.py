"""
SMS処理バリデーションモジュール

このモジュールは、SMS処理で使用する共通バリデーション関数を提供します。
電話番号、日付、金額などの妥当性検証を行います。
"""

import re
import pandas as pd
from typing import Union, Optional, Tuple
from datetime import datetime


def validate_phone_number(phone: str, allow_empty: bool = False) -> bool:
    """
    電話番号の妥当性を検証
    
    Args:
        phone: 検証する電話番号
        allow_empty: 空文字を許可するかどうか
        
    Returns:
        妥当な電話番号の場合True
    """
    if not phone or pd.isna(phone):
        return allow_empty
    
    # 文字列に変換して空白を除去
    phone_str = str(phone).strip()
    
    if not phone_str:
        return allow_empty
    
    # 数字とハイフンのみを許可
    if not re.match(r'^[\d\-]+$', phone_str):
        return False
    
    # ハイフンを除去して数字のみにする
    digits_only = re.sub(r'-', '', phone_str)
    
    # 10桁または11桁の数字であることを確認
    if not re.match(r'^\d{10,11}$', digits_only):
        return False
    
    return True


def is_mobile_number(phone: str) -> bool:
    """
    携帯電話番号かどうかを判定
    
    Args:
        phone: 判定する電話番号
        
    Returns:
        携帯電話番号の場合True
    """
    if not validate_phone_number(phone):
        return False
    
    # ハイフンを除去
    phone_digits = re.sub(r'-', '', str(phone).strip())
    
    # 090, 080, 070で始まる11桁の番号
    return bool(re.match(r'^0[789]0\d{8}$', phone_digits))


def validate_date_format(date_str: str, format_str: str = '%Y-%m-%d') -> bool:
    """
    日付形式の妥当性を検証
    
    Args:
        date_str: 検証する日付文字列
        format_str: 期待する日付フォーマット
        
    Returns:
        妥当な日付形式の場合True
    """
    if not date_str or pd.isna(date_str):
        return False
    
    try:
        datetime.strptime(str(date_str).strip(), format_str)
        return True
    except ValueError:
        return False


def validate_amount(amount: Union[str, int, float], 
                   min_amount: Optional[float] = None,
                   max_amount: Optional[float] = None) -> Tuple[bool, Optional[float]]:
    """
    金額の妥当性を検証
    
    Args:
        amount: 検証する金額
        min_amount: 最小金額（オプション）
        max_amount: 最大金額（オプション）
        
    Returns:
        (妥当性フラグ, 数値化された金額)のタプル
    """
    if pd.isna(amount):
        return False, None
    
    try:
        # 文字列の場合、カンマを除去
        if isinstance(amount, str):
            amount_str = str(amount).replace(',', '').strip()
            if not amount_str:
                return False, None
            amount_float = float(amount_str)
        else:
            amount_float = float(amount)
        
        # 範囲チェック
        if min_amount is not None and amount_float < min_amount:
            return False, amount_float
        
        if max_amount is not None and amount_float > max_amount:
            return False, amount_float
        
        return True, amount_float
        
    except (ValueError, TypeError):
        return False, None


def validate_client_id(client_id: Union[str, int], valid_ids: list) -> bool:
    """
    委託先法人IDの妥当性を検証
    
    Args:
        client_id: 検証する委託先法人ID
        valid_ids: 有効なIDのリスト
        
    Returns:
        有効なIDの場合True
    """
    if pd.isna(client_id):
        return False
    
    try:
        # 空白の場合の特別処理
        if str(client_id).strip() == '':
            return '' in valid_ids
        
        # 数値に変換して比較
        id_int = int(float(str(client_id)))
        return id_int in valid_ids
        
    except (ValueError, TypeError):
        # 数値に変換できない場合は文字列として比較
        return str(client_id).strip() in [str(v) for v in valid_ids]


def validate_text_field(text: str, 
                       min_length: int = 0,
                       max_length: Optional[int] = None,
                       allow_empty: bool = True) -> bool:
    """
    テキストフィールドの妥当性を検証
    
    Args:
        text: 検証するテキスト
        min_length: 最小文字数
        max_length: 最大文字数（オプション）
        allow_empty: 空文字を許可するかどうか
        
    Returns:
        妥当なテキストの場合True
    """
    if pd.isna(text):
        return allow_empty
    
    text_str = str(text).strip()
    
    if not text_str:
        return allow_empty
    
    text_length = len(text_str)
    
    if text_length < min_length:
        return False
    
    if max_length is not None and text_length > max_length:
        return False
    
    return True


def validate_collection_rank(rank: str, exclude_ranks: list) -> bool:
    """
    回収ランクの妥当性を検証（除外対象でないことを確認）
    
    Args:
        rank: 検証する回収ランク
        exclude_ranks: 除外する回収ランクのリスト
        
    Returns:
        除外対象でない場合True
    """
    if pd.isna(rank):
        # NaNは除外対象ではないとみなす
        return True
    
    rank_str = str(rank).strip()
    
    # 除外ランクに含まれているかチェック
    for exclude in exclude_ranks:
        if exclude in rank_str:
            return False
    
    return True


def validate_row_completeness(row: pd.Series, required_fields: list) -> Tuple[bool, list]:
    """
    行の完全性を検証（必須フィールドが全て存在し、有効な値を持つか）
    
    Args:
        row: 検証する行
        required_fields: 必須フィールドのリスト
        
    Returns:
        (完全性フラグ, 不足フィールドのリスト)のタプル
    """
    missing_fields = []
    
    for field in required_fields:
        if field not in row:
            missing_fields.append(f"{field}(列なし)")
        elif pd.isna(row[field]) or str(row[field]).strip() == '':
            missing_fields.append(f"{field}(値なし)")
    
    return len(missing_fields) == 0, missing_fields


def clean_and_validate_dataframe(df: pd.DataFrame, 
                               validation_rules: dict) -> Tuple[pd.DataFrame, dict]:
    """
    データフレーム全体のクリーニングとバリデーション
    
    Args:
        df: 検証するデータフレーム
        validation_rules: バリデーションルール
        
    Returns:
        (検証済みデータフレーム, バリデーション結果)のタプル
    """
    validation_results = {
        'total_rows': len(df),
        'valid_rows': 0,
        'invalid_rows': 0,
        'field_errors': {}
    }
    
    # 各フィールドのバリデーション結果を格納
    for field, rules in validation_rules.items():
        if field not in df.columns:
            validation_results['field_errors'][field] = f"列が存在しません"
            continue
        
        # ルールに基づいてバリデーション実行
        # ここは具体的なルールに応じて実装
        
    return df, validation_results