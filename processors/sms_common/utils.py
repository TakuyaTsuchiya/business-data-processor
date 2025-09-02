"""
SMS共通ユーティリティ関数

SMSプロセッサで使用する共通の関数を定義。
重複コードを排除し、保守性を向上。
"""

import pandas as pd
import io
from datetime import date
from typing import List


def format_payment_deadline(date_input: date) -> str:
    """
    日付オブジェクトを日本語形式に変換
    
    Args:
        date_input: datetimeのdateオブジェクト
        
    Returns:
        str: 'YYYY年MM月DD日' 形式の文字列
        
    Examples:
        format_payment_deadline(date(2025, 6, 30)) -> '2025年6月30日'
        format_payment_deadline(date(2025, 12, 1)) -> '2025年12月1日'
    """
    return date_input.strftime("%Y年%m月%d日")


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """
    アップロードされたCSVファイルを自動エンコーディング判定で読み込み
    
    Args:
        file_content: CSVファイルのバイトデータ
        
    Returns:
        pd.DataFrame: 読み込んだDataFrame（すべての列をstr型として）
        
    Raises:
        ValueError: すべてのエンコーディングで読み込みに失敗した場合
    """
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'euc_jp']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")