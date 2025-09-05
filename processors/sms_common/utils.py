"""
SMS共通ユーティリティ関数

SMSプロセッサで使用する共通の関数を定義。
重複コードを排除し、保守性を向上。
"""

import pandas as pd
import io
from datetime import date
from typing import List
from infrastructure import read_csv_auto_encoding


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


# read_csv_auto_encoding関数は infrastructure から再エクスポート
# 互換性のために関数名は残すが、実装はインフラ層を使用