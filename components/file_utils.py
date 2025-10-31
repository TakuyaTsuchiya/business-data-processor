"""
ファイル処理ユーティリティ
Business Data Processor

ファイル読み込みなどの共通処理を提供
"""

import pandas as pd
import io


def read_csv_with_encoding(file_data: bytes, **kwargs) -> pd.DataFrame:
    """
    エンコーディングを自動判定してCSVを読み込む

    Args:
        file_data: CSVファイルのバイトデータ
        **kwargs: pd.read_csvに渡す追加引数（low_memoryなど）

    Returns:
        pd.DataFrame: 読み込んだDataFrame

    Raises:
        ValueError: サポートされていないエンコーディングの場合
    """
    encodings = ['cp932', 'shift_jis', 'utf-8-sig']

    for encoding in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_data), encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            continue

    raise ValueError("サポートされていないエンコーディングです。対応形式: CP932, Shift_JIS, UTF-8")
