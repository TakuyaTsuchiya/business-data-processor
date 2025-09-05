"""
CSV読み込み機能（インフラストラクチャ層）

CSVファイルの読み込みに関する技術的詳細を扱う。
エンコーディング自動検出機能を提供し、日本語環境での
様々なエンコーディングに対応。
"""

import pandas as pd
import io
from typing import Optional, List


class CsvReader:
    """
    CSV読み込みの技術的実装
    
    日本語環境で使用される主要なエンコーディングに対応し、
    自動的に適切なエンコーディングを検出してCSVを読み込む。
    """
    
    # 日本語環境で一般的なエンコーディング（試行順序順）
    ENCODINGS = [
        'utf-8',        # 標準的なUTF-8
        'utf-8-sig',    # BOM付きUTF-8
        'shift_jis',    # Windows日本語
        'cp932',        # Windows拡張Shift-JIS
        'euc_jp',       # Unix/Linux日本語
    ]
    
    @classmethod
    def read_with_auto_encoding(
        cls, 
        file_content: bytes, 
        dtype: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        エンコーディングを自動判定してCSVを読み込む
        
        Args:
            file_content: CSVファイルのバイトデータ
            dtype: データ型の指定（デフォルト: すべてstr型）
            
        Returns:
            pd.DataFrame: 読み込んだDataFrame
            
        Raises:
            ValueError: すべてのエンコーディングで読み込みに失敗
        """
        if dtype is None:
            dtype = str
            
        errors = []
        
        for enc in cls.ENCODINGS:
            try:
                return pd.read_csv(
                    io.BytesIO(file_content), 
                    encoding=enc, 
                    dtype=dtype
                )
            except Exception as e:
                errors.append(f"{enc}: {str(e)}")
                continue
        
        # すべて失敗した場合は詳細なエラーメッセージ
        raise ValueError(
            "CSVファイルの読み込みに失敗しました。\n"
            "試行したエンコーディング:\n" + 
            "\n".join(errors)
        )
    
    @classmethod
    def get_supported_encodings(cls) -> List[str]:
        """
        サポートしているエンコーディングのリストを返す
        
        Returns:
            List[str]: サポートされているエンコーディング
        """
        return cls.ENCODINGS.copy()