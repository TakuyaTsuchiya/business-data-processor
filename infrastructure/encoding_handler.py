"""
エンコーディング処理の共通化モジュール

CSVファイルの読み込み時のエンコーディング自動検出を統一。
特殊なケース（ARK、CAPCO）を除く全プロセッサーで使用可能。
"""

import pandas as pd
import io
from typing import Union, List, Optional


class EncodingHandler:
    """
    エンコーディング処理を統一するハンドラークラス
    
    統一パターン: utf-8-sig → utf-8 → cp932 → shift_jis → euc_jp
    この順序で試行し、最初に成功したエンコーディングで読み込む
    """
    
    # 統一エンコーディングパターン
    DEFAULT_ENCODINGS = ['utf-8-sig', 'utf-8', 'cp932', 'shift_jis', 'euc_jp']
    
    def __init__(self, custom_encodings: Optional[List[str]] = None):
        """
        初期化
        
        Args:
            custom_encodings: カスタムエンコーディングリスト（デフォルトを上書き）
        """
        self.encodings = custom_encodings or self.DEFAULT_ENCODINGS
    
    def read_csv_auto_encoding(
        self, 
        content: bytes,
        **pandas_kwargs
    ) -> pd.DataFrame:
        """
        エンコーディングを自動検出してCSVを読み込む
        
        Args:
            content: CSVファイルのバイトデータ
            **pandas_kwargs: pd.read_csvに渡す追加引数
            
        Returns:
            読み込まれたDataFrame
            
        Raises:
            ValueError: 全てのエンコーディングで読み込みに失敗した場合
        """
        errors = []
        
        for encoding in self.encodings:
            try:
                df = pd.read_csv(
                    io.BytesIO(content), 
                    encoding=encoding,
                    **pandas_kwargs
                )
                return df
            except UnicodeDecodeError as e:
                errors.append(f"{encoding}: {str(e)}")
                continue
            except Exception as e:
                # UnicodeDecodeError以外のエラーは即座に再発生
                raise
        
        # 全て失敗した場合
        error_details = '\n'.join(errors)
        raise ValueError(
            f"サポートされているエンコーディングで読み込めませんでした。\n"
            f"試行したエンコーディング: {', '.join(self.encodings)}\n"
            f"エラー詳細:\n{error_details}"
        )
    
    def detect_encoding(self, content: bytes) -> Optional[str]:
        """
        ファイルのエンコーディングを検出（読み込みはしない）
        
        Args:
            content: 検査するバイトデータ
            
        Returns:
            検出されたエンコーディング名、検出できない場合はNone
        """
        for encoding in self.encodings:
            try:
                content.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        return None
    
    def get_encoding_info(self) -> dict:
        """
        現在の設定情報を取得
        
        Returns:
            エンコーディング設定情報の辞書
        """
        return {
            'encodings': self.encodings,
            'pattern': ' → '.join(self.encodings)
        }


# シングルトンインスタンス（デフォルト設定）
default_handler = EncodingHandler()


# 便利な関数（デフォルトハンドラーを使用）
def read_csv_auto_encoding(content: bytes, **pandas_kwargs) -> pd.DataFrame:
    """
    デフォルトのエンコーディングハンドラーを使ってCSVを読み込む
    
    Args:
        content: CSVファイルのバイトデータ
        **pandas_kwargs: pd.read_csvに渡す追加引数
        
    Returns:
        読み込まれたDataFrame
    """
    return default_handler.read_csv_auto_encoding(content, **pandas_kwargs)