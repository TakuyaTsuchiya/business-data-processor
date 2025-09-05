"""
ファイル出力処理の共通化モジュール

CSV/Excel出力の共通処理を提供。
エンコーディング指定、絵文字対策なども含む。
"""

import pandas as pd
import io
import re
from typing import Optional, Union, Dict, Any


class FileWriter:
    """
    ファイル出力処理を統一するライタークラス
    
    CSV、Excelなど様々な形式での出力をサポート
    """
    
    # 絵文字パターン（CP932でエンコードできない文字の除去用）
    EMOJI_PATTERN = re.compile(
        "["
        u"\U00010000-\U0010FFFF"  # 絵文字
        u"\U0001F600-\U0001F64F"  # 顔文字
        u"\U0001F300-\U0001F5FF"  # シンボル
        u"\U0001F680-\U0001F6FF"  # 乗り物
        u"\U0001F1E0-\U0001F1FF"  # 国旗
        "]+", 
        flags=re.UNICODE
    )
    
    def __init__(self):
        """初期化"""
        pass
    
    def to_csv_bytes(
        self, 
        df: pd.DataFrame,
        encoding: str = 'utf-8',
        index: bool = False,
        **kwargs
    ) -> bytes:
        """
        DataFrameをCSVバイトデータに変換
        
        Args:
            df: 出力するDataFrame
            encoding: 出力エンコーディング
            index: インデックスを含めるか
            **kwargs: DataFrame.to_csvに渡す追加引数
            
        Returns:
            CSVのバイトデータ
        """
        csv_string = df.to_csv(index=index, **kwargs)
        return csv_string.encode(encoding)
    
    def to_csv_cp932_safe(
        self,
        df: pd.DataFrame,
        index: bool = False,
        remove_emoji: bool = True,
        **kwargs
    ) -> bytes:
        """
        CP932で安全にCSV出力（絵文字エラー対策）
        
        Args:
            df: 出力するDataFrame
            index: インデックスを含めるか
            remove_emoji: 絵文字を除去するか
            **kwargs: DataFrame.to_csvに渡す追加引数
            
        Returns:
            CP932エンコードされたCSVバイトデータ
        """
        if remove_emoji:
            # 絵文字を除去したDataFrameのコピーを作成
            df_safe = df.copy()
            for col in df_safe.select_dtypes(include=['object']).columns:
                df_safe[col] = df_safe[col].apply(
                    lambda x: self.remove_emojis(str(x)) if pd.notna(x) else x
                )
            df = df_safe
        
        try:
            return self.to_csv_bytes(df, encoding='cp932', index=index, **kwargs)
        except UnicodeEncodeError as e:
            # エラーの詳細情報を含めて再発生
            raise ValueError(
                f"CP932でエンコードできない文字が含まれています: {str(e)}\n"
                "remove_emoji=Trueを指定しても解決しない場合は、"
                "データに特殊文字が含まれている可能性があります。"
            )
    
    def to_excel_bytes(
        self,
        df: pd.DataFrame,
        sheet_name: str = 'Sheet1',
        index: bool = False,
        **kwargs
    ) -> bytes:
        """
        DataFrameをExcelバイトデータに変換
        
        Args:
            df: 出力するDataFrame
            sheet_name: シート名
            index: インデックスを含めるか
            **kwargs: DataFrame.to_excelに渡す追加引数
            
        Returns:
            Excelのバイトデータ
        """
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index, **kwargs)
        buffer.seek(0)
        return buffer.getvalue()
    
    def to_excel_multi_sheets(
        self,
        sheets: Dict[str, pd.DataFrame],
        index: bool = False,
        **kwargs
    ) -> bytes:
        """
        複数のDataFrameを複数シートのExcelファイルとして出力
        
        Args:
            sheets: {シート名: DataFrame}の辞書
            index: インデックスを含めるか
            **kwargs: DataFrame.to_excelに渡す追加引数
            
        Returns:
            Excelのバイトデータ
        """
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=index, **kwargs)
        buffer.seek(0)
        return buffer.getvalue()
    
    def remove_emojis(self, text: str) -> str:
        """
        テキストから絵文字を除去
        
        Args:
            text: 処理するテキスト
            
        Returns:
            絵文字が除去されたテキスト
        """
        return self.EMOJI_PATTERN.sub('', text)
    
    def create_download_response(
        self,
        df: pd.DataFrame,
        filename: str,
        format: str = 'csv',
        encoding: str = 'utf-8',
        **kwargs
    ) -> Dict[str, Any]:
        """
        ダウンロード用のレスポンスデータを作成
        
        Args:
            df: 出力するDataFrame
            filename: ファイル名（拡張子なし）
            format: 出力形式 ('csv' or 'excel')
            encoding: CSVの場合のエンコーディング
            **kwargs: 各出力メソッドに渡す追加引数
            
        Returns:
            {
                'data': バイトデータ,
                'filename': 完全なファイル名,
                'mime_type': MIMEタイプ
            }
        """
        if format.lower() == 'csv':
            if encoding.lower() == 'cp932':
                data = self.to_csv_cp932_safe(df, **kwargs)
            else:
                data = self.to_csv_bytes(df, encoding=encoding, **kwargs)
            full_filename = f"{filename}.csv"
            mime_type = 'text/csv'
        elif format.lower() == 'excel':
            data = self.to_excel_bytes(df, **kwargs)
            full_filename = f"{filename}.xlsx"
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else:
            raise ValueError(f"サポートされていない形式: {format}")
        
        return {
            'data': data,
            'filename': full_filename,
            'mime_type': mime_type
        }


# シングルトンインスタンス
default_writer = FileWriter()


# 便利な関数
def to_csv_bytes(df: pd.DataFrame, encoding: str = 'utf-8', **kwargs) -> bytes:
    """デフォルトライターを使ってCSVバイトデータを生成"""
    return default_writer.to_csv_bytes(df, encoding=encoding, **kwargs)


def to_csv_cp932_safe(df: pd.DataFrame, **kwargs) -> bytes:
    """デフォルトライターを使ってCP932安全なCSVバイトデータを生成"""
    return default_writer.to_csv_cp932_safe(df, **kwargs)


def to_excel_bytes(df: pd.DataFrame, **kwargs) -> bytes:
    """デフォルトライターを使ってExcelバイトデータを生成"""
    return default_writer.to_excel_bytes(df, **kwargs)