"""
CSV読込処理の共通化 - Infrastructure Layer

このモジュールは、複数のプロセッサーで重複していたCSV読込処理を統合し、
エンコーディング自動判定機能を提供します。

統合対象：
- 15のプロセッサーファイルで重複していた read_csv_auto_encoding() 関数
- エンコーディング判定ロジック
- エラーハンドリングの標準化
"""

import io
import pandas as pd
from typing import List, Optional
import chardet


class CSVReadError(Exception):
    """CSV読込エラーの専用例外クラス"""
    pass


def read_csv_auto_encoding(
    file_content: bytes, 
    encodings: Optional[List[str]] = None,
    **kwargs
) -> pd.DataFrame:
    """
    アップロードされたCSVファイルを自動エンコーディング判定で読み込み
    
    Args:
        file_content (bytes): CSVファイルの内容（バイト形式）
        encodings (List[str], optional): 試行するエンコーディングのリスト
                                       未指定の場合はデフォルトのエンコーディング順序を使用
        **kwargs: pandas.read_csv() に渡す追加パラメータ
                 （例: dtype=str, sep=',', etc.）
    
    Returns:
        pd.DataFrame: 読み込んだCSVデータ
        
    Raises:
        CSVReadError: 全てのエンコーディングで読み込みに失敗した場合
        
    Examples:
        >>> file_data = uploaded_file.read()
        >>> df = read_csv_auto_encoding(file_data, dtype=str)
        >>> print(f"読み込み完了: {len(df)}行")
    """
    # デフォルトエンコーディング順序（CLAUDE.mdの指示に従い優先順位設定）
    if encodings is None:
        encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8', 'iso-2022-jp', 'euc-jp']
    
    # デフォルトパラメータを設定（既存の挙動を維持）
    csv_params = {'dtype': str}
    csv_params.update(kwargs)
    
    last_error = None
    
    for encoding in encodings:
        try:
            # バイト内容をPandas DataFrameとして読み込み
            df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, **csv_params)
            
            # 読み込み成功時のデバッグ情報
            print(f"CSV読み込み成功: エンコーディング={encoding}, 行数={len(df)}, 列数={len(df.columns)}")
            
            return df
            
        except Exception as e:
            last_error = e
            print(f"エンコーディング '{encoding}' での読み込み失敗: {str(e)[:100]}")
            continue
    
    # 全てのエンコーディングで失敗した場合
    raise CSVReadError(f"CSVファイルの読み込みに失敗しました。試行したエンコーディング: {encodings}. 最後のエラー: {last_error}")


def detect_encoding(file_content: bytes) -> str:
    """
    ファイル内容のエンコーディングを自動検出
    
    Args:
        file_content (bytes): ファイル内容
        
    Returns:
        str: 検出されたエンコーディング名
        
    Examples:
        >>> file_data = uploaded_file.read()
        >>> encoding = detect_encoding(file_data)
        >>> print(f"検出されたエンコーディング: {encoding}")
    """
    result = chardet.detect(file_content)
    detected_encoding = result.get('encoding', 'utf-8')
    confidence = result.get('confidence', 0.0)
    
    print(f"エンコーディング検出: {detected_encoding} (信頼度: {confidence:.2f})")
    
    return detected_encoding


def read_csv_with_validation(
    file_content: bytes,
    required_columns: Optional[List[str]] = None,
    min_rows: int = 1,
    **kwargs
) -> pd.DataFrame:
    """
    CSV読み込み + 基本的な検証を行う
    
    Args:
        file_content (bytes): CSVファイルの内容
        required_columns (List[str], optional): 必須カラムのリスト
        min_rows (int): 最小行数（デフォルト: 1）
        **kwargs: read_csv_auto_encoding() に渡すパラメータ
        
    Returns:
        pd.DataFrame: 検証済みのCSVデータ
        
    Raises:
        CSVReadError: 読み込み失敗または検証失敗の場合
    """
    # CSV読み込み
    df = read_csv_auto_encoding(file_content, **kwargs)
    
    # 基本検証
    if len(df) < min_rows:
        raise CSVReadError(f"データ行数が不足しています。最小{min_rows}行必要ですが、{len(df)}行でした。")
    
    # 必須カラム検証
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise CSVReadError(f"必須カラムが不足しています: {missing_columns}")
    
    return df