"""
CSV書き込み処理の共通化 - Infrastructure Layer

このモジュールは、app.pyの safe_csv_download 関数を抽象化し、
Streamlit固有の処理とCSV生成ロジックを分離します。

共通化対象：
- CP932エンコーディング対応
- 空列問題の解決
- エラーハンドリングの標準化
"""

import pandas as pd
from typing import Optional, Tuple
import streamlit as st


class CSVWriteError(Exception):
    """CSV書き込みエラーの専用例外クラス"""
    pass


def generate_csv_bytes(
    df: pd.DataFrame, 
    encoding: str = 'cp932',
    fallback_encoding: str = 'utf-8-sig',
    handle_empty_columns: bool = True
) -> Tuple[bytes, str, bool]:
    """
    DataFrameをCSVバイト形式で出力する
    
    Args:
        df (pd.DataFrame): 出力するDataFrame
        encoding (str): 優先エンコーディング（デフォルト: cp932）
        fallback_encoding (str): フォールバックエンコーディング（デフォルト: utf-8-sig）
        handle_empty_columns (bool): 空列名の処理を行うか（デフォルト: True）
        
    Returns:
        Tuple[bytes, str, bool]: (CSVバイト, 使用されたエンコーディング, フォールバック使用フラグ)
        
    Raises:
        CSVWriteError: CSV生成に失敗した場合
    """
    try:
        # DataFrameのコピーを作成
        df_copy = df.copy()
        original_columns = list(df.columns)
        
        # 空列名の処理
        if handle_empty_columns:
            df_copy = _handle_empty_columns(df_copy)
        
        # 優先エンコーディングで試行
        try:
            csv_data = df_copy.to_csv(
                index=False, 
                encoding=encoding, 
                errors='replace', 
                header=original_columns
            )
            csv_bytes = csv_data.encode(encoding, errors='replace')
            return csv_bytes, encoding, False
            
        except UnicodeEncodeError as e:
            # フォールバックエンコーディングで再試行
            print(f"エンコーディング '{encoding}' で失敗: {e}")
            print(f"フォールバック '{fallback_encoding}' を使用")
            
            csv_data = df_copy.to_csv(
                index=False, 
                encoding=fallback_encoding, 
                header=original_columns
            )
            csv_bytes = csv_data.encode(fallback_encoding)
            return csv_bytes, fallback_encoding, True
            
    except Exception as e:
        raise CSVWriteError(f"CSV生成中にエラーが発生しました: {str(e)}")


def _handle_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    空文字列のカラム名に一時的な名前を付ける
    
    Args:
        df (pd.DataFrame): 処理対象のDataFrame
        
    Returns:
        pd.DataFrame: 空列名が処理されたDataFrame
    """
    df_copy = df.copy()
    columns = list(df_copy.columns)
    empty_col_counter = 1
    
    for i, col in enumerate(columns):
        if col == "":
            columns[i] = f"_empty_col_{empty_col_counter}_"
            empty_col_counter += 1
    
    df_copy.columns = columns
    return df_copy


def safe_csv_download_button(
    df: pd.DataFrame, 
    filename: str, 
    label: str = "📥 CSVファイルをダウンロード",
    encoding: str = 'cp932',
    **kwargs
) -> bool:
    """
    StreamlitのダウンロードボタンでCSVを安全に出力する
    
    Args:
        df (pd.DataFrame): 出力するDataFrame
        filename (str): ダウンロードファイル名
        label (str): ボタンのラベル
        encoding (str): 優先エンコーディング（デフォルト: cp932）
        **kwargs: st.download_button に渡す追加パラメータ
        
    Returns:
        bool: ダウンロードボタンが押されたかどうか
        
    Examples:
        >>> df = pd.DataFrame({'列1': ['データ1', 'データ2']})
        >>> if safe_csv_download_button(df, "output.csv"):
        ...     st.success("ダウンロード開始")
    """
    try:
        csv_bytes, used_encoding, is_fallback = generate_csv_bytes(df, encoding=encoding)
        
        # フォールバック使用時の警告表示
        if is_fallback:
            st.warning(f"⚠️ 一部の文字が{encoding}に対応していないため、{used_encoding}で出力します")
        
        # Streamlitダウンロードボタンのデフォルトパラメータ
        button_params = {
            'label': label,
            'data': csv_bytes,
            'file_name': filename,
            'mime': "text/csv",
            'type': "primary"
        }
        button_params.update(kwargs)
        
        return st.download_button(**button_params)
        
    except CSVWriteError as e:
        st.error(f"CSVファイル生成エラー: {str(e)}")
        return False
    except Exception as e:
        st.error(f"予期しないエラーが発生しました: {str(e)}")
        return False


def save_csv_to_bytes(
    df: pd.DataFrame,
    encoding: str = 'cp932',
    **kwargs
) -> bytes:
    """
    DataFrameをCSVバイトデータとして保存（Streamlit非依存）
    
    Args:
        df (pd.DataFrame): 出力するDataFrame
        encoding (str): エンコーディング（デフォルト: cp932）
        **kwargs: pandas.to_csv() に渡すパラメータ
        
    Returns:
        bytes: CSVのバイトデータ
        
    Examples:
        >>> df = pd.DataFrame({'col1': ['data1', 'data2']})
        >>> csv_bytes = save_csv_to_bytes(df)
        >>> with open('output.csv', 'wb') as f:
        ...     f.write(csv_bytes)
    """
    csv_bytes, used_encoding, is_fallback = generate_csv_bytes(df, encoding=encoding)
    
    if is_fallback:
        print(f"警告: {encoding}からフォールバック {used_encoding} を使用しました")
    
    return csv_bytes