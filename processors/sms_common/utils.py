"""
SMS処理ユーティリティモジュール

このモジュールは、SMS処理で使用する共通ユーティリティ関数を提供します。
電話番号フォーマット、テキストクリーニング、ファイル名生成などの機能を含みます。
"""

import re
import pandas as pd
import chardet
from typing import Optional, Union, Dict, List, Tuple
from datetime import datetime
import unicodedata


def format_phone_number(phone: str, add_hyphen: bool = True) -> str:
    """
    電話番号をフォーマット
    
    Args:
        phone: フォーマットする電話番号
        add_hyphen: ハイフンを追加するかどうか
        
    Returns:
        フォーマット済み電話番号
    """
    if not phone or pd.isna(phone):
        return ''
    
    # 文字列に変換して数字以外を除去
    phone_str = re.sub(r'[^\d]', '', str(phone))
    
    if not phone_str:
        return ''
    
    # 既にフォーマット済みの場合はそのまま返す
    if add_hyphen:
        # 携帯電話（11桁）
        if len(phone_str) == 11 and phone_str.startswith(('090', '080', '070')):
            return f"{phone_str[:3]}-{phone_str[3:7]}-{phone_str[7:]}"
        # 固定電話（10桁）
        elif len(phone_str) == 10:
            # 市外局番が3桁の場合（東京03、大阪06など）
            if phone_str.startswith(('03', '06')):
                return f"{phone_str[:2]}-{phone_str[2:6]}-{phone_str[6:]}"
            # 市外局番が4桁の場合
            else:
                return f"{phone_str[:4]}-{phone_str[4:6]}-{phone_str[6:]}"
    
    return phone_str


def clean_text(text: str, remove_spaces: bool = False) -> str:
    """
    テキストをクリーニング
    
    Args:
        text: クリーニングするテキスト
        remove_spaces: 空白を除去するかどうか
        
    Returns:
        クリーニング済みテキスト
    """
    if not text or pd.isna(text):
        return ''
    
    text_str = str(text).strip()
    
    # 制御文字を除去
    text_str = ''.join(char for char in text_str if not unicodedata.category(char).startswith('C'))
    
    # 連続する空白を単一の空白に置換
    text_str = re.sub(r'\s+', ' ', text_str)
    
    if remove_spaces:
        text_str = text_str.replace(' ', '')
    
    return text_str.strip()


def generate_output_filename(base_name: str, 
                           suffix: Optional[str] = None,
                           date_format: str = '%m%d',
                           extension: str = 'csv') -> str:
    """
    出力ファイル名を生成
    
    Args:
        base_name: ベースとなるファイル名
        suffix: 追加するサフィックス
        date_format: 日付フォーマット
        extension: ファイル拡張子
        
    Returns:
        生成されたファイル名
    """
    date_str = datetime.now().strftime(date_format)
    
    parts = [date_str, base_name]
    if suffix:
        parts.append(suffix)
    
    filename = '_'.join(parts)
    
    return f"{filename}.{extension}"


def get_encoding(file_path: str) -> str:
    """
    ファイルのエンコーディングを自動判定
    
    Args:
        file_path: ファイルパス
        
    Returns:
        判定されたエンコーディング
    """
    encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8', 'iso-2022-jp', 'euc-jp']
    
    # chardetによる自動判定
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # 最初の10KBを読み込み
            result = chardet.detect(raw_data)
            if result['encoding'] and result['confidence'] > 0.7:
                detected = result['encoding'].lower()
                # shift_jisとcp932の判定を統一
                if 'shift' in detected or detected == 'cp932':
                    return 'cp932'
                return result['encoding']
    except Exception:
        pass
    
    # 順番に試す
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1000)  # 最初の1000文字を読み込んでテスト
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # デフォルト
    return 'cp932'


def extract_phone_from_mixed_field(field: str) -> str:
    """
    混在フィールドから電話番号を抽出
    
    例: "未080-5787-5364" -> "080-5787-5364"
    
    Args:
        field: 電話番号が含まれるフィールド
        
    Returns:
        抽出された電話番号
    """
    if not field or pd.isna(field):
        return ''
    
    field_str = str(field).strip()
    
    # 電話番号パターンを検索
    # 携帯電話パターン
    mobile_match = re.search(r'0[789]0[-\d]{8,12}', field_str)
    if mobile_match:
        return format_phone_number(mobile_match.group())
    
    # 固定電話パターン
    fixed_match = re.search(r'0\d{1,4}[-\d]{6,12}', field_str)
    if fixed_match:
        return format_phone_number(fixed_match.group())
    
    # パターンにマッチしない場合は数字のみ抽出
    digits = re.sub(r'[^\d]', '', field_str)
    if len(digits) >= 10:
        return format_phone_number(digits)
    
    return ''


def split_dataframe_by_condition(df: pd.DataFrame, 
                                condition_column: str,
                                condition_func) -> Dict[str, pd.DataFrame]:
    """
    条件に基づいてデータフレームを分割
    
    Args:
        df: 分割するデータフレーム
        condition_column: 条件判定に使用する列名
        condition_func: 条件判定関数
        
    Returns:
        分割されたデータフレームの辞書
    """
    if condition_column not in df.columns:
        raise ValueError(f"列 '{condition_column}' が存在しません")
    
    # 条件を適用
    mask = df[condition_column].apply(condition_func)
    
    return {
        'matched': df[mask].copy(),
        'not_matched': df[~mask].copy()
    }


def merge_duplicate_columns(df: pd.DataFrame, 
                          primary_col: str,
                          secondary_cols: List[str]) -> pd.DataFrame:
    """
    重複する列をマージ（プライマリ列を優先）
    
    Args:
        df: データフレーム
        primary_col: 優先する列名
        secondary_cols: 代替列名のリスト
        
    Returns:
        マージ済みデータフレーム
    """
    result_df = df.copy()
    
    # プライマリ列が存在しない場合は作成
    if primary_col not in result_df.columns:
        result_df[primary_col] = pd.NA
    
    # 各行についてマージ処理
    for idx in result_df.index:
        if pd.isna(result_df.at[idx, primary_col]) or str(result_df.at[idx, primary_col]).strip() == '':
            # セカンダリ列から値を探す
            for sec_col in secondary_cols:
                if sec_col in result_df.columns:
                    value = result_df.at[idx, sec_col]
                    if pd.notna(value) and str(value).strip() != '':
                        result_df.at[idx, primary_col] = value
                        break
    
    return result_df


def calculate_date_difference(date1: Union[str, datetime], 
                            date2: Union[str, datetime],
                            date_format: str = '%Y-%m-%d') -> Optional[int]:
    """
    2つの日付の差を計算（日数）
    
    Args:
        date1: 日付1
        date2: 日付2
        date_format: 日付フォーマット
        
    Returns:
        日数差（date1 - date2）
    """
    try:
        if isinstance(date1, str):
            date1 = datetime.strptime(date1, date_format)
        if isinstance(date2, str):
            date2 = datetime.strptime(date2, date_format)
        
        return (date1 - date2).days
        
    except (ValueError, TypeError):
        return None


def safe_convert_to_int(value: Union[str, int, float], default: int = 0) -> int:
    """
    安全に整数に変換
    
    Args:
        value: 変換する値
        default: 変換できない場合のデフォルト値
        
    Returns:
        整数値
    """
    if pd.isna(value):
        return default
    
    try:
        # 文字列の場合はカンマを除去
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        
        return int(float(value))
        
    except (ValueError, TypeError):
        return default


def create_summary_report(df: pd.DataFrame, 
                        group_by_columns: List[str]) -> pd.DataFrame:
    """
    サマリーレポートを作成
    
    Args:
        df: 集計するデータフレーム
        group_by_columns: グループ化する列のリスト
        
    Returns:
        サマリーデータフレーム
    """
    if not group_by_columns:
        # 全体のサマリー
        return pd.DataFrame({
            '件数': [len(df)],
            '処理日時': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })
    
    # グループ別集計
    summary = df.groupby(group_by_columns).size().reset_index(name='件数')
    summary['処理日時'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return summary