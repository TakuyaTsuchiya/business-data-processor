"""
ドメイン固有の例外クラス定義

ビジネスロジックに関連するエラーを明確に分類し、
適切なエラーハンドリングとユーザーへの情報提供を実現。
"""

from typing import Optional, Dict, Any


class BusinessDataProcessorError(Exception):
    """
    Business Data Processorの基底例外クラス
    
    全てのカスタム例外はこのクラスを継承する。
    エラーコード、ユーザー向けメッセージ、技術的詳細を含む。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str,
        user_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        初期化
        
        Args:
            message: 技術的なエラーメッセージ（ログ用）
            error_code: エラーコード（例: "E001", "VAL001"）
            user_message: ユーザー向けメッセージ（日本語）
            details: 追加の詳細情報（デバッグ用）
        """
        super().__init__(message)
        self.error_code = error_code
        self.user_message = user_message or message
        self.details = details or {}


class DataValidationError(BusinessDataProcessorError):
    """
    データ検証エラー
    
    CSVフォーマット、エンコーディング、必須カラムの不足など、
    入力データの検証に失敗した場合に発生。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str = "VAL001",
        user_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        missing_columns: Optional[list] = None,
        found_columns: Optional[list] = None
    ):
        super().__init__(message, error_code, user_message, details)
        if missing_columns:
            self.details['missing_columns'] = missing_columns
        if found_columns:
            self.details['found_columns'] = found_columns


class EncodingError(DataValidationError):
    """
    エンコーディングエラー
    
    ファイルのエンコーディングが判定できない、
    またはデコードに失敗した場合に発生。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str = "ENC001",
        user_message: Optional[str] = None,
        tried_encodings: Optional[list] = None,
        file_info: Optional[Dict[str, Any]] = None
    ):
        details = {}
        if tried_encodings:
            details['tried_encodings'] = tried_encodings
        if file_info:
            details['file_info'] = file_info
            
        default_user_message = (
            "ファイルの文字コードを判定できませんでした。\n"
            "ファイルがCP932（Windows）またはUTF-8形式で保存されているか確認してください。"
        )
        
        super().__init__(
            message, 
            error_code,
            user_message or default_user_message,
            details
        )


class ProcessingError(BusinessDataProcessorError):
    """
    処理エラー
    
    ビジネスロジックの実行中に発生したエラー。
    データ変換、マッピング、出力生成などで使用。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str = "PRO001",
        user_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        processor_name: Optional[str] = None,
        row_number: Optional[int] = None
    ):
        super().__init__(message, error_code, user_message, details)
        if processor_name:
            self.details['processor'] = processor_name
        if row_number is not None:
            self.details['row_number'] = row_number


class ConfigurationError(BusinessDataProcessorError):
    """
    設定エラー
    
    無効なパラメータ、設定値の不整合、
    必須設定の欠如などで発生。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str = "CFG001",
        user_message: Optional[str] = None,
        parameter_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        valid_values: Optional[list] = None
    ):
        details = {}
        if parameter_name:
            details['parameter'] = parameter_name
        if invalid_value is not None:
            details['invalid_value'] = invalid_value
        if valid_values:
            details['valid_values'] = valid_values
            
        super().__init__(message, error_code, user_message, details)


class FileHandlingError(BusinessDataProcessorError):
    """
    ファイル操作エラー
    
    ファイルの読み込み、書き込み、
    ファイル形式の判定などで発生。
    """
    
    def __init__(
        self,
        message: str,
        error_code: str = "FIL001",
        user_message: Optional[str] = None,
        file_path: Optional[str] = None,
        operation: Optional[str] = None
    ):
        details = {}
        if file_path:
            details['file_path'] = file_path
        if operation:
            details['operation'] = operation
            
        super().__init__(message, error_code, user_message, details)


# エラーコード定義（参考用）
ERROR_CODES = {
    # データ検証エラー (VAL)
    "VAL001": "一般的なデータ検証エラー",
    "VAL002": "必須カラムが不足",
    "VAL003": "データ型が不正",
    "VAL004": "データ範囲が不正",
    
    # エンコーディングエラー (ENC)
    "ENC001": "エンコーディング判定失敗",
    "ENC002": "デコードエラー",
    "ENC003": "エンコードエラー（出力時）",
    
    # 処理エラー (PRO)
    "PRO001": "一般的な処理エラー",
    "PRO002": "データマッピングエラー",
    "PRO003": "データ変換エラー",
    "PRO004": "ビジネスルール違反",
    
    # 設定エラー (CFG)
    "CFG001": "一般的な設定エラー",
    "CFG002": "無効なパラメータ値",
    "CFG003": "必須パラメータの欠如",
    
    # ファイル操作エラー (FIL)
    "FIL001": "一般的なファイル操作エラー",
    "FIL002": "ファイルが見つからない",
    "FIL003": "ファイル読み込みエラー",
    "FIL004": "ファイル書き込みエラー",
}


def create_user_friendly_message(error: BusinessDataProcessorError) -> str:
    """
    エラーからユーザーフレンドリーなメッセージを生成
    
    Args:
        error: BusinessDataProcessorErrorまたはその派生クラス
        
    Returns:
        ユーザー向けのエラーメッセージ（日本語）
    """
    base_message = f"エラーが発生しました（コード: {error.error_code}）\n\n"
    base_message += error.user_message
    
    # 特定のエラータイプに応じた追加情報
    if isinstance(error, DataValidationError) and 'missing_columns' in error.details:
        base_message += f"\n\n不足しているカラム: {', '.join(error.details['missing_columns'])}"
    
    if isinstance(error, ConfigurationError) and 'valid_values' in error.details:
        base_message += f"\n\n有効な値: {', '.join(map(str, error.details['valid_values']))}"
    
    return base_message