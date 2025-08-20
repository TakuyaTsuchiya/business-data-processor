"""
Mapperで使用するカスタム例外クラス

マッピング処理中に発生する可能性のある各種エラーを定義します。
"""


class MappingError(Exception):
    """マッピングエラーの基底クラス"""
    pass


class RequiredFieldError(MappingError):
    """必須フィールドが存在しない場合のエラー"""
    
    def __init__(self, field_name: str, message: str = None):
        self.field_name = field_name
        if message is None:
            message = f"必須フィールド '{field_name}' が見つかりません"
        super().__init__(message)


class DataTypeError(MappingError):
    """データ型が期待と異なる場合のエラー"""
    
    def __init__(self, field_name: str, expected_type: type, actual_type: type, message: str = None):
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_type = actual_type
        if message is None:
            message = f"フィールド '{field_name}' のデータ型が不正です。期待: {expected_type.__name__}, 実際: {actual_type.__name__}"
        super().__init__(message)


class ValidationError(MappingError):
    """バリデーションエラー"""
    
    def __init__(self, field_name: str, value: any, reason: str):
        self.field_name = field_name
        self.value = value
        self.reason = reason
        message = f"フィールド '{field_name}' の値 '{value}' が無効です: {reason}"
        super().__init__(message)


class MappingRuleError(MappingError):
    """マッピングルール定義エラー"""
    
    def __init__(self, rule_name: str, reason: str):
        self.rule_name = rule_name
        self.reason = reason
        message = f"マッピングルール '{rule_name}' にエラーがあります: {reason}"
        super().__init__(message)


class BatchProcessingError(MappingError):
    """バッチ処理エラー"""
    
    def __init__(self, row_index: int, original_error: Exception):
        self.row_index = row_index
        self.original_error = original_error
        message = f"行 {row_index} の処理中にエラーが発生しました: {str(original_error)}"
        super().__init__(message)