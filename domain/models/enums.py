"""
処理関連の列挙型定義

処理ステータス・エラータイプ等の標準定義を提供します。
"""

from enum import Enum, auto


class ProcessingStatus(Enum):
    """処理ステータス"""
    SUCCESS = "success"          # 正常完了
    WARNING = "warning"          # 警告あり完了
    ERROR = "error"              # エラー終了
    PROCESSING = "processing"    # 処理中
    CANCELLED = "cancelled"      # キャンセル
    
    def is_complete(self) -> bool:
        """処理完了判定"""
        return self in [ProcessingStatus.SUCCESS, ProcessingStatus.WARNING, ProcessingStatus.ERROR]
    
    def is_successful(self) -> bool:
        """成功判定（警告含む）"""
        return self in [ProcessingStatus.SUCCESS, ProcessingStatus.WARNING]


class ErrorType(Enum):
    """エラータイプ分類"""
    VALIDATION_ERROR = "validation_error"      # 入力検証エラー
    TYPE_ERROR = "type_error"                  # 型不整合エラー
    PROCESSING_ERROR = "processing_error"      # 処理実行エラー
    FILE_ERROR = "file_error"                  # ファイル関連エラー
    ENCODING_ERROR = "encoding_error"          # エンコーディングエラー
    FILTER_ERROR = "filter_error"              # フィルタ処理エラー
    MAPPING_ERROR = "mapping_error"            # マッピングエラー
    UNKNOWN_ERROR = "unknown_error"            # 不明なエラー
    
    @classmethod
    def from_exception(cls, exception: Exception) -> "ErrorType":
        """例外からエラータイプを判定"""
        exception_type = type(exception).__name__
        
        if "Validation" in exception_type:
            return cls.VALIDATION_ERROR
        elif "Type" in exception_type or isinstance(exception, TypeError):
            return cls.TYPE_ERROR
        elif "File" in exception_type or isinstance(exception, (IOError, FileNotFoundError)):
            return cls.FILE_ERROR
        elif "Encoding" in exception_type or isinstance(exception, UnicodeError):
            return cls.ENCODING_ERROR
        elif "Mapping" in exception_type:
            return cls.MAPPING_ERROR
        elif "Filter" in exception_type:
            return cls.FILTER_ERROR
        else:
            return cls.UNKNOWN_ERROR


class MessageType(Enum):
    """メッセージタイプ"""
    INFO = "info"        # 情報
    WARNING = "warning"  # 警告
    ERROR = "error"      # エラー
    DEBUG = "debug"      # デバッグ


class ProcessorType(Enum):
    """プロセッサータイプ"""
    # オートコール系
    MIRAIL_AUTOCALL = "mirail_autocall"
    FAITH_AUTOCALL = "faith_autocall"
    PLAZA_AUTOCALL = "plaza_autocall"
    
    # SMS系
    FAITH_SMS = "faith_sms"
    
    # 登録系
    ARK_REGISTRATION = "ark_registration"
    CAPCO_REGISTRATION = "capco_registration"
    
    # 更新系
    ARK_DEBT_UPDATE = "ark_debt_update"
    CAPCO_DEBT_UPDATE = "capco_debt_update"
    
    def get_display_name(self) -> str:
        """表示名を取得"""
        display_names = {
            self.MIRAIL_AUTOCALL: "ミライルオートコール",
            self.FAITH_AUTOCALL: "フェイスオートコール",
            self.PLAZA_AUTOCALL: "プラザオートコール",
            self.FAITH_SMS: "フェイスSMS",
            self.ARK_REGISTRATION: "アーク新規登録",
            self.CAPCO_REGISTRATION: "カプコ新規登録",
            self.ARK_DEBT_UPDATE: "アーク残債更新",
            self.CAPCO_DEBT_UPDATE: "カプコ残債更新"
        }
        return display_names.get(self, self.value)