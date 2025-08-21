"""
Domain層モデル定義

処理結果・統計情報の標準データモデルを提供します。
"""

from .processing_models import (
    ProcessingResult,
    ProcessingStatistics
)
from .enums import (
    ProcessingStatus,
    ErrorType,
    MessageType
)

__all__ = [
    'ProcessingResult',
    'ProcessingStatistics',
    'ProcessingStatus',
    'ErrorType',
    'MessageType'
]