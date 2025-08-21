"""
Services層アダプター

既存プロセッサーとの互換性を提供するアダプター群です。
"""

from .legacy_adapter import LegacyProcessorAdapter

__all__ = [
    'LegacyProcessorAdapter'
]