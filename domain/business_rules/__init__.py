"""
ビジネスルール定義モジュール

各システムの純粋なビジネスロジックを定義。
技術的な実装詳細から完全に分離された、宣言的なルール定義。
"""

from .mirail_rules import MirailBusinessRules
from .faith_rules import FaithBusinessRules
from .plaza_rules import PlazaBusinessRules

__all__ = [
    'MirailBusinessRules',
    'FaithBusinessRules',
    'PlazaBusinessRules',
]