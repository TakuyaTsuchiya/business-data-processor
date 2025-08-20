"""
マッピング関連の Domain Layer

各システム用のMapperクラスを提供します。
"""

# 基底クラス
from .base import BaseMapper

# ミライル用Mapper
from .mirail_mapper import (
    MirailContractMapper,
    MirailGuarantorMapper,
    MirailEmergencyContactMapper
)

# ファクトリー
from .mapper_factory import MapperFactory

# エラークラス
from .exceptions import (
    MappingError,
    RequiredFieldError,
    DataTypeError,
    ValidationError,
    MappingRuleError,
    BatchProcessingError
)

__all__ = [
    # 基底クラス
    'BaseMapper',
    
    # ミライル用
    'MirailContractMapper',
    'MirailGuarantorMapper',
    'MirailEmergencyContactMapper',
    
    # ファクトリー
    'MapperFactory',
    
    # エラークラス
    'MappingError',
    'RequiredFieldError',
    'DataTypeError',
    'ValidationError',
    'MappingRuleError',
    'BatchProcessingError',
]