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

# フェイス用Mapper
from .faith_mapper import (
    FaithContractMapper,
    FaithGuarantorMapper,
    FaithEmergencyContactMapper,
    FaithSMSMapper
)

# プラザ用Mapper
from .plaza_mapper import (
    PlazaMainMapper,
    PlazaGuarantorMapper,
    PlazaContactMapper
)

# アーク用Mapper
from .ark_mapper import (
    ArkRegistrationMapper,
    ArkLatePaymentMapper
)

# カプコ用Mapper
from .capco_mapper import (
    CapcoRegistrationMapper,
    CapcoDebtUpdateMapper,
    CapcoPhoneCleaningMapper
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
    
    # フェイス用
    'FaithContractMapper',
    'FaithGuarantorMapper',
    'FaithEmergencyContactMapper',
    'FaithSMSMapper',
    
    # プラザ用
    'PlazaMainMapper',
    'PlazaGuarantorMapper',
    'PlazaContactMapper',
    
    # アーク用
    'ArkRegistrationMapper',
    'ArkLatePaymentMapper',
    
    # カプコ用
    'CapcoRegistrationMapper',
    'CapcoDebtUpdateMapper',
    'CapcoPhoneCleaningMapper',
    
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