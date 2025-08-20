"""
Mapperファクトリークラス

適切なMapperインスタンスを生成するファクトリークラスを提供します。
システムとプロセスタイプの組み合わせから、対応するMapperを選択します。
"""

from typing import Optional, Dict, Type
from .base import BaseMapper
from .mirail_mapper import (
    MirailContractMapper,
    MirailGuarantorMapper,
    MirailEmergencyContactMapper
)
from .exceptions import MappingRuleError


class MapperFactory:
    """
    Mapperインスタンスを生成するファクトリークラス
    
    システム名とプロセスタイプから適切なMapperを選択して生成します。
    """
    
    # Mapperクラスのレジストリ
    _mapper_registry: Dict[str, Type[BaseMapper]] = {
        # ミライルシステム
        "mirail_contract": MirailContractMapper,
        "mirail_guarantor": MirailGuarantorMapper,
        "mirail_emergency": MirailEmergencyContactMapper,
        "mirail_emergency_contact": MirailEmergencyContactMapper,  # エイリアス
        
        # フェイスシステム（今後実装）
        # "faith_contract": FaithContractMapper,
        # "faith_guarantor": FaithGuarantorMapper,
        # "faith_emergency": FaithEmergencyContactMapper,
        # "faith_sms": FaithSMSMapper,
        
        # プラザシステム（今後実装）
        # "plaza_main": PlazaMainMapper,
        # "plaza_guarantor": PlazaGuarantorMapper,
        # "plaza_contact": PlazaContactMapper,
        
        # アークシステム（今後実装）
        # "ark_registration": ArkRegistrationMapper,
        # "ark_late_payment": ArkLatePaymentMapper,
        
        # カプコシステム（今後実装）
        # "capco_registration": CapcoRegistrationMapper,
        # "capco_debt_update": CapcoDebtUpdateMapper,
    }
    
    @classmethod
    def create(cls, mapper_type: str) -> Optional[BaseMapper]:
        """
        指定されたタイプのMapperを生成
        
        Args:
            mapper_type (str): Mapperタイプ（例: "mirail_contract"）
            
        Returns:
            BaseMapper: 生成されたMapperインスタンス
            
        Raises:
            MappingRuleError: 不明なMapperタイプの場合
        """
        mapper_class = cls._mapper_registry.get(mapper_type)
        
        if mapper_class is None:
            available_types = ", ".join(cls._mapper_registry.keys())
            raise MappingRuleError(
                mapper_type,
                f"未知のMapperタイプです。利用可能なタイプ: {available_types}"
            )
        
        return mapper_class()
    
    @classmethod
    def create_by_system_and_process(cls, system: str, process_type: str) -> Optional[BaseMapper]:
        """
        システム名とプロセスタイプからMapperを生成
        
        Args:
            system (str): システム名（例: "mirail", "faith", "plaza"）
            process_type (str): プロセスタイプ（例: "contract", "guarantor", "emergency"）
            
        Returns:
            BaseMapper: 生成されたMapperインスタンス
            
        Raises:
            MappingRuleError: 不明な組み合わせの場合
        """
        # プロセスタイプの正規化（緊急連絡人の表記ゆれ対応）
        if process_type == "emergency_contact":
            process_type = "emergency"
        
        mapper_type = f"{system}_{process_type}"
        return cls.create(mapper_type)
    
    @classmethod
    def register(cls, mapper_type: str, mapper_class: Type[BaseMapper]):
        """
        新しいMapperクラスを登録
        
        Args:
            mapper_type (str): Mapperタイプ
            mapper_class (Type[BaseMapper]): Mapperクラス
        """
        if not issubclass(mapper_class, BaseMapper):
            raise ValueError(f"{mapper_class.__name__} はBaseMapperを継承していません")
        
        cls._mapper_registry[mapper_type] = mapper_class
    
    @classmethod
    def get_available_types(cls) -> list:
        """
        利用可能なMapperタイプのリストを取得
        
        Returns:
            list: Mapperタイプのリスト
        """
        return list(cls._mapper_registry.keys())
    
    @classmethod
    def is_supported(cls, mapper_type: str) -> bool:
        """
        指定されたMapperタイプがサポートされているか確認
        
        Args:
            mapper_type (str): Mapperタイプ
            
        Returns:
            bool: サポートされている場合True
        """
        return mapper_type in cls._mapper_registry