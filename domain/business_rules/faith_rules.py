"""
フェイスシステムのビジネスルール定義

技術的詳細から分離された純粋なビジネスロジック。
"""

from typing import Dict, List, Any


class FaithBusinessRules:
    """フェイスのビジネスルール"""
    
    # システム識別
    SYSTEM_NAME = "faith"
    
    # 委託先法人ID
    CLIENT_IDS = [1, 2, 3, 4]
    
    # 除外金額（フェイスは12を含まない）
    EXCLUDE_AMOUNTS = [2, 3, 5]
    
    # 除外する回収ランク
    EXCLUDE_RANKS = ["死亡決定", "破産決定", "弁護士介入"]
    
    # 対象者別設定
    TARGET_CONFIG = {
        "contract": {
            "name": "契約者",
            "phone_field": "TEL携帯",
            "phone_column_index": 27  # AB列
        },
        "guarantor": {
            "name": "保証人",
            "phone_field": "TEL携帯.1",
            "phone_column_index": 46  # AU列
        },
        "emergency_contact": {
            "name": "連絡人",  # フェイスは「連絡人」
            "display_name": "緊急連絡人",
            "phone_field": "TEL携帯.2",
            "phone_column_index": 56  # BE列
        }
    }
    
    @classmethod
    def get_filter_rules(cls, target: str) -> Dict[str, Any]:
        """
        フィルタリングルールを取得
        
        Args:
            target: 対象者タイプ（contract/guarantor/emergency_contact）
            
        Returns:
            フィルタリングルールの辞書
        """
        if target not in cls.TARGET_CONFIG:
            raise ValueError(f"無効な対象者タイプ: {target}")
        
        return {
            "trustee_id": {
                "column": "委託先法人ID",
                "column_index": 77,
                "values": cls.CLIENT_IDS,
                "type": "include"
            },
            "payment_date": {
                "column": "入金予定日",
                "column_index": 8,
                "type": "before_today",
                "include_today": False  # フェイスは当日を含まない
            },
            "payment_amount": {
                "column": "入金予定金額",
                "column_index": 14,
                "values": cls.EXCLUDE_AMOUNTS,
                "type": "exclude"
            },
            "collection_rank": {
                "column": "回収ランク",
                "column_index": 111,
                "values": cls.EXCLUDE_RANKS,
                "type": "exclude"
            },
            "phone": {
                "column": cls.TARGET_CONFIG[target]["phone_field"],
                "column_index": cls.TARGET_CONFIG[target]["phone_column_index"],
                "type": "not_empty"
            }
        }
    
    @classmethod
    def get_output_mapping(cls, target: str) -> Dict[str, int]:
        """
        出力用のカラムマッピングを取得
        
        Args:
            target: 対象者タイプ
            
        Returns:
            {"出力カラム名": 入力カラムインデックス} の辞書
        """
        phone_col_index = cls.TARGET_CONFIG[target]["phone_column_index"]
        
        return {
            "入居ステータス": 17,      # R列
            "滞納ステータス": 18,      # S列
            "管理番号": 0,              # A列
            "契約者名（カナ）": 3,      # D列
            "物件名": 73,               # BV列
            "クライアント": 75,         # BX列
            "残債": 26,                 # AA列
            "電話番号": phone_col_index,
            "架電番号": phone_col_index
        }
    
    @classmethod
    def get_output_filename_pattern(cls, target: str) -> str:
        """
        出力ファイル名のパターンを取得
        
        Args:
            target: 対象者タイプ
            
        Returns:
            ファイル名パターン
        """
        target_config = cls.TARGET_CONFIG[target]
        target_name = target_config.get("display_name", target_config["name"])
        return f"autocall_faith_{target_name}"