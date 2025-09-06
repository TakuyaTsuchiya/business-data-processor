"""
ミライルシステムのビジネスルール定義

技術的詳細から分離された純粋なビジネスロジック。
"""

from typing import Dict, List, Any


class MirailBusinessRules:
    """ミライルのビジネスルール"""
    
    # システム識別
    SYSTEM_NAME = "mirail"
    
    # 委託先法人ID
    CLIENT_IDS = ["", "5"]  # 空白と"5"
    
    # 除外金額
    EXCLUDE_AMOUNTS = [2, 3, 5]
    SPECIAL_DEBT_AMOUNTS = [10000, 11000]  # 1万円、1万1千円
    
    # 除外する回収ランク
    EXCLUDE_RANKS = ["弁護士介入"]
    
    # 特殊残債フィルタ対象のクライアントCD
    SPECIAL_DEBT_CLIENT_CDS = [1, 4]
    
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
            "name": "緊急連絡人",
            "phone_field": "TEL携帯.2", 
            "phone_column_index": 56  # BE列
        }
    }
    
    @classmethod
    def get_filter_rules(cls, target: str, with_10k: bool = True) -> Dict[str, Any]:
        """
        フィルタリングルールを取得
        
        Args:
            target: 対象者タイプ（contract/guarantor/emergency_contact）
            with_10k: 1万円以上を含むか
            
        Returns:
            フィルタリングルールの辞書
        """
        if target not in cls.TARGET_CONFIG:
            raise ValueError(f"無効な対象者タイプ: {target}")
        
        rules = {
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
                "include_today": True
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
        
        # 1万円未満の場合、特殊残債フィルタを追加
        if not with_10k:
            rules["special_debt"] = {
                "client_cd_column": "クライアントCD",
                "client_cd_column_index": 76,
                "debt_column": "残債",
                "debt_column_index": 26,
                "client_cds": cls.SPECIAL_DEBT_CLIENT_CDS,
                "debt_amounts": cls.SPECIAL_DEBT_AMOUNTS,
                "type": "exclude_combination"
            }
        
        return rules
    
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
    def get_output_filename_pattern(cls, target: str, with_10k: bool = True) -> str:
        """
        出力ファイル名のパターンを取得
        
        Args:
            target: 対象者タイプ
            with_10k: 1万円以上を含むか
            
        Returns:
            ファイル名パターン
        """
        target_name = cls.TARGET_CONFIG[target]["name"]
        base_name = f"autocall_mirail_{target_name}"
        
        if not with_10k:
            base_name += "_1万未満"
        
        return base_name