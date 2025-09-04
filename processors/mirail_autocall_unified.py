"""
ミライルオートコール統合プロセッサー
6つの個別プロセッサーを1つに統合
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, List, Optional, Dict, Any

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.abspath(__file__))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS
sys.path.append(os.path.join(processors_dir, 'autocall_common'))
from filter_engine import apply_filters
from common.contract_list_columns import ContractListColumns as COL


class MirailAutocallUnifiedProcessor:
    """ミライルオートコール統合プロセッサー"""
    
    # 対象者別の設定
    TARGET_CONFIG = {
        "contract": {
            "phone_column": COL.TEL_MOBILE,        # AB列（27）
            "name_suffix": "契約者",
            "display_name": "契約者"
        },
        "guarantor": {
            "phone_column": COL.TEL_MOBILE_1,      # AU列（46）
            "name_suffix": "保証人",
            "display_name": "保証人"
        },
        "emergency_contact": {
            "phone_column": COL.TEL_MOBILE_2,      # BE列（56）
            "name_suffix": "緊急連絡人",
            "display_name": "緊急連絡人"
        }
    }
    
    # 残債除外金額の定義
    MIRAIL_DEBT_EXCLUDE = [10000, 11000]
    
    # 共通の除外金額
    COMMON_EXCLUDE_AMOUNTS = [2, 3, 5, 12]
    
    def __init__(self):
        """初期化"""
        self.logs = []
    
    def get_base_filter_config(self, target: str, with_10k: bool) -> Dict[str, Any]:
        """
        ベースとなるフィルタ設定を取得
        
        Args:
            target: 対象者タイプ
            with_10k: 10k含むかどうか
        
        Returns:
            フィルタ設定の辞書
        """
        # 95%共通の設定
        filter_config = {
            "trustee_id": {
                "column": COL.TRUSTEE_ID,
                "values": ["", "5"],
                "log_type": "id",
                "label": "委託先法人ID"
            },
            "payment_date": {
                "column": COL.PAYMENT_DATE,
                "type": "before_today",
                "log_type": "date",
                "label": "入金予定日",
                "top_n": 3
            },
            "collection_rank": {
                "column": COL.COLLECTION_RANK,
                "exclude": ["弁護士介入"],
                "log_type": "category",
                "label": "回収ランク"
            },
            "mobile_phone": {
                "column": self.TARGET_CONFIG[target]["phone_column"],
                "log_type": "phone",
                "label": f"{self.TARGET_CONFIG[target]['display_name']}電話"
            },
            "payment_amount": {
                "column": COL.PAYMENT_AMOUNT,
                "exclude": self.COMMON_EXCLUDE_AMOUNTS,
                "log_type": "amount",
                "label": "除外金額"
            }
        }
        
        # without10kの場合、残債フィルタを追加
        if not with_10k:
            filter_config["special_debt"] = {
                "client_cd_column": COL.CLIENT_CD,
                "debt_column": COL.DEBT_AMOUNT,
                "conditions": {
                    "client_cds": [1, 4],
                    "debt_amounts": self.MIRAIL_DEBT_EXCLUDE
                },
                "label": "ミライル特殊残債"
            }
        
        return filter_config
    
    def read_csv_auto_encoding(self, file_content: bytes) -> pd.DataFrame:
        """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
        encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
        
        for enc in encodings:
            try:
                return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
            except Exception:
                continue
        
        raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")
    
    def get_mapping_rules(self, target: str) -> Dict[str, int]:
        """
        対象者別のマッピングルールを取得
        
        Args:
            target: 対象者タイプ
            
        Returns:
            マッピングルールの辞書
        """
        # 基本マッピング（全対象者共通）
        base_mapping = {
            "入居ステータス": COL.RESIDENCE_STATUS,
            "滞納ステータス": COL.DELINQUENT_STATUS,
            "管理番号": COL.MANAGEMENT_NO,
            "契約者名（カナ）": COL.CONTRACT_KANA,
            "物件名": COL.PROPERTY_NAME,
            "クライアント": COL.CLIENT_NAME,
            "残債": COL.DEBT_AMOUNT
        }
        
        # 対象者別の電話番号列を追加
        phone_col = self.TARGET_CONFIG[target]["phone_column"]
        base_mapping["電話番号"] = phone_col
        base_mapping["架電番号"] = phone_col
        
        return base_mapping
    
    def create_output_data(self, df_filtered: pd.DataFrame, target: str) -> pd.DataFrame:
        """
        出力データを作成
        
        Args:
            df_filtered: フィルタリング済みDataFrame
            target: 対象者タイプ
            
        Returns:
            28列統一フォーマットのDataFrame
        """
        # 28列の統一フォーマットで初期化
        df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
        df_output = df_output.fillna("")
        
        # データをマッピング
        mapping_rules = self.get_mapping_rules(target)
        for i in range(len(df_filtered)):
            for output_col, col_index in mapping_rules.items():
                if output_col in df_output.columns:
                    value = df_filtered.iloc[i, col_index]
                    df_output.at[i, output_col] = str(value) if pd.notna(value) else ""
        
        return df_output
    
    def process_mirail_autocall(
        self,
        file_content: bytes,
        target: str,
        with_10k: bool = True
    ) -> Tuple[pd.DataFrame, List[str], str]:
        """
        ミライルオートコール処理のメイン関数
        
        Args:
            file_content: ContractListのファイル内容（bytes）
            target: 対象者タイプ ("contract", "guarantor", "emergency_contact")
            with_10k: 10,000円・11,000円を含むかどうか
            
        Returns:
            tuple: (出力DF, 処理ログ, 出力ファイル名)
        """
        try:
            # パラメータ検証
            if target not in self.TARGET_CONFIG:
                raise ValueError(f"無効な対象者タイプ: {target}")
            
            # 1. CSVファイル読み込み
            self.logs = [f"📂 {self.TARGET_CONFIG[target]['display_name']}データ処理開始..."]
            df_input = self.read_csv_auto_encoding(file_content)
            self.logs.append(f"ファイル読み込み完了: {len(df_input)}件")
            
            # 2. フィルタ設定を取得
            filter_config = self.get_base_filter_config(target, with_10k)
            
            # 3. 共通フィルタリングエンジンを使用
            df_filtered, filter_logs = apply_filters(df_input, filter_config)
            self.logs.extend(filter_logs)
            
            # 4. 出力データ作成
            df_output = self.create_output_data(df_filtered, target)
            
            # 5. 出力ファイル名生成
            today_str = datetime.now().strftime("%m%d")
            suffix = self.TARGET_CONFIG[target]["name_suffix"]
            prefix = "with10k" if with_10k else "without10k"
            output_filename = f"{today_str}ミライル_{prefix}_{suffix}.csv"
            
            self.logs.append(f"✅ 処理完了: {len(df_output)}件出力")
            
            return df_output, self.logs, output_filename
            
        except Exception as e:
            error_msg = f"{self.TARGET_CONFIG[target]['display_name']}データ処理エラー: {str(e)}"
            self.logs.append(f"❌ {error_msg}")
            raise Exception(error_msg)