"""
プラザオートコール統合プロセッサー
3つの個別プロセッサーを1つに統合
列番号ベース + FilterEngine使用
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, List, Dict, Any

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.abspath(__file__))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_constants import AUTOCALL_OUTPUT_COLUMNS
sys.path.append(os.path.join(processors_dir, 'autocall_common'))
from filter_engine import apply_filters
from common.contract_list_columns import ContractListColumns as COL


class PlazaAutocallUnifiedProcessor:
    """プラザオートコール統合プロセッサー"""
    
    # 対象者別の設定（列番号ベース）
    TARGET_CONFIG = {
        "main": {
            "phone_column": COL.TEL_MOBILE,        # 27 (AB列: TEL携帯)
            "name_suffix": "契約者",
            "display_name": "契約者",
            "include_today": True                  # 当日を含む
        },
        "guarantor": {
            "phone_column": COL.TEL_MOBILE_1,      # 46 (AU列: TEL携帯.1)
            "name_suffix": "保証人",
            "display_name": "保証人",
            "include_today": False                 # 当日を除外
        },
        "contact": {
            "phone_column": COL.TEL_MOBILE_2,      # 56 (BE列: 緊急連絡人１のTEL（携帯）)
            "name_suffix": "緊急連絡人",
            "display_name": "緊急連絡人",
            "include_today": False                 # 当日を除外
        }
    }
    
    # プラザ固有の設定
    PLAZA_CLIENT_ID = "6"
    EXCLUDE_RANKS = ["督促停止", "弁護士介入"]
    EXCLUDE_AMOUNTS = [2, 3, 5, 12]
    
    def __init__(self):
        """初期化"""
        self.logs = []
    
    def get_filter_config(self, target: str) -> Dict[str, Any]:
        """
        プラザ用のフィルタ設定を取得
        
        Args:
            target: 対象者タイプ
            
        Returns:
            FilterEngine用の設定辞書
        """
        filter_config = {
            "trustee_id": {
                "column": COL.TRUSTEE_ID,
                "values": [self.PLAZA_CLIENT_ID],
                "log_type": "id",
                "label": "委託先法人ID"
            },
            "payment_date": {
                "column": COL.PAYMENT_DATE,
                "type": "before_today",
                "include_today": self.TARGET_CONFIG[target]["include_today"],
                "log_type": "date",
                "label": "入金予定日",
                "top_n": 3
            },
            "collection_rank": {
                "column": COL.COLLECTION_RANK,
                "exclude": self.EXCLUDE_RANKS,
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
                "exclude": self.EXCLUDE_AMOUNTS,
                "log_type": "amount",
                "label": "除外金額"
            }
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
        対象者別のマッピングルールを取得（列番号ベース）
        
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
            "クライアント": COL.CLIENT_NAME
            # 注: プラザは残債列なし
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
    
    def process_plaza_autocall(
        self,
        file_content: bytes,
        target: str
    ) -> Tuple[pd.DataFrame, List[str], str]:
        """
        プラザオートコール処理のメイン関数
        
        Args:
            file_content: ContractListのファイル内容（bytes）
            target: 対象者タイプ ("main", "guarantor", "contact")
            
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
            filter_config = self.get_filter_config(target)
            
            # 3. 共通フィルタリングエンジンを使用
            df_filtered, filter_logs = apply_filters(df_input, filter_config)
            self.logs.extend(filter_logs)
            
            # 4. 出力データ作成
            df_output = self.create_output_data(df_filtered, target)
            
            # 5. 出力ファイル名生成
            today_str = datetime.now().strftime("%m%d")
            suffix = self.TARGET_CONFIG[target]["name_suffix"]
            output_filename = f"{today_str}プラザ_{suffix}.csv"
            
            self.logs.append(f"✅ 処理完了: {len(df_output)}件出力")
            
            return df_output, self.logs, output_filename
            
        except Exception as e:
            error_msg = f"{self.TARGET_CONFIG[target]['display_name']}データ処理エラー: {str(e)}"
            self.logs.append(f"❌ {error_msg}")
            raise Exception(error_msg)