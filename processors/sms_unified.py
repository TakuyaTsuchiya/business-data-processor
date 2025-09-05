"""
SMS統合プロセッサー
ミライル・フェイス・プラザのSMS処理を統一

9つの個別プロセッサーを1つに統合し、
システム別の設定を外部化することで保守性を向上。
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime, date
from typing import Tuple, List, Dict, Any, Optional

# インフラ層のインポート
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infrastructure import read_csv_auto_encoding

# 共通定義のインポート
from processors.sms_common.constants import SMS_TEMPLATE_HEADERS as SMS_OUTPUT_COLUMNS
from processors.sms_common.utils import format_payment_deadline
from processors.common.contract_list_columns import ContractListColumns as COL
from processors.common.detailed_logger import DetailedLogger


class SmsUnifiedProcessor:
    """SMS統合プロセッサー"""
    
    # システム別設定
    SYSTEM_CONFIG = {
        "mirail": {
            "client_ids": ["5", ""],
            "exclude_amounts": [2, 3, 5, 12],
            "exclude_ranks": ["弁護士介入", "訴訟中"],
            "date_filter": "before_today",  # 入金予定日フィルタ
            "phone_columns": {
                "contract": COL.TEL_MOBILE,        # 27 (AB列)
                "guarantor": COL.TEL_MOBILE_1,     # 46 (AU列)
                "emergency": COL.TEL_MOBILE_2      # 56 (BE列)
            },
            "display_names": {
                "contract": "契約者",
                "guarantor": "保証人",
                "emergency": "緊急連絡先"
            }
        },
        "faith": {
            "client_ids": ["1", "2", "3", "4"],
            "exclude_amounts": [2, 3, 5],
            "exclude_ranks": ["弁護士介入", "破産決定", "死亡決定"],
            "date_filter": "before_today",
            "phone_columns": {
                "contract": COL.TEL_MOBILE,
                "guarantor": COL.TEL_MOBILE_1,
                "emergency": COL.TEL_MOBILE_2
            },
            "display_names": {
                "contract": "契約者",
                "guarantor": "保証人",
                "emergency": "緊急連絡人"
            },
            "column_access": "name"  # フェイスは列名アクセス
        },
        "plaza": {
            "client_ids": ["6"],
            "exclude_amounts": [2, 3, 5, 12],
            "exclude_ranks": ["弁護士介入", "死亡決定", "破産決定"],
            "date_filter": "before_today",
            "phone_columns": {
                "contract": COL.TEL_MOBILE,
                "guarantor": COL.TEL_MOBILE_1,
                "emergency": COL.TEL_MOBILE_2  # プラザは"連絡先"と呼ぶ
            },
            "display_names": {
                "contract": "契約者",
                "guarantor": "保証人",
                "emergency": "連絡先"
            },
            "column_access": "name",
            "special_process": "nationality_split"  # 国籍による分離処理
        }
    }
    
    # 銀行口座情報（全システム共通）
    BANK_ACCOUNT = "三井住友銀行 京都支店 普通 7052916"
    
    def __init__(self):
        """初期化"""
        self.logs = []
    
    def get_column_value(self, df: pd.DataFrame, column_index: int, column_access: str = "index") -> pd.Series:
        """
        列アクセス方法に応じて値を取得
        
        Args:
            df: DataFrame
            column_index: 列番号
            column_access: アクセス方法（"index" or "name"）
        """
        if column_access == "name":
            # 列名でアクセス（フェイス・プラザ用）
            column_name = df.columns[column_index]
            return df[column_name]
        else:
            # 列番号でアクセス（ミライル用）
            return df.iloc[:, column_index]
    
    def apply_filters(self, df: pd.DataFrame, system: str, target: str, payment_deadline: str) -> pd.DataFrame:
        """
        共通フィルタリング処理
        
        Args:
            df: 入力DataFrame
            system: システム名
            target: 対象者タイプ
            payment_deadline: 支払期限文字列
        
        Returns:
            フィルタリング済みDataFrame
        """
        config = self.SYSTEM_CONFIG[system]
        column_access = config.get("column_access", "index")
        original_count = len(df)
        self.logs.append(DetailedLogger.log_initial_load(original_count))
        
        # 1. 委託先法人IDフィルタ
        before_filter = len(df)
        client_ids = config["client_ids"]
        client_id_col = self.get_column_value(df, COL.TRUSTEE_ID, column_access)
        
        if system == "mirail":
            # ミライルは特殊（空白と"5"）
            df = df[client_id_col.astype(str).str.strip().isin(client_ids)]
        else:
            # フェイス・プラザは数値として比較
            df = df[client_id_col.astype(str).str.strip().isin([str(id) for id in client_ids])]
        
        self.logs.append(DetailedLogger.log_filter_result(
            f"委託先法人ID（{', '.join(map(str, client_ids))}）",
            before_filter,
            len(df)
        ))
        
        # 2. 入金予定日フィルタ（前日以前またはNaN）
        before_filter = len(df)
        payment_date_col = self.get_column_value(df, COL.PAYMENT_DATE, column_access)
        payment_dates = pd.to_datetime(payment_date_col, errors='coerce')
        yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        df = df[payment_dates.isna() | (payment_dates <= yesterday)]
        
        self.logs.append(DetailedLogger.log_filter_result(
            "入金予定日（前日以前またはNaN）",
            before_filter,
            len(df)
        ))
        
        # 3. 支払期限代入（NaN値のみ）
        payment_date_col = self.get_column_value(df, COL.PAYMENT_DATE, column_access)
        nan_indices = payment_date_col.isna()
        nan_count = nan_indices.sum()
        
        if nan_count > 0:
            if column_access == "name":
                df.loc[nan_indices, df.columns[COL.PAYMENT_DATE]] = payment_deadline
            else:
                df.iloc[nan_indices, COL.PAYMENT_DATE] = payment_deadline
            self.logs.append(f"支払期限代入: {nan_count}件に「{payment_deadline}」を設定")
        
        # 4. 入金予定金額フィルタ
        before_filter = len(df)
        payment_amount_col = self.get_column_value(df, COL.PAYMENT_AMOUNT, column_access)
        exclude_amounts = config["exclude_amounts"]
        payment_amounts = pd.to_numeric(payment_amount_col, errors='coerce')
        df = df[~payment_amounts.isin(exclude_amounts)]
        
        self.logs.append(DetailedLogger.log_filter_result(
            f"入金予定金額（{', '.join(map(str, exclude_amounts))}円除外）",
            before_filter,
            len(df)
        ))
        
        # 5. 回収ランクフィルタ
        before_filter = len(df)
        rank_col = self.get_column_value(df, COL.COLLECTION_RANK, column_access)
        exclude_ranks = config["exclude_ranks"]
        df = df[~rank_col.isin(exclude_ranks)]
        
        self.logs.append(DetailedLogger.log_filter_result(
            f"回収ランク（{', '.join(exclude_ranks)}除外）",
            before_filter,
            len(df)
        ))
        
        # 6. 電話番号フィルタ
        before_filter = len(df)
        phone_col_index = config["phone_columns"][target]
        phone_col = self.get_column_value(df, phone_col_index, column_access)
        df = df[
            phone_col.notna() &
            (~phone_col.astype(str).str.strip().isin(["", "nan", "NaN"]))
        ]
        
        self.logs.append(DetailedLogger.log_filter_result(
            f"{config['display_names'][target]}電話番号",
            before_filter,
            len(df)
        ))
        
        return df
    
    def create_output_data(self, df_filtered: pd.DataFrame, system: str, target: str) -> pd.DataFrame:
        """
        SMS出力データを作成
        
        Args:
            df_filtered: フィルタリング済みDataFrame
            system: システム名
            target: 対象者タイプ
        
        Returns:
            出力用DataFrame
        """
        config = self.SYSTEM_CONFIG[system]
        column_access = config.get("column_access", "index")
        
        # 出力用DataFrameを作成
        df_output = pd.DataFrame(index=range(len(df_filtered)), columns=SMS_OUTPUT_COLUMNS)
        df_output = df_output.fillna("")
        
        # データマッピング
        for i in range(len(df_filtered)):
            # 管理番号
            df_output.at[i, "契約No"] = str(
                self.get_column_value(df_filtered, COL.MANAGEMENT_NO, column_access).iloc[i]
            )
            
            # 電話番号
            phone_col_index = config["phone_columns"][target]
            df_output.at[i, "電話番号"] = str(
                self.get_column_value(df_filtered, phone_col_index, column_access).iloc[i]
            )
            
            # 対象者名（カナ）
            if target == "contract":
                name_col_index = COL.CONTRACT_KANA
            elif target == "guarantor":
                name_col_index = COL.GUARANTOR_KANA_1
            else:  # emergency
                name_col_index = COL.EMERGENCY_KANA_1
            
            df_output.at[i, "顧客名（カナ）"] = str(
                self.get_column_value(df_filtered, name_col_index, column_access).iloc[i]
            )
            
            # 物件名
            df_output.at[i, "物件名"] = str(
                self.get_column_value(df_filtered, COL.PROPERTY_NAME, column_access).iloc[i]
            )
            
            # 入金予定金額
            df_output.at[i, "入金額"] = str(
                self.get_column_value(df_filtered, COL.PAYMENT_AMOUNT, column_access).iloc[i]
            )
            
            # 銀行口座（固定値）
            df_output.at[i, "銀行口座"] = self.BANK_ACCOUNT
            
            # メモ（管理番号を再度設定）
            df_output.at[i, "メモ"] = str(
                self.get_column_value(df_filtered, COL.MANAGEMENT_NO, column_access).iloc[i]
            )
        
        return df_output
    
    def process_sms(
        self, 
        file_content: bytes,
        system: str,
        target: str,
        payment_deadline: date,
        call_center_file: Optional[bytes] = None
    ) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """
        SMS処理のメイン関数
        
        Args:
            file_content: ContractListのファイル内容
            system: システム名（"mirail", "faith", "plaza"）
            target: 対象者タイプ（"contract", "guarantor", "emergency"）
            payment_deadline: 支払期限
            call_center_file: プラザ用コールセンターCSV（オプション）
        
        Returns:
            tuple: (出力DataFrameリスト, 処理ログ, 出力ファイル名リスト)
        """
        try:
            # パラメータ検証
            if system not in self.SYSTEM_CONFIG:
                raise ValueError(f"無効なシステム: {system}")
            
            config = self.SYSTEM_CONFIG[system]
            if target not in config["phone_columns"]:
                raise ValueError(f"無効な対象者タイプ: {target}")
            
            # 1. 支払期限の文字列化
            payment_deadline_str = format_payment_deadline(payment_deadline)
            
            # 2. CSVファイル読み込み（インフラ層を使用）
            self.logs = [f"📂 {system.upper()} {config['display_names'][target]}SMS処理開始..."]
            df_input = read_csv_auto_encoding(file_content, dtype=str)
            self.logs.append(f"ファイル読み込み完了: {len(df_input)}件")
            
            # 3. 共通フィルタリング
            df_filtered = self.apply_filters(df_input, system, target, payment_deadline_str)
            
            # 4. プラザの特殊処理（国籍分離）
            if system == "plaza" and config.get("special_process") == "nationality_split":
                return self._process_plaza_nationality_split(
                    df_filtered, target, call_center_file, payment_deadline_str
                )
            
            # 5. 通常の出力処理
            df_output = self.create_output_data(df_filtered, system, target)
            
            # 6. 出力ファイル名生成
            system_prefix = system.capitalize()
            target_suffix = config['display_names'][target]
            output_filename = f"SMS{system_prefix}{target_suffix}.csv"
            
            self.logs.append(f"✅ 処理完了: {len(df_output)}件出力")
            
            return [df_output], self.logs, [output_filename]
            
        except Exception as e:
            error_msg = f"{system.upper()} {target}SMS処理エラー: {str(e)}"
            self.logs.append(f"❌ {error_msg}")
            raise Exception(error_msg)
    
    def _process_plaza_nationality_split(
        self, 
        df_filtered: pd.DataFrame,
        target: str,
        call_center_file: Optional[bytes],
        payment_deadline_str: str
    ) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """プラザの国籍分離処理（特殊ケース）"""
        # TODO: プラザの国籍分離とVLOOKUP処理を実装
        # 現時点では通常処理を返す
        df_output = self.create_output_data(df_filtered, "plaza", target)
        
        system_prefix = "Plaza"
        target_suffix = self.SYSTEM_CONFIG["plaza"]["display_names"][target]
        output_filename = f"SMS{system_prefix}{target_suffix}.csv"
        
        return [df_output], self.logs, [output_filename]