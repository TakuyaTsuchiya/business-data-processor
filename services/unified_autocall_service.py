"""
統合オートコールサービス

Infrastructure層とDomain層を橋渡しし、オートコール処理のフローを制御。
"""

import pandas as pd
from typing import Tuple, List, Dict, Any
from datetime import datetime

from infrastructure import CsvProcessor, DataFrameBuilder
from domain.business_rules import (
    MirailBusinessRules,
    FaithBusinessRules,
    PlazaBusinessRules
)
from processors.autocall_common.filter_engine import apply_filters
from processors.autocall_constants import AUTOCALL_OUTPUT_COLUMNS
from services.logger import get_logger


logger = get_logger(__name__)


class UnifiedAutocallService:
    """統合オートコールサービス"""
    
    # システムとビジネスルールのマッピング
    BUSINESS_RULES_MAP = {
        "mirail": MirailBusinessRules,
        "faith": FaithBusinessRules,
        "plaza": PlazaBusinessRules
    }
    
    def __init__(self):
        """初期化"""
        self.csv_processor = CsvProcessor()
        self.dataframe_builder = DataFrameBuilder()
        self.logs = []
    
    def process(
        self,
        system: str,
        target: str,
        file_content: bytes,
        **options
    ) -> Tuple[pd.DataFrame, List[str], str]:
        """
        統合オートコール処理
        
        Args:
            system: システム名（mirail/faith/plaza）
            target: 対象者タイプ
            file_content: CSVファイルのバイトデータ
            options: 追加オプション（with_10k等）
            
        Returns:
            tuple: (出力DataFrame, ログリスト, ファイル名)
        """
        try:
            # ビジネスルールの取得
            if system not in self.BUSINESS_RULES_MAP:
                raise ValueError(f"未対応のシステム: {system}")
            
            business_rules = self.BUSINESS_RULES_MAP[system]
            
            # ログ開始
            target_config = business_rules.TARGET_CONFIG.get(target, {})
            display_name = target_config.get("display_name", target_config.get("name", target))
            self.logs = [f"📂 {system.capitalize()} {display_name}データ処理開始..."]
            
            # 1. Infrastructure層でCSV読み込み
            df_input = self.csv_processor.read_csv(file_content)
            self.logs.append(f"ファイル読み込み完了: {len(df_input)}件")
            
            # 2. Domain層でフィルタルール取得
            filter_args = [target]
            if system == "mirail" and "with_10k" in options:
                filter_args.append(options["with_10k"])
            
            filter_rules = business_rules.get_filter_rules(*filter_args)
            
            # 3. フィルタリング実行（既存のfilter_engineを活用）
            filter_config = self._convert_to_filter_config(filter_rules)
            df_filtered, filter_logs = apply_filters(df_input, filter_config)
            self.logs.extend(filter_logs)
            
            # 4. Domain層でマッピングルール取得
            mapping_rules = business_rules.get_output_mapping(target)
            
            # 5. Infrastructure層で出力DataFrame作成（ベクトル化）
            df_output = self.dataframe_builder.map_data_vectorized(
                df_filtered,
                mapping_rules,
                AUTOCALL_OUTPUT_COLUMNS
            )
            
            # 6. ファイル名生成
            filename_args = [target]
            if system == "mirail" and "with_10k" in options:
                filename_args.append(options["with_10k"])
            
            filename_pattern = business_rules.get_output_filename_pattern(*filename_args)
            filename = self.csv_processor.generate_filename(
                filename_pattern,
                timestamp=False,
                extension="csv"
            )
            
            # 7. 統計情報
            self.logs.append(f"\n📊 処理完了: {len(df_output)}件")
            
            # ロギング
            log_suffix = ""
            if system == "mirail" and "with_10k" in options:
                log_suffix = f"（1万円{'以上' if options['with_10k'] else '未満'}）"
            
            logger.info(
                f"処理完了 - {system.capitalize()} {display_name}{log_suffix} - {len(df_output)}件"
            )
            
            return df_output, self.logs, filename
            
        except Exception as e:
            error_msg = f"エラーが発生しました: {str(e)}"
            self.logs.append(f"❌ {error_msg}")
            logger.error(f"エラー - {system} {target}: {str(e)}", exc_info=True)
            raise
    
    def _convert_to_filter_config(self, filter_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        ビジネスルールのフィルタ定義を既存のfilter_engine形式に変換
        
        Args:
            filter_rules: ビジネスルールのフィルタ定義
            
        Returns:
            filter_engine用の設定
        """
        filter_config = {}
        
        for key, rule in filter_rules.items():
            if rule.get("type") == "include":
                # 含むフィルタ
                filter_config[key] = {
                    "column": rule["column_index"],
                    "values": rule["values"],
                    "log_type": "id" if key == "trustee_id" else "value",
                    "label": rule["column"]
                }
            elif rule.get("type") == "exclude":
                # 除外フィルタ
                filter_config[key] = {
                    "column": rule["column_index"],
                    "exclude": rule["values"],
                    "log_type": "amount" if key == "payment_amount" else "category",
                    "label": rule["column"]
                }
            elif rule.get("type") == "before_today":
                # 日付フィルタ
                filter_config[key] = {
                    "column": rule["column_index"],
                    "type": "before_today",
                    "include_today": rule.get("include_today", True),
                    "log_type": "date",
                    "label": rule["column"],
                    "top_n": 3
                }
            elif rule.get("type") == "not_empty":
                # 空でないフィルタ
                filter_config[key] = {
                    "column": rule["column_index"],
                    "log_type": "phone",
                    "label": rule["column"]
                }
            elif rule.get("type") == "exclude_combination":
                # 特殊残債フィルタ（ミライル用）
                filter_config["special_debt"] = {
                    "client_cd_column": rule["client_cd_column_index"],
                    "debt_column": rule["debt_column_index"],
                    "conditions": {
                        "client_cds": rule["client_cds"],
                        "debt_amounts": rule["debt_amounts"]
                    },
                    "label": "特殊残債フィルタ"
                }
        
        return filter_config