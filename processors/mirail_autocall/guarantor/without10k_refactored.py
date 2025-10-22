"""
ミライル保証人オートコール処理プロセッサー（残債10,000円・11,000円除外・リファクタリング版）
共通フィルタリングエンジンを使用
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, List

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS
from domain.rules.business_rules import MIRAIL_DEBT_EXCLUDE
sys.path.append(os.path.join(processors_dir, 'autocall_common'))
from filter_engine import apply_filters
from common.contract_list_columns import ContractListColumns as COL


class MirailGuarantorWithout10kConfig:
    """ミライル保証人（残債除外版）処理の設定"""
    
    # フィルタ設定（without10k版：残債10,000円・11,000円除外）
    FILTER_CONFIG = {
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
        "arrears": {
            "column": COL.DEBT_AMOUNT,
            "min_amount": 1,
            "label": "滞納残債"
        },
        "special_debt": {
            "client_cd_column": COL.CLIENT_CD,
            "debt_column": COL.DEBT_AMOUNT,
            "conditions": {
                "client_cds": [1, 4],
                "debt_amounts": MIRAIL_DEBT_EXCLUDE  # [10000, 11000]
            },
            "label": "ミライル特殊残債"
        },
        "mobile_phone": {
            "column": COL.TEL_MOBILE_1,  # 保証人電話番号（AU列=列46）
            "log_type": "phone",
            "label": "保証人電話"
        },
        "payment_amount": {
            "column": COL.PAYMENT_AMOUNT,
            "exclude": [2, 3, 5, 12],
            "log_type": "amount",
            "label": "除外金額"
        }
    }
    
    # マッピングルール（列番号ベース）
    MAPPING_RULES = {
        "電話番号": COL.TEL_MOBILE_1,
        "架電番号": COL.TEL_MOBILE_1,
        "入居ステータス": COL.RESIDENCE_STATUS,
        "滞納ステータス": COL.DELINQUENT_STATUS,
        "管理番号": COL.MANAGEMENT_NO,
        "契約者名（カナ）": COL.CONTRACT_KANA,  # 保証人でも契約者名を入れる
        "物件名": COL.PROPERTY_NAME,
        "クライアント": COL.CLIENT_NAME,
        "残債": COL.DEBT_AMOUNT  # J列「残債」にBT列「滞納残債」を格納
    }
    
    OUTPUT_FILE_PREFIX = "ミライル_without10k_保証人"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def create_mirail_guarantor_output(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """ミライル保証人出力データ作成（28列統一フォーマット）"""
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # データが0件の場合
    if len(df_filtered) == 0:
        return df_output
    
    # データをマッピング
    mapping_rules = MirailGuarantorWithout10kConfig.MAPPING_RULES
    for i in range(len(df_filtered)):
        for output_col, col_index in mapping_rules.items():
            if output_col in df_output.columns:
                value = df_filtered.iloc[i, col_index]
                df_output.at[i, output_col] = str(value) if pd.notna(value) else ""
    
    return df_output


def process_mirail_guarantor_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    ミライル保証人（残債除外版）データの処理メイン関数
    
    Args:
        file_content: ContractListのファイル内容（bytes）
        
    Returns:
        tuple: (出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み（保証人データ）
        logs.append("ファイル読み込み開始")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み完了: {len(df_input)}件")
        
        # 保証人処理に必須な列チェック（列数で確認）
        if len(df_input.columns) < 119:  # 最低限必要な列数（委託先法人IDはDO列=118）
            raise ValueError(f"列数が不足しています。期待: 119列以上、実際: {len(df_input.columns)}列")
        
        # 2. 共通フィルタリングエンジンを使用
        df_filtered, filter_logs = apply_filters(df_input, MirailGuarantorWithout10kConfig.FILTER_CONFIG)
        logs.extend(filter_logs)
        
        # 3. 保証人出力データ作成
        df_output = create_mirail_guarantor_output(df_filtered)
        logs.append(f"保証人出力データ作成完了: {len(df_output)}件")
        
        # 4. 保証人用出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{MirailGuarantorWithout10kConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル保証人（残債除外版）データ処理エラー: {str(e)}")