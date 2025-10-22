"""
ミライル（残債含む）契約者データ処理プロセッサー（リファクタリング版）
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
sys.path.append(os.path.join(processors_dir, 'autocall_common'))
from filter_engine import apply_filters
from common.contract_list_columns import ContractListColumns as COL


class MirailWith10kConfig:
    """ミライル契約者（残債含む）の設定"""
    
    # フィルタ設定（with10k版：特殊残債フィルタなし）
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
        # with10k版では特殊残債フィルタなし（10,000円・11,000円も含む）
        "mobile_phone": {
            "column": COL.TEL_MOBILE,
            "log_type": "phone",
            "label": "携帯電話"
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
        "電話番号": COL.TEL_MOBILE,
        "架電番号": COL.TEL_MOBILE,
        "入居ステータス": COL.RESIDENCE_STATUS,
        "滞納ステータス": COL.DELINQUENT_STATUS,
        "管理番号": COL.MANAGEMENT_NO,
        "契約者名（カナ）": COL.CONTRACT_KANA,
        "物件名": COL.PROPERTY_NAME,
        "クライアント": COL.CLIENT_NAME,
        "残債": COL.DEBT_AMOUNT  # J列「残債」にBT列「滞納残債」を格納
    }
    
    OUTPUT_FILE_PREFIX = "ミライル_with10k_契約者"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def create_mirail_with10k_output(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """ミライル（残債含む）出力データ作成（28列統一フォーマット）"""
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # データをマッピング
    mapping_rules = MirailWith10kConfig.MAPPING_RULES
    for i in range(len(df_filtered)):
        for output_col, col_index in mapping_rules.items():
            if output_col in df_output.columns:
                value = df_filtered.iloc[i, col_index]
                df_output.at[i, output_col] = str(value) if pd.notna(value) else ""
    
    return df_output


def process_mirail_contract_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    ミライル契約者（残債含む）データの処理メイン関数
    
    Args:
        file_content: ContractListのファイル内容（bytes）
        
    Returns:
        tuple: (出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        # 1. CSVファイル読み込み
        df_input = read_csv_auto_encoding(file_content)
        
        # 2. 共通フィルタリングエンジンを使用
        df_filtered, logs = apply_filters(df_input, MirailWith10kConfig.FILTER_CONFIG)
        
        # 3. 出力データ作成
        df_output = create_mirail_with10k_output(df_filtered)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{MirailWith10kConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル契約者（残債含む）データ処理エラー: {str(e)}")