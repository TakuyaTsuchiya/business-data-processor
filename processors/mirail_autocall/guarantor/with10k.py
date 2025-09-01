"""
ミライル保証人オートコール処理プロセッサー（残債含む全件）
統合アプリ用に移植
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


class MirailGuarantorWith10kConfig:
    """ミライル保証人（残債含む全件）処理の設定"""
    
    # フィルタリング条件（with10k版：残債フィルタなし・クライアントCDフィルタなし）
    FILTER_CONDITIONS = {
        "委託先法人ID": "空白と5",
        "入金予定日": "前日以前またはNaN",
        "回収ランク_not_in": ["弁護士介入"],
        "TEL携帯.1": "空でない値のみ",
        "入金予定金額_not_in": [2, 3, 5, 12]
        # 注意：with10k版では残債フィルタ・クライアントCDフィルタなし（全件処理）
    }
    
    # マッピングルール
    MAPPING_RULES = {
        "電話番号": "TEL携帯.1",
        "架電番号": "TEL携帯.1", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",
        "物件名": "物件名",
        "クライアント": "クライアント名",
        "残債": "滞納残債"  # J列「残債」にBT列「滞納残債」を格納
    }
    
    OUTPUT_FILE_PREFIX = "ミライル_with10k_保証人"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_mirail_guarantor_with10k_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    ミライル保証人（残債含む）フィルタリング処理
    
    📋 フィルタリング条件 (with10k版):
    - 委託先法人ID: 空白と5
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - クライアントCD: 1のみ（特定クライアントのみ）
    - 残債含む: 全件処理（10,000円・11,000円も含む）
    - TEL携帯.1: 空でない値のみ（保証人電話番号）
    
    ⚠️ without10k版との違い:
    - without10k: 残債10,000円・11,000円を除外
    - with10k: 残債フィルタなし（全件処理）
    """
    logs = []
    original_count = len(df)
    logs.append(f"元データ件数: {original_count}件")
    
    # 📊 フィルタリング条件の適用
    # 1. 委託先法人IDが空白と5
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~(df["委託先法人ID"].isna() | 
                        (df["委託先法人ID"].astype(str).str.strip() == "") | 
                        (df["委託先法人ID"].astype(str).str.strip() == "5"))]
    if len(excluded_data) > 0:
        excluded_counts = excluded_data['委託先法人ID'].value_counts().to_dict()
        excluded_counts_str = {str(k): v for k, v in excluded_counts.items()}
        logs.append(f"委託先法人ID除外詳細: {excluded_counts_str}")
    
    df = df[df["委託先法人ID"].isna() | 
           (df["委託先法人ID"].astype(str).str.strip() == "") | 
           (df["委託先法人ID"].astype(str).str.strip() == "5")]
    logs.append(f"委託先法人ID（空白と5）フィルタ後: {len(df)}件 (除外: {before_filter - len(df)}件)")
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN：当日は除外）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~(df["入金予定日"].isna() | (df["入金予定日"] < today))]
    if len(excluded_data) > 0:
        excluded_dates = excluded_data['入金予定日'].dt.strftime('%Y/%m/%d').value_counts().head(10).to_dict()
        logs.append(f"入金予定日除外詳細（上位10件）: {excluded_dates}")
        if len(excluded_data) > 10:
            logs.append(f"  ※他{len(excluded_data) - 10}件の日付も除外")
    
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(f"入金予定日フィルタ後: {len(df)}件 (除外: {before_filter - len(df)}件)")
    
    # 3. 回収ランクのフィルタリング（弁護士介入案件は除外）
    if "回収ランク_not_in" in MirailGuarantorWith10kConfig.FILTER_CONDITIONS:
        before_filter = len(df)
        # 除外されるデータの詳細を記録
        excluded_data = df[df["回収ランク"].isin(MirailGuarantorWith10kConfig.FILTER_CONDITIONS["回収ランク_not_in"])]
        if len(excluded_data) > 0:
            excluded_ranks = excluded_data['回収ランク'].value_counts().to_dict()
            logs.append(f"回収ランク除外詳細: {excluded_ranks}")
        
        df = df[~df["回収ランク"].isin(MirailGuarantorWith10kConfig.FILTER_CONDITIONS["回収ランク_not_in"])]
        logs.append(f"回収ランクフィルタ後: {len(df)}件 (除外: {before_filter - len(df)}件)")
    
    # 4. 残債・クライアントCDのフィルタリング（with10k版では除外なし - 全件処理）
    # ※ without10k版では残債10,000円・11,000円を除外するが、with10k版は全件処理
    # ※ with10k版ではクライアントCDフィルタも適用しない（全クライアント対象）
    logs.append("残債フィルタ: 除外なし（with10k版：10,000円・11,000円も含む全件処理）")
    logs.append("クライアントCDフィルタ: 除外なし（with10k版：全クライアント対象）")
    
    # 5. 入金予定金額のフィルタリング（2,3,5,12を除外）
    if "入金予定金額_not_in" in MirailGuarantorWith10kConfig.FILTER_CONDITIONS:
        df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
        before_filter = len(df)
        # 除外されるデータの詳細を記録
        excluded_data = df[df["入金予定金額"].isin(MirailGuarantorWith10kConfig.FILTER_CONDITIONS["入金予定金額_not_in"])]
        if len(excluded_data) > 0:
            excluded_amounts = excluded_data['入金予定金額'].value_counts().to_dict()
            excluded_amounts_str = {f"{int(k)}円": v for k, v in excluded_amounts.items() if pd.notna(k)}
            logs.append(f"除外金額詳細: {excluded_amounts_str}")
        
        df = df[df["入金予定金額"].isna() | ~df["入金予定金額"].isin(MirailGuarantorWith10kConfig.FILTER_CONDITIONS["入金予定金額_not_in"])]
        logs.append(f"入金予定金額フィルタ後: {len(df)}件 (除外: {before_filter - len(df)}件)")
    
    # 6. TEL携帯.1のフィルタリング（保証人電話番号が必須）
    if "TEL携帯.1" in MirailGuarantorWith10kConfig.FILTER_CONDITIONS:
        before_filter = len(df)
        # 除外されるデータの詳細を記録
        excluded_data = df[~(df["TEL携帯.1"].notna() &
                            (~df["TEL携帯.1"].astype(str).str.strip().isin(["", "nan", "NaN"])))]
        if len(excluded_data) > 0:
            tel_data = excluded_data['TEL携帯.1'].astype(str).str.strip()
            empty_count = tel_data[tel_data.isin(['', 'nan', 'NaN'])].count()
            fixed_phone_count = len(excluded_data) - empty_count
            logs.append(f"保証人電話除外詳細: {{空白/NaN: {empty_count}件, 固定電話等: {fixed_phone_count}件}}")
        
        df = df[
            df["TEL携帯.1"].notna() &
            (~df["TEL携帯.1"].astype(str).str.strip().isin(["", "nan", "NaN"]))
        ]
        logs.append(f"TEL携帯.1フィルタ後: {len(df)}件 (除外: {before_filter - len(df)}件)")
    
    return df, logs


def create_mirail_guarantor_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """ミライル保証人出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング
    mapping_rules = {
        "電話番号": "TEL携帯.1",
        "架電番号": "TEL携帯.1", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",  # 保証人でも契約者名を入れる
        "物件名": "物件名",
        "クライアント": "クライアント名",
        "残債": "滞納残債"  # J列「残債」にBT列「滞納残債」を格納
    }
    
    # データが0件の場合
    if len(df_filtered) == 0:
        logs.append("保証人出力データ作成完了: 0件（フィルタリング後データなし）")
        return df_output, logs
    
    # データをマッピング
    for i, (_, row) in enumerate(df_filtered.iterrows()):
        for output_col, input_col in mapping_rules.items():
            if output_col in df_output.columns and input_col in row:
                df_output.at[i, output_col] = str(row[input_col]) if pd.notna(row[input_col]) else ""
    
    logs.append(f"保証人出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_mirail_guarantor_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    ミライル保証人（残債含む）データの処理メイン関数
    
    📋 フィルタリング条件 (with10k版):
    - 委託先法人ID: 空白と5
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - クライアントCD: 1のみ（特定クライアントのみ）
    - 残債含む: 全件処理（10,000円・11,000円も含む）
    - TEL携帯.1: 空でない値のみ（保証人電話番号）
    
    ⚠️ without10k版との違い:
    - without10k: 残債10,000円・11,000円を除外
    - with10k: 残債フィルタなし（全件処理）
    
    Args:
        file_content: ContractListのファイル内容（bytes）
        
    Returns:
        tuple: (出力DF, フィルタ済みDF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み（保証人データ）
        logs.append("ファイル読み込み開始")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み完了: {len(df_input)}件")
        
        # 保証人処理に必須な列チェック
        required_columns = ["委託先法人ID", "クライアントCD", "TEL携帯.1", "回収ランク"]
        missing_columns = [col for col in required_columns if col not in df_input.columns]
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 2. 保証人フィルタリング処理
        df_filtered, filter_logs = apply_mirail_guarantor_with10k_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 保証人出力データ作成
        df_output, output_logs = create_mirail_guarantor_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 保証人用出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}ミライル_with10k_保証人.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル保証人（残債含む）データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "クライアント"
    ]
    return pd.DataFrame(columns=columns)