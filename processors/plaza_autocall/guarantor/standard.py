"""
プラザ保証人オートコール処理プロセッサー
統合アプリ用に実装
注意：プラザ処理は2つのファイル（ContractList + Excel報告書）が必要です
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_plaza_guarantor_filters(df_contract: pd.DataFrame, df_report: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], str]:
    """プラザ保証人フィルタリング処理（ContractList + Excel報告書の結合処理）"""
    logs = []
    original_count = len(df_contract)
    logs.append(f"元データ件数（ContractList）: {original_count}件")
    logs.append(f"報告書件数: {len(df_report)}件")
    
    # ContractListの管理番号を会員番号に変換（必要に応じて）
    if "会員番号" not in df_contract.columns and "管理番号" in df_contract.columns:
        df_contract = df_contract.rename(columns={"管理番号": "会員番号"})
    
    # 会員番号で結合
    merged = df_contract.merge(
        df_report[["会員番号", "延滞額合計", "緊急連絡人　電話番号"]],
        on="会員番号",
        how="left"
    )
    logs.append(f"結合後件数: {len(merged)}件")
    
    # 1. 延滞額合計フィルター（0円、2円、3円、5円を除外）
    drop_values = ["0", "2", "3", "5"]
    merged = merged[~merged["延滞額合計"].isin(drop_values)]
    logs.append(f"延滞額フィルタ後: {len(merged)}件")
    
    # 2. TEL無効を除外
    merged = merged[~merged["緊急連絡人　電話番号"].str.contains("TEL無効", na=False)]
    logs.append(f"TEL無効除外後: {len(merged)}件")
    
    # 3. 回収ランクフィルター（督促停止、弁護士介入を除外）
    if "回収ランク" in merged.columns:
        merged = merged[~merged["回収ランク"].isin(["督促停止", "弁護士介入"])]
        logs.append(f"回収ランクフィルタ後: {len(merged)}件")
    
    # 4. 保証人TEL携帯のフィルタリング（保証人の電話番号が必須）
    # 保証人関連の電話番号列を検索（TEL携帯.1、TEL携帯.2なども含む）
    guarantor_tel_columns = []
    
    # まず保証人関連の電話番号列を探す
    for col in merged.columns:
        if "保証人" in col and ("TEL" in col or "電話" in col):
            guarantor_tel_columns.append(col)
    
    # 保証人専用列がない場合は、TEL携帯.1、TEL携帯.2を探す（保証人の電話番号として使用）
    if not guarantor_tel_columns:
        for col in merged.columns:
            if col in ["TEL携帯.1", "TEL携帯.2"] or col.startswith("保証人TEL"):
                guarantor_tel_columns.append(col)
    
    if not guarantor_tel_columns:
        raise ValueError("保証人電話番号列が見つかりません。利用可能な列: " + str(merged.columns.tolist()))
    
    tel_column = guarantor_tel_columns[0]  # 1つ目の列を使用
    
    # 電話番号の整形と空値除外
    merged[tel_column] = (
        merged[tel_column]
        .fillna("")
        .astype(str)
        .apply(lambda x: x if x.startswith("0") else f"0{x}" if x else "")
    )
    merged = merged[merged[tel_column].str.strip() != ""]
    logs.append(f"保証人TEL携帯フィルタ後（{tel_column}使用）: {len(merged)}件")
    
    return merged, logs, tel_column


def create_plaza_guarantor_output(df_filtered: pd.DataFrame, tel_column: str) -> Tuple[pd.DataFrame, List[str]]:
    """プラザ保証人出力データ作成"""
    logs = []
    
    # 電話番号にダブルクォーテーションを追加（Excel対策）
    tel_series = df_filtered[tel_column].apply(lambda x: f'"{x}"' if x else "")
    
    # データが0件の場合でも列構造を維持
    output_columns = [
        "電話番号", "架電番号", "管理番号", "保証人名（カナ）", "契約者名（カナ）",
        "物件名", "クライアント", "入居ステータス", "滞納ステータス"
    ]
    
    if len(df_filtered) == 0:
        df_output = pd.DataFrame(columns=output_columns)
        logs.append("保証人出力データ作成完了: 0件（フィルタリング後データなし）")
        return df_output, logs
    
    # プラザ保証人用の出力データ作成
    output_data = []
    for i, (_, row) in enumerate(df_filtered.iterrows()):
        # 保証人名を取得（複数の可能性のある列名を確認）
        guarantor_name = ""
        guarantor_name_columns = ["保証人名", "保証人氏名", "保証人", "保証人名（カナ）", "保証人カナ", "保証人１氏名"]
        for col in guarantor_name_columns:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                guarantor_name = str(row[col])
                break
        
        output_row = {
            "電話番号": tel_series.iloc[i],
            "架電番号": tel_series.iloc[i],
            "管理番号": str(row.get("会員番号", "")),
            "保証人名（カナ）": guarantor_name,
            "契約者名（カナ）": str(row.get("契約者カナ", "")),
            "物件名": str(row.get("物件名", "")),
            "クライアント": "プラザ賃貸管理保証",
            "入居ステータス": "入居中",
            "滞納ステータス": "未精算"
        }
        output_data.append(output_row)
    
    df_output = pd.DataFrame(output_data)
    logs.append(f"保証人出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_plaza_guarantor_data(contract_content: bytes, report_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    プラザ保証人データの処理メイン関数
    
    Args:
        contract_content: ContractListのファイル内容
        report_content: Excel報告書のファイル内容
        
    Returns:
        tuple: (フィルタ済みDF, 出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み
        logs.append("ContractList読み込み開始")
        df_contract = read_csv_auto_encoding(contract_content)
        logs.append(f"ContractList読み込み完了: {len(df_contract)}件")
        
        # 2. Excel報告書読み込み
        logs.append("Excel報告書読み込み開始")
        df_report = pd.read_excel(io.BytesIO(report_content), dtype=str)
        logs.append(f"Excel報告書読み込み完了: {len(df_report)}件")
        
        # 必須列チェック
        required_contract_columns = ["会員番号", "管理番号"]  # どちらかがあればOK
        required_report_columns = ["会員番号", "延滞額合計"]
        
        contract_has_key = any(col in df_contract.columns for col in required_contract_columns)
        if not contract_has_key:
            raise ValueError(f"ContractListに必須列がありません: {required_contract_columns}")
        
        missing_report_columns = [col for col in required_report_columns if col not in df_report.columns]
        if missing_report_columns:
            raise ValueError(f"Excel報告書に必須列が不足しています: {missing_report_columns}")
        
        # 3. フィルタリング処理
        df_filtered, filter_logs, tel_column = apply_plaza_guarantor_filters(df_contract, df_report)
        logs.extend(filter_logs)
        
        # 4. 出力データ作成
        df_output, output_logs = create_plaza_guarantor_output(df_filtered, tel_column)
        logs.extend(output_logs)
        
        # 5. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}プラザ_保証人.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"プラザ保証人データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "管理番号", "保証人名（カナ）", "契約者名（カナ）",
        "物件名", "クライアント", "入居ステータス", "滞納ステータス"
    ]
    return pd.DataFrame(columns=columns)