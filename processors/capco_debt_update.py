"""
カプコ残債の更新処理モジュール

このモジュールは、カプコの滞納データ（csv_arrear_*.csv）とContractList_*.csvを統合し、
残債情報を更新するためのCSVファイルを生成します。

処理フロー:
1. csv_arrear_*.csv から「契約No」と「滞納額合計」を抽出
2. ContractList_*.csv から「管理番号」「引継番号」「滞納残債」「クライアントCD」を抽出
3. データのマッチングと統合処理（後続ステップで実装）
4. 「管理番号,管理前滞納額」形式で出力

出力ファイル: MMDDカプコ残債の更新.csv
"""

import pandas as pd
import chardet
from datetime import datetime
import logging
from typing import Tuple, Optional
import io

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定数定義
# csv_arrear_*.csv の必要な列（0ベースのインデックス）
ARREAR_REQUIRED_COLUMNS = {
    'contract_no': 0,      # A列: 契約No
    'total_arrears': 24    # Y列: 滞納額合計
}

# ContractList_*.csv の必要な列（0ベースのインデックス）
CONTRACT_REQUIRED_COLUMNS = {
    'management_no': 0,    # A列: 管理番号
    'takeover_no': 1,      # B列: 引継番号
    'arrears': 71,         # BT列: 滞納残債（72列目なので、0ベースで71）
    'client_cd': 97        # CT列: クライアントCD（98列目なので、0ベースで97）
}

# 出力ヘッダー（絶対に変更しない）
OUTPUT_HEADERS = ['管理番号', '管理前滞納額']

def detect_encoding(file_content: bytes) -> str:
    """ファイルのエンコーディングを自動検出する"""
    # まずバイナリデータとして読み込んでエンコーディングを検出
    detection = chardet.detect(file_content)
    encoding = detection['encoding']
    
    # 信頼度が低い場合は一般的なエンコーディングを試す
    if detection['confidence'] < 0.7:
        encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']
        for enc in encodings:
            try:
                file_content.decode(enc)
                return enc
            except:
                continue
    
    return encoding or 'utf-8'

def read_csv_with_encoding(file_content: bytes, file_name: str) -> pd.DataFrame:
    """エンコーディングを自動判定してCSVを読み込む"""
    try:
        encoding = detect_encoding(file_content)
        logger.info(f"ファイル {file_name} のエンコーディング: {encoding}")
        
        # バイトデータをデコードしてテキストとして読み込む
        text_data = file_content.decode(encoding)
        df = pd.read_csv(io.StringIO(text_data))
        
        return df
    except Exception as e:
        logger.error(f"CSVファイルの読み込みエラー: {str(e)}")
        raise

def extract_arrear_data(df: pd.DataFrame) -> pd.DataFrame:
    """csv_arrear_*.csv から必要な列のみを抽出"""
    try:
        # 列数の確認
        logger.info(f"滞納データの列数: {len(df.columns)}")
        
        # 必要な列のみを抽出（列インデックスで指定）
        required_data = pd.DataFrame()
        
        # 契約No（A列 = インデックス0）
        if len(df.columns) > ARREAR_REQUIRED_COLUMNS['contract_no']:
            required_data['契約No'] = df.iloc[:, ARREAR_REQUIRED_COLUMNS['contract_no']]
        else:
            raise ValueError(f"契約No列（{ARREAR_REQUIRED_COLUMNS['contract_no']+1}列目）が存在しません")
        
        # 滞納額合計（Y列 = インデックス24）
        if len(df.columns) > ARREAR_REQUIRED_COLUMNS['total_arrears']:
            required_data['滞納額合計'] = df.iloc[:, ARREAR_REQUIRED_COLUMNS['total_arrears']]
        else:
            raise ValueError(f"滞納額合計列（{ARREAR_REQUIRED_COLUMNS['total_arrears']+1}列目）が存在しません")
        
        # 契約No列を文字列型に変換
        required_data['契約No'] = required_data['契約No'].astype(str)
        logger.info(f"契約No列のデータ型: {required_data['契約No'].dtype}")
        
        logger.info(f"滞納データから {len(required_data)} 件のデータを抽出しました")
        return required_data
        
    except Exception as e:
        logger.error(f"滞納データの抽出エラー: {str(e)}")
        raise

def extract_contract_data(df: pd.DataFrame) -> pd.DataFrame:
    """ContractList_*.csv から必要な列のみを抽出"""
    try:
        # 列数の確認
        logger.info(f"ContractListの列数: {len(df.columns)}")
        
        # 必要な列のみを抽出（列インデックスで指定）
        required_data = pd.DataFrame()
        
        # 管理番号（A列 = インデックス0）
        if len(df.columns) > CONTRACT_REQUIRED_COLUMNS['management_no']:
            required_data['管理番号'] = df.iloc[:, CONTRACT_REQUIRED_COLUMNS['management_no']]
        else:
            raise ValueError(f"管理番号列（{CONTRACT_REQUIRED_COLUMNS['management_no']+1}列目）が存在しません")
        
        # 引継番号（B列 = インデックス1）
        if len(df.columns) > CONTRACT_REQUIRED_COLUMNS['takeover_no']:
            required_data['引継番号'] = df.iloc[:, CONTRACT_REQUIRED_COLUMNS['takeover_no']]
        else:
            raise ValueError(f"引継番号列（{CONTRACT_REQUIRED_COLUMNS['takeover_no']+1}列目）が存在しません")
        
        # 滞納残債（BT列 = インデックス71）
        if len(df.columns) > CONTRACT_REQUIRED_COLUMNS['arrears']:
            required_data['滞納残債'] = df.iloc[:, CONTRACT_REQUIRED_COLUMNS['arrears']]
        else:
            raise ValueError(f"滞納残債列（{CONTRACT_REQUIRED_COLUMNS['arrears']+1}列目）が存在しません")
        
        # クライアントCD（CT列 = インデックス97）
        if len(df.columns) > CONTRACT_REQUIRED_COLUMNS['client_cd']:
            required_data['クライアントCD'] = df.iloc[:, CONTRACT_REQUIRED_COLUMNS['client_cd']]
        else:
            raise ValueError(f"クライアントCD列（{CONTRACT_REQUIRED_COLUMNS['client_cd']+1}列目）が存在しません")
        
        # 引継番号列を文字列型に変換
        required_data['引継番号'] = required_data['引継番号'].astype(str)
        logger.info(f"引継番号列のデータ型: {required_data['引継番号'].dtype}")
        
        logger.info(f"ContractListから {len(required_data)} 件のデータを抽出しました")
        return required_data
        
    except Exception as e:
        logger.error(f"ContractListデータの抽出エラー: {str(e)}")
        raise

def filter_client_cd(df: pd.DataFrame) -> pd.DataFrame:
    """クライアントCDが1または4のデータのみを抽出"""
    try:
        # クライアントCDを数値型に変換
        df['クライアントCD'] = pd.to_numeric(df['クライアントCD'], errors='coerce')
        
        # クライアントCDが1または4のデータのみを抽出
        filtered_df = df[df['クライアントCD'].isin([1, 4])].copy()
        
        logger.info(f"クライアントCDフィルタリング: {len(df)} -> {len(filtered_df)} 件")
        logger.info(f"除外されたデータ: {len(df) - len(filtered_df)} 件")
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"クライアントCDフィルタリングエラー: {str(e)}")
        raise

def merge_data(contract_data: pd.DataFrame, arrear_data: pd.DataFrame) -> pd.DataFrame:
    """引継番号をキーにデータをマッチング"""
    try:
        logger.info("=== Step 3: データマッチング処理 ===")
        
        # マッチング前のデータ件数
        logger.info(f"マッチング前 - ContractList: {len(contract_data)} 件")
        logger.info(f"マッチング前 - csv_arrear: {len(arrear_data)} 件")
        
        # マージ前の型確認
        logger.info(f"引継番号の型: {contract_data['引継番号'].dtype}")
        logger.info(f"契約Noの型: {arrear_data['契約No'].dtype}")
        
        # 念のため、マージキー列を再度文字列型に統一
        contract_data['引継番号'] = contract_data['引継番号'].astype(str)
        arrear_data['契約No'] = arrear_data['契約No'].astype(str)
        
        logger.info(f"型変換後 - 引継番号の型: {contract_data['引継番号'].dtype}")
        logger.info(f"型変換後 - 契約Noの型: {arrear_data['契約No'].dtype}")
        
        # 引継番号をキーにマッチング（left join）
        merged_df = pd.merge(
            contract_data,
            arrear_data,
            left_on='引継番号',
            right_on='契約No',
            how='left',
            suffixes=('', '_arrear')
        )
        
        # マッチしなかった場合（NaN）を0に変換
        merged_df['滞納額合計'] = merged_df['滞納額合計'].fillna(0)
        
        # 数値型に変換
        merged_df['滞納額合計'] = pd.to_numeric(merged_df['滞納額合計'], errors='coerce').fillna(0)
        merged_df['滞納残債'] = pd.to_numeric(merged_df['滞納残債'], errors='coerce').fillna(0)
        
        # マッチング統計
        matched_count = merged_df['契約No'].notna().sum()
        not_matched_count = merged_df['契約No'].isna().sum()
        
        logger.info(f"マッチング結果:")
        logger.info(f"  - マッチ成功: {matched_count} 件")
        logger.info(f"  - マッチ失敗（完済扱い）: {not_matched_count} 件")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"データマッチングエラー: {str(e)}")
        logger.error(f"エラー詳細: {type(e).__name__}")
        raise

def extract_changed_data(merged_df: pd.DataFrame) -> pd.DataFrame:
    """新旧の残債額が異なるデータのみを抽出"""
    try:
        logger.info("=== Step 4: 差分データ抽出 ===")
        
        # 新旧の残債額が異なるデータのみを抽出
        changed_df = merged_df[merged_df['滞納残債'] != merged_df['滞納額合計']].copy()
        
        logger.info(f"差分抽出結果: {len(merged_df)} -> {len(changed_df)} 件")
        logger.info(f"変更なしで除外: {len(merged_df) - len(changed_df)} 件")
        
        # 詳細統計
        increased = len(changed_df[changed_df['滞納額合計'] > changed_df['滞納残債']])
        decreased = len(changed_df[changed_df['滞納額合計'] < changed_df['滞納残債']])
        
        logger.info(f"  - 残債増加: {increased} 件")
        logger.info(f"  - 残債減少: {decreased} 件")
        
        return changed_df
        
    except Exception as e:
        logger.error(f"差分データ抽出エラー: {str(e)}")
        raise

def create_output_dataframe(changed_df: pd.DataFrame) -> pd.DataFrame:
    """最終出力用のDataFrameを作成"""
    try:
        logger.info("=== Step 5: 出力データ作成 ===")
        
        # 出力用のDataFrameを作成（ヘッダーは厳守）
        output_df = pd.DataFrame({
            OUTPUT_HEADERS[0]: changed_df['管理番号'],  # 管理番号
            OUTPUT_HEADERS[1]: changed_df['滞納額合計']  # 管理前滞納額
        })
        
        # インデックスをリセット
        output_df = output_df.reset_index(drop=True)
        
        logger.info(f"最終出力データ: {len(output_df)} 件")
        
        return output_df
        
    except Exception as e:
        logger.error(f"出力データ作成エラー: {str(e)}")
        raise

def process_capco_debt_update(arrear_file_content: bytes, contract_file_content: bytes) -> Tuple[pd.DataFrame, str]:
    """
    カプコ残債更新処理のメイン関数
    
    Args:
        arrear_file_content: csv_arrear_*.csv のファイル内容
        contract_file_content: ContractList_*.csv のファイル内容
    
    Returns:
        処理結果のDataFrameと出力ファイル名のタプル
    """
    try:
        # Step 1: ファイルを読み込む
        logger.info("=== Step 1: ファイル読み込み開始 ===")
        arrear_df = read_csv_with_encoding(arrear_file_content, "csv_arrear_*.csv")
        contract_df = read_csv_with_encoding(contract_file_content, "ContractList_*.csv")
        
        # Step 2: 必要な列のみを抽出
        logger.info("=== Step 2: 必要な列の抽出 ===")
        arrear_data = extract_arrear_data(arrear_df)
        contract_data = extract_contract_data(contract_df)
        
        # Step 2.5: クライアントCDでフィルタリング
        logger.info("=== Step 2.5: クライアントCDフィルタリング ===")
        contract_filtered = filter_client_cd(contract_data)
        
        if len(contract_filtered) == 0:
            logger.warning("クライアントCD=1,4のデータが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新.csv"
            return empty_df, output_filename
        
        # Step 3: データマッチング
        merged_df = merge_data(contract_filtered, arrear_data)
        
        # Step 4: 差分データ抽出
        changed_df = extract_changed_data(merged_df)
        
        if len(changed_df) == 0:
            logger.warning("更新が必要なデータが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新.csv"
            return empty_df, output_filename
        
        # Step 5: 最終出力データ作成
        result_df = create_output_dataframe(changed_df)
        
        # 出力ファイル名の生成
        timestamp = datetime.now().strftime("%m%d")
        output_filename = f"{timestamp}カプコ残債の更新.csv"
        
        logger.info("=== 処理完了 ===")
        logger.info(f"出力ファイル名: {output_filename}")
        
        return result_df, output_filename
        
    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {str(e)}")
        raise