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
from typing import Tuple, Optional, Dict, List
import io
import unicodedata
from processors.common.detailed_logger import DetailedLogger

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

# メモリ最適化：読み込み対象列のリスト
ARREAR_USECOLS = [0, 24]  # 契約No(A列), 滞納額合計(Y列)
CONTRACT_USECOLS = [0, 1, 71, 97]  # 管理番号(A列), 引継番号(B列), 滞納残債(BT列), クライアントCD(CT列)

# 出力ヘッダー（絶対に変更しない）
OUTPUT_HEADERS = ['管理番号', '管理前滞納額']

def clean_amount(x) -> Optional[str]:
    """
    金額文字列のクレンジング処理
    
    Args:
        x: 金額を表す可能性のある値
        
    Returns:
        クレンジング済み文字列（数値変換可能な形式）またはNone
        
    Examples:
        "¥12,345" -> "12345"
        "￥1,000円" -> "1000"
        "(500)" -> "-500"
        "12３45" -> "12345"  # 全角→半角
    """
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return None
    
    # 全角→半角正規化
    s = unicodedata.normalize("NFKC", s)
    
    # 通貨記号・カンマ・単位除去
    s = s.replace("¥", "").replace("￥", "").replace("円", "").replace(",", "")
    
    # 括弧表記の負数処理 (123) → -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    
    return s

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

def read_csv_with_encoding(file_content: bytes, file_name: str, usecols: Optional[list] = None) -> pd.DataFrame:
    """エンコーディングを自動判定してCSVを読み込む"""
    try:
        encoding = detect_encoding(file_content)
        logger.info(f"ファイル {file_name} のエンコーディング: {encoding}")
        
        # バイトデータをデコードしてテキストとして読み込む
        text_data = file_content.decode(encoding)
        
        # usecolsパラメータがある場合は必要な列のみ読み込み（dtype=strで先頭ゼロを保持）
        if usecols is not None:
            df = pd.read_csv(io.StringIO(text_data), usecols=usecols, dtype=str)
            logger.info(f"メモリ最適化: {len(usecols)} 列のみ読み込み（全列数は不明）")
        else:
            df = pd.read_csv(io.StringIO(text_data), dtype=str)
            logger.info(f"全列読み込み: {len(df.columns)} 列")
        
        return df
    except Exception as e:
        logger.error(f"CSVファイルの読み込みエラー: {str(e)}")
        raise

def extract_arrear_data(df: pd.DataFrame) -> pd.DataFrame:
    """csv_arrear_*.csv から必要な列のみを抽出（メモリ最適化版：既に必要列のみ読み込み済み）"""
    try:
        # usecolsで読み込んだため、列数は必要最小限（2列）
        logger.info(f"滞納データの読み込み列数: {len(df.columns)} 列（メモリ最適化済み）")
        
        # usecolsで読み込んだ場合、列番号は0, 1となる
        if len(df.columns) != 2:
            raise ValueError(f"期待される列数: 2列、実際の列数: {len(df.columns)}列")
        
        # 必要な列のみを抽出（usecolsで読み込んだ順序：0=契約No, 1=滞納額合計）
        required_data = pd.DataFrame()
        
        # 契約No（読み込み後の1列目 = インデックス0）
        required_data['契約No'] = df.iloc[:, 0]
        # 文字列型に変換
        required_data['契約No'] = required_data['契約No'].astype(str)
        # 'nan'文字列を除外
        required_data = required_data[required_data['契約No'] != 'nan']
        logger.info(f"契約No列のデータ型: {required_data['契約No'].dtype}")
        
        # 滞納額合計（読み込み後の2列目 = インデックス1）
        required_data['滞納額合計'] = df.iloc[:, 1]
        
        # 契約Noの重複削除（最後の行を保持）
        before_count = len(required_data)
        required_data = required_data.drop_duplicates(subset=['契約No'], keep='last')
        after_count = len(required_data)
        
        logger.info(f"重複削除: {before_count} -> {after_count} 件 (削除: {before_count - after_count} 件)")
        logger.info(f"滞納データから {after_count} 件のデータを抽出しました")
        return required_data
        
    except Exception as e:
        logger.error(f"滞納データの抽出エラー: {str(e)}")
        raise

def extract_contract_data(df: pd.DataFrame) -> pd.DataFrame:
    """ContractList_*.csv から必要な列のみを抽出（メモリ最適化版：既に必要列のみ読み込み済み）"""
    try:
        # usecolsで読み込んだため、列数は必要最小限（4列）
        logger.info(f"ContractListの読み込み列数: {len(df.columns)} 列（メモリ最適化済み）")
        
        # usecolsで読み込んだ場合、列番号は0, 1, 2, 3となる
        if len(df.columns) != 4:
            raise ValueError(f"期待される列数: 4列、実際の列数: {len(df.columns)}列")
        
        # 必要な列のみを抽出（usecolsで読み込んだ順序：0=管理番号, 1=引継番号, 2=滞納残債, 3=クライアントCD）
        required_data = pd.DataFrame()
        
        # 管理番号（読み込み後の1列目 = インデックス0）
        required_data['管理番号'] = df.iloc[:, 0]
        
        # 引継番号（読み込み後の2列目 = インデックス1）
        required_data['引継番号'] = df.iloc[:, 1]
        # 文字列型に変換
        required_data['引継番号'] = required_data['引継番号'].astype(str)
        # 'nan'文字列を除外
        required_data = required_data[required_data['引継番号'] != 'nan']
        logger.info(f"引継番号列のデータ型: {required_data['引継番号'].dtype}")
        
        # 滞納残債（読み込み後の3列目 = インデックス2）
        required_data['滞納残債'] = df.iloc[:, 2]
        
        # クライアントCD（読み込み後の4列目 = インデックス3）
        required_data['クライアントCD'] = df.iloc[:, 3]
        
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
        
        # 念のため、マージキー列を再度文字列型に統一し、引継番号の.0を除去
        contract_data['引継番号'] = contract_data['引継番号'].astype(str).str.replace('.0', '', regex=False)
        arrear_data['契約No'] = arrear_data['契約No'].astype(str)
        
        # 空文字列や'nan'を除外（既に処理済みですが念のため）
        contract_data = contract_data[contract_data['引継番号'].str.strip() != '']
        arrear_data = arrear_data[arrear_data['契約No'].str.strip() != '']
        
        logger.info(f"型変換後 - 引継番号の型: {contract_data['引継番号'].dtype}")
        logger.info(f"型変換後 - 契約Noの型: {arrear_data['契約No'].dtype}")
        
        # === 診断コード: 実際のデータ確認 ===
        logger.info("=== 実際のデータ確認 ===")
        logger.info(f"引継番号サンプル（10件）: {contract_data['引継番号'].head(10).tolist()}")
        logger.info(f"引継番号の実際の型: {type(contract_data['引継番号'].iloc[0])}")
        logger.info(f"契約Noサンプル（10件）: {arrear_data['契約No'].head(10).tolist()}")  
        logger.info(f"契約Noの実際の型: {type(arrear_data['契約No'].iloc[0])}")

        # 実際に共通する値があるか確認
        contract_set = set(contract_data['引継番号'])
        arrear_set = set(arrear_data['契約No'])
        common_values = contract_set & arrear_set
        logger.info(f"共通する値の数: {len(common_values)}")
        if common_values:
            logger.info(f"共通する値（例）: {sorted(list(common_values))[:10]}")
        else:
            logger.info("共通する値が存在しません")
            # サンプル比較
            logger.info(f"引継番号の例（ソート済み最初の5件）: {sorted(list(contract_set))[:5]}")
            logger.info(f"契約Noの例（ソート済み最初の5件）: {sorted(list(arrear_set))[:5]}")
        
        # 引継番号をキーにマッチング（left join）
        merged_df = pd.merge(
            contract_data,
            arrear_data,
            left_on='引継番号',
            right_on='契約No',
            how='left',
            suffixes=('', '_arrear')
        )
        
        # マッチフラグを追加（未マッチレコードの識別用）
        merged_df['is_matched'] = merged_df['契約No'].notna()
        
        # 金額クレンジング＆数値化（0埋めなし - 差分判定まではNaNのまま保持）
        mask = merged_df['is_matched']
        merged_df.loc[mask, '滞納額合計_num'] = pd.to_numeric(
            merged_df.loc[mask, '滞納額合計'].map(clean_amount),
            errors='coerce'
        )
        merged_df['滞納残債_num'] = pd.to_numeric(
            merged_df['滞納残債'].map(clean_amount),
            errors='coerce'
        )
        
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
    """新旧の残債額が異なるデータのみを抽出（完済レコードも含む）"""
    try:
        logger.info("=== Step 4: 差分データ抽出（完済レコード含む） ===")
        
        # 1. マッチしたレコードの差分抽出（既存ロジック）
        valid = (
            merged_df['is_matched'] &
            merged_df['滞納額合計_num'].notna() &
            merged_df['滞納残債_num'].notna()
        )
        
        logger.info(f"マッチ済み: {merged_df['is_matched'].sum()} 件")
        logger.info(f"滞納額合計_num有効: {merged_df['滞納額合計_num'].notna().sum()} 件")
        logger.info(f"滞納残債_num有効: {merged_df['滞納残債_num'].notna().sum()} 件")
        logger.info(f"両方有効（差分判定対象）: {valid.sum()} 件")
        
        # マッチして差分があるデータ
        matched_changed = merged_df.loc[
            valid & (merged_df['滞納残債_num'] != merged_df['滞納額合計_num'])
        ].copy()
        
        logger.info(f"マッチして差分あり: {len(matched_changed)} 件")
        
        # 2. 未マッチレコードの完済判定（新規追加）
        unmatched = merged_df[~merged_df['is_matched']].copy()
        
        # 滞納残債 > 0 の未マッチレコードは完済として処理
        completed_payments = unmatched[
            (unmatched['滞納残債_num'] > 0) & 
            unmatched['滞納残債_num'].notna()
        ].copy()
        
        if len(completed_payments) > 0:
            # 完済レコードは滞納額合計を0に設定
            completed_payments['滞納額合計_num'] = 0
            completed_payments['__delta__'] = -completed_payments['滞納残債_num']
            logger.info(f"完済として処理: {len(completed_payments)} 件")
        else:
            logger.info("完済として処理: 0 件")
        
        # 3. 両方を結合
        if len(completed_payments) > 0:
            changed_df = pd.concat([matched_changed, completed_payments], ignore_index=True)
        else:
            changed_df = matched_changed
        
        logger.info(f"差分抽出結果（完済含む）: {len(merged_df)} -> {len(changed_df)} 件")
        logger.info(f"  - マッチして差分あり: {len(matched_changed)} 件")
        logger.info(f"  - 未マッチで完済扱い: {len(completed_payments)} 件")
        
        # 詳細統計（増減内訳）
        if len(changed_df) > 0:
            if '__delta__' not in changed_df.columns:
                changed_df['__delta__'] = changed_df['滞納額合計_num'] - changed_df['滞納残債_num']
            
            increased = int((changed_df['__delta__'] > 0).sum())  # 新 > 旧
            decreased = int((changed_df['__delta__'] < 0).sum())  # 新 < 旧
            
            logger.info(f"  - 残債増加: {increased} 件")
            logger.info(f"  - 残債減少（完済含む）: {decreased} 件")
        else:
            logger.info("  - 残債増加: 0 件")
            logger.info("  - 残債減少: 0 件")
        
        return changed_df
        
    except Exception as e:
        logger.error(f"差分データ抽出エラー: {str(e)}")
        raise

def create_output_dataframe(changed_df: pd.DataFrame) -> pd.DataFrame:
    """最終出力用のDataFrameを作成（出力直前のみ0埋め実行）"""
    try:
        logger.info("=== Step 5: 出力データ作成 ===")
        
        # 出力用のDataFrameを作成（ヘッダーは厳守）
        output_df = pd.DataFrame({
            OUTPUT_HEADERS[0]: changed_df['管理番号'],  # 管理番号
            # 出力直前のみ0埋め（差分判定が完了してから）
            OUTPUT_HEADERS[1]: changed_df['滞納額合計_num'].fillna(0).round(0).astype('Int64')  # 管理前滞納額
        })
        
        # インデックスをリセット
        output_df = output_df.reset_index(drop=True)
        
        logger.info(f"最終出力データ: {len(output_df)} 件")
        
        return output_df
        
    except Exception as e:
        logger.error(f"出力データ作成エラー: {str(e)}")
        raise

def process_capco_debt_update(arrear_file_content: bytes, contract_file_content: bytes, progress_callback=None) -> Tuple[pd.DataFrame, str, Dict[str, any], List[str]]:
    """
    カプコ残債更新処理のメイン関数
    
    Args:
        arrear_file_content: csv_arrear_*.csv のファイル内容
        contract_file_content: ContractList_*.csv のファイル内容
        progress_callback: 進捗を更新するコールバック関数
    
    Returns:
        処理結果のDataFrame、出力ファイル名、処理統計情報、ログリストのタプル
    """
    try:
        # 統計情報を保存する辞書
        stats = {}
        logs = []
        
        # Step 1: ファイルを読み込む（メモリ最適化：必要な列のみ）
        logger.info("=== Step 1: ファイル読み込み開始 ===")
        if progress_callback:
            progress_callback(0.1, "Step 1/5: ファイル読み込み中...")
        
        # 必要な列のみを読み込んでメモリ使用量を大幅削減
        arrear_df = read_csv_with_encoding(arrear_file_content, "csv_arrear_*.csv", usecols=ARREAR_USECOLS)
        contract_df = read_csv_with_encoding(contract_file_content, "ContractList_*.csv", usecols=CONTRACT_USECOLS)
        stats['arrear_total_rows'] = len(arrear_df)
        stats['contract_total_rows'] = len(contract_df)
        
        # 初期データ読み込みログ
        logs.append(DetailedLogger.log_initial_load(stats['contract_total_rows']))
        stats['arrear_columns_optimized'] = len(ARREAR_USECOLS)
        stats['contract_columns_optimized'] = len(CONTRACT_USECOLS)
        
        logger.info(f"メモリ最適化完了 - 滞納データ: {stats['arrear_columns_optimized']}列読み込み")
        logger.info(f"メモリ最適化完了 - ContractList: {stats['contract_columns_optimized']}列読み込み")
        
        # Step 2: 必要な列のみを抽出
        logger.info("=== Step 2: 必要な列の抽出 ===")
        if progress_callback:
            progress_callback(0.3, "Step 2/5: データ抽出処理中...")
        
        arrear_data = extract_arrear_data(arrear_df)
        contract_data = extract_contract_data(contract_df)
        stats['arrear_columns'] = len(arrear_df.columns)  # メモリ最適化後の列数（2列）
        stats['arrear_unique_before'] = len(arrear_df)
        stats['arrear_unique_after'] = len(arrear_data)
        stats['arrear_duplicates_removed'] = stats['arrear_unique_before'] - stats['arrear_unique_after']
        stats['contract_extracted'] = len(contract_data)
        stats['contract_columns'] = len(contract_df.columns)  # メモリ最適化後の列数（4列）
        
        # Step 2.5: クライアントCDでフィルタリング
        logger.info("=== Step 2.5: クライアントCDフィルタリング ===")
        if progress_callback:
            progress_callback(0.5, "Step 2.5/5: フィルタリング処理中...")
        
        contract_filtered = filter_client_cd(contract_data)
        stats['client_cd_before'] = len(contract_data)
        stats['client_cd_after'] = len(contract_filtered)
        stats['client_cd_excluded'] = stats['client_cd_before'] - stats['client_cd_after']
        
        # クライアントCDフィルタログ
        logs.append(DetailedLogger.log_filter_result(
            stats['client_cd_before'], stats['client_cd_after'], 'クライアントCD'
        ))
        
        # 除外されたクライアントCDの詳細
        excluded_client_cd = contract_data[~contract_data['管理番号'].isin(contract_filtered['管理番号'])]
        if len(excluded_client_cd) > 0:
            client_cd_col = contract_data.columns.get_loc('クライアントCD')
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_client_cd, client_cd_col, 'クライアントCD', 'category'
            )
            if detail_log:
                logs.append(detail_log)
        
        if len(contract_filtered) == 0:
            logger.warning("クライアントCD=1,4のデータが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新.csv"
            return empty_df, output_filename, stats, logs
        
        # Step 3: データマッチング
        if progress_callback:
            progress_callback(0.7, "Step 3/5: データマッチング処理中...")
        
        merged_df = merge_data(contract_filtered, arrear_data)
        stats['match_contract_count'] = len(contract_filtered)
        stats['match_arrear_count'] = len(arrear_data)
        stats['match_success'] = merged_df['契約No'].notna().sum()
        stats['match_failed'] = merged_df['契約No'].isna().sum()
        
        # Step 4: 差分データ抽出
        if progress_callback:
            progress_callback(0.85, "Step 4/5: 差分データ抽出中...")
        
        changed_df = extract_changed_data(merged_df)
        stats['diff_total'] = len(merged_df)
        stats['diff_matched'] = merged_df['is_matched'].sum()
        stats['diff_valid'] = (merged_df['is_matched'] & merged_df['滞納額合計_num'].notna() & merged_df['滞納残債_num'].notna()).sum()
        stats['diff_changed'] = len(changed_df)
        stats['diff_parse_failed'] = stats['diff_matched'] - stats['diff_valid']
        
        # 差分データ抽出ログ
        logs.append(DetailedLogger.log_filter_result(
            stats['diff_total'], stats['diff_changed'], '差分抽出'
        ))
        
        # 完済件数を追加
        unmatched = merged_df[~merged_df['is_matched']]
        completed_count = len(unmatched[(unmatched['滞納残債_num'] > 0) & unmatched['滞納残債_num'].notna()])
        stats['diff_completed'] = completed_count
        stats['diff_unchanged'] = stats['diff_valid'] - (stats['diff_changed'] - completed_count)
        
        # 増減統計（_num列を使用）
        if len(changed_df) > 0 and '__delta__' in changed_df.columns:
            stats['diff_increased'] = int((changed_df['__delta__'] > 0).sum())
            stats['diff_decreased'] = int((changed_df['__delta__'] < 0).sum())
        else:
            stats['diff_increased'] = 0
            stats['diff_decreased'] = 0
        
        if len(changed_df) == 0:
            logger.warning("更新が必要なデータが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新.csv"
            return empty_df, output_filename, stats, logs
        
        # Step 5: 最終出力データ作成
        if progress_callback:
            progress_callback(0.95, "Step 5/5: 出力データ作成中...")
        
        result_df = create_output_dataframe(changed_df)
        stats['output_count'] = len(result_df)
        
        # 最終結果ログ
        logs.append(DetailedLogger.log_final_result(stats['output_count']))
        
        # 出力ファイル名の生成
        timestamp = datetime.now().strftime("%m%d")
        output_filename = f"{timestamp}カプコ残債の更新.csv"
        
        logger.info("=== 処理完了 ===")
        logger.info(f"出力ファイル名: {output_filename}")
        
        return result_df, output_filename, stats, logs
        
    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {str(e)}")
        raise