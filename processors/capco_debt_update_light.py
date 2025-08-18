"""
カプコ残債の更新処理モジュール（軽量版）

このモジュールは手動前処理済みの軽量CSVファイルを使用して、
カプコの残債情報を更新するためのCSVファイルを生成します。

【前提条件】ユーザーが手動で以下の前処理を完了済み：
1. csv_arrear_*.csv から「契約No」と「滞納額合計」のみ抽出
2. ContractList_*.csv から「管理番号」「引継番号」「滞納残債」「クライアントCD」のみ抽出
3. クライアントCD=1,4のデータのみに絞り込み

【軽量版処理フロー】
1. 前処理済み滞納データ読み込み（契約No, 滞納額合計）
2. 前処理済み契約データ読み込み（管理番号, 引継番号, 滞納残債, クライアントCD）
3. データマッチング（引継番号 = 契約No）
4. 差分抽出（新旧残債額が異なるデータのみ）
5. 出力ファイル生成（管理番号, 管理前滞納額）

出力ファイル: MMDDカプコ残債の更新_軽量版.csv
"""

import pandas as pd
import chardet
from datetime import datetime
import logging
from typing import Tuple, Optional, Dict
import io

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 期待される列名（手動前処理済みファイル用）
ARREAR_EXPECTED_COLUMNS = ['契約No', '滞納額合計']
CONTRACT_EXPECTED_COLUMNS = ['管理番号', '引継番号', '滞納残債', 'クライアントCD']

# 出力ヘッダー（既存版と同じ）
OUTPUT_HEADERS = ['管理番号', '管理前滞納額']

def detect_encoding(file_content: bytes) -> str:
    """ファイルのエンコーディングを自動検出する（改善版）"""
    # まずバイナリデータとして読み込んでエンコーディングを検出
    detection = chardet.detect(file_content)
    detected_encoding = detection['encoding']
    confidence = detection['confidence']
    
    logger.info(f"エンコーディング自動検出: {detected_encoding} (信頼度: {confidence:.2f})")
    
    # 日本語CSVでよく使われるエンコーディングを優先的に試行
    # 信頼度が低い場合や、より確実にするために全て試行
    encodings_to_try = [
        'cp932',        # Windows標準（Excel標準保存）
        'shift_jis',    # Shift_JIS
        'utf-8-sig',    # UTF-8 with BOM
        'utf-8',        # UTF-8
        'iso-2022-jp',  # JIS
        'euc-jp'        # EUC-JP
    ]
    
    # 検出されたエンコーディングがリストにある場合は最優先にする
    if detected_encoding and detected_encoding.lower() in [enc.lower() for enc in encodings_to_try]:
        encodings_to_try.remove(detected_encoding)
        encodings_to_try.insert(0, detected_encoding)
    
    # 各エンコーディングを順番に試行
    for enc in encodings_to_try:
        try:
            decoded_text = file_content.decode(enc)
            # 日本語文字が含まれているかチェック（列名が日本語の場合）
            if '契約' in decoded_text or '滞納' in decoded_text or 'No' in decoded_text:
                logger.info(f"エンコーディング確定: {enc} (日本語検出)")
                return enc
            # 日本語がない場合でも正常にデコードできれば採用
            logger.info(f"エンコーディング確定: {enc} (デコード成功)")
            return enc
        except (UnicodeDecodeError, LookupError):
            logger.debug(f"エンコーディング {enc} でのデコードに失敗")
            continue
    
    # すべて失敗した場合はUTF-8をデフォルトとする
    logger.warning(f"エンコーディング検出に失敗、UTF-8を使用")
    return 'utf-8'

def clean_column_name(column_name: str) -> str:
    """列名をクリーニングする（BOM除去、空白除去等）"""
    if not isinstance(column_name, str):
        column_name = str(column_name)
    
    # BOM（Byte Order Mark）を除去
    if column_name.startswith('\ufeff'):
        column_name = column_name[1:]
    
    # 前後の空白を除去
    column_name = column_name.strip()
    
    # 全角スペースも除去
    column_name = column_name.replace('\u3000', '')
    
    return column_name

def find_matching_column(target_column: str, actual_columns: list) -> str:
    """期待される列名に対応する実際の列名を見つける（柔軟照合）"""
    target_cleaned = clean_column_name(target_column)
    
    for actual_col in actual_columns:
        actual_cleaned = clean_column_name(actual_col)
        
        # 完全一致
        if target_cleaned == actual_cleaned:
            return actual_col
        
        # 大文字小文字を無視した一致
        if target_cleaned.lower() == actual_cleaned.lower():
            return actual_col
        
        # 部分一致チェック（主要キーワードが含まれている）
        if target_column == '契約No':
            if '契約' in actual_cleaned and ('No' in actual_cleaned or 'no' in actual_cleaned or 'NO' in actual_cleaned or '番号' in actual_cleaned):
                return actual_col
        elif target_column == '滞納額合計':
            if '滞納' in actual_cleaned and ('合計' in actual_cleaned or '額' in actual_cleaned):
                return actual_col
        elif target_column == '管理番号':
            if '管理' in actual_cleaned and '番号' in actual_cleaned:
                return actual_col
        elif target_column == '引継番号':
            if '引継' in actual_cleaned and '番号' in actual_cleaned:
                return actual_col
        elif target_column == '滞納残債':
            if '滞納' in actual_cleaned and '残債' in actual_cleaned:
                return actual_col
        elif target_column == 'クライアントCD':
            if 'クライアント' in actual_cleaned and ('CD' in actual_cleaned or 'cd' in actual_cleaned):
                return actual_col
    
    return None

def read_csv_with_encoding(file_content: bytes, file_name: str, expected_columns: list) -> pd.DataFrame:
    """エンコーディングを自動判定してCSVを読み込む（軽量版・改善版）"""
    try:
        encoding = detect_encoding(file_content)
        logger.info(f"ファイル {file_name} のエンコーディング: {encoding}")
        
        # バイトデータをデコードしてテキストとして読み込む
        text_data = file_content.decode(encoding)
        df = pd.read_csv(io.StringIO(text_data))
        
        logger.info(f"軽量版読み込み完了: {len(df)} 行, {len(df.columns)} 列")
        
        # 実際の列名をログ出力（デバッグ用）
        actual_columns = [clean_column_name(col) for col in df.columns]
        logger.info(f"実際の列名（クリーニング後）: {actual_columns}")
        logger.info(f"期待される列名: {expected_columns}")
        
        # 列名マッピングを作成
        column_mapping = {}
        missing_columns = []
        
        for expected_col in expected_columns:
            matching_col = find_matching_column(expected_col, df.columns.tolist())
            if matching_col is not None:
                column_mapping[matching_col] = expected_col
                logger.info(f"列名マッピング: '{matching_col}' → '{expected_col}'")
            else:
                missing_columns.append(expected_col)
        
        if missing_columns:
            logger.error(f"必要な列が見つかりません: {missing_columns}")
            logger.error(f"実際の列名: {list(df.columns)}")
            logger.error(f"クリーニング後の列名: {actual_columns}")
            
            # 類似した列名の提案
            for missing_col in missing_columns:
                suggestions = []
                for actual_col in df.columns:
                    actual_cleaned = clean_column_name(actual_col)
                    if missing_col in actual_cleaned or actual_cleaned in missing_col:
                        suggestions.append(actual_col)
                if suggestions:
                    logger.info(f"'{missing_col}' に類似した列名: {suggestions}")
            
            raise ValueError(f"必要な列が見つかりません: {missing_columns}")
        
        # 列名を標準化してDataFrameを作成
        df_renamed = df.rename(columns=column_mapping)
        df_filtered = df_renamed[expected_columns].copy()
        
        logger.info(f"必要列抽出完了: {expected_columns}")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"CSVファイルの読み込みエラー: {str(e)}")
        raise

def validate_arrear_data(df: pd.DataFrame) -> pd.DataFrame:
    """滞納データの検証とクリーニング"""
    try:
        logger.info("=== 滞納データの検証 ===")
        
        # データ型の確認と変換
        df['契約No'] = df['契約No'].astype(str)
        df['滞納額合計'] = pd.to_numeric(df['滞納額合計'], errors='coerce').fillna(0)
        
        # 空値・NaN値の除外
        before_count = len(df)
        df = df[df['契約No'].str.strip() != '']
        df = df[df['契約No'] != 'nan']
        after_count = len(df)
        
        logger.info(f"無効データ除外: {before_count} -> {after_count} 件")
        
        # 重複削除（最後の行を保持）
        before_dup = len(df)
        df = df.drop_duplicates(subset=['契約No'], keep='last')
        after_dup = len(df)
        
        logger.info(f"重複削除: {before_dup} -> {after_dup} 件 (削除: {before_dup - after_dup} 件)")
        logger.info(f"滞納データ検証完了: {after_dup} 件")
        
        return df
        
    except Exception as e:
        logger.error(f"滞納データの検証エラー: {str(e)}")
        raise

def validate_contract_data(df: pd.DataFrame) -> pd.DataFrame:
    """契約データの検証とクリーニング"""
    try:
        logger.info("=== 契約データの検証 ===")
        
        # データ型の確認と変換
        df['引継番号'] = df['引継番号'].astype(str)
        df['滞納残債'] = pd.to_numeric(df['滞納残債'], errors='coerce').fillna(0)
        df['クライアントCD'] = pd.to_numeric(df['クライアントCD'], errors='coerce')
        
        # 空値・NaN値の除外
        before_count = len(df)
        df = df[df['引継番号'].str.strip() != '']
        df = df[df['引継番号'] != 'nan']
        after_count = len(df)
        
        logger.info(f"無効データ除外: {before_count} -> {after_count} 件")
        
        # クライアントCD=1,4の確認（手動前処理済みのはずだが念のため）
        valid_client_cd = df['クライアントCD'].isin([1, 4])
        invalid_count = len(df[~valid_client_cd])
        if invalid_count > 0:
            logger.warning(f"クライアントCD=1,4以外のデータ: {invalid_count} 件（除外します）")
            df = df[valid_client_cd]
        
        logger.info(f"契約データ検証完了: {len(df)} 件")
        
        return df
        
    except Exception as e:
        logger.error(f"契約データの検証エラー: {str(e)}")
        raise

def merge_data_light(contract_data: pd.DataFrame, arrear_data: pd.DataFrame) -> pd.DataFrame:
    """引継番号をキーにデータをマッチング（軽量版）"""
    try:
        logger.info("=== データマッチング処理（軽量版） ===")
        
        # マッチング前のデータ件数
        logger.info(f"マッチング前 - 契約データ: {len(contract_data)} 件")
        logger.info(f"マッチング前 - 滞納データ: {len(arrear_data)} 件")
        
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
        
        # マッチング統計
        matched_count = merged_df['契約No'].notna().sum()
        not_matched_count = merged_df['契約No'].isna().sum()
        
        logger.info(f"マッチング結果:")
        logger.info(f"  - マッチ成功: {matched_count} 件")
        logger.info(f"  - マッチ失敗（完済扱い）: {not_matched_count} 件")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"データマッチングエラー: {str(e)}")
        raise

def extract_changed_data_light(merged_df: pd.DataFrame) -> pd.DataFrame:
    """新旧の残債額が異なるデータのみを抽出（軽量版）"""
    try:
        logger.info("=== 差分データ抽出（軽量版） ===")
        
        # 新旧の残債額が異なるデータのみを抽出
        changed_df = merged_df[merged_df['滞納残債'] != merged_df['滞納額合計']].copy()
        
        logger.info(f"差分抽出結果: {len(merged_df)} -> {len(changed_df)} 件")
        logger.info(f"変更なしで除外: {len(merged_df) - len(changed_df)} 件")
        
        # 詳細統計
        if len(changed_df) > 0:
            increased = len(changed_df[changed_df['滞納額合計'] > changed_df['滞納残債']])
            decreased = len(changed_df[changed_df['滞納額合計'] < changed_df['滞納残債']])
            
            logger.info(f"  - 残債増加: {increased} 件")
            logger.info(f"  - 残債減少: {decreased} 件")
        
        return changed_df
        
    except Exception as e:
        logger.error(f"差分データ抽出エラー: {str(e)}")
        raise

def create_output_dataframe_light(changed_df: pd.DataFrame) -> pd.DataFrame:
    """最終出力用のDataFrameを作成（軽量版）"""
    try:
        logger.info("=== 出力データ作成（軽量版） ===")
        
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

def process_capco_debt_update_light(arrear_file_content: bytes, contract_file_content: bytes, progress_callback=None) -> Tuple[pd.DataFrame, str, Dict[str, any]]:
    """
    カプコ残債更新処理のメイン関数（軽量版）
    
    Args:
        arrear_file_content: 前処理済み滞納データCSVのファイル内容
        contract_file_content: 前処理済み契約データCSVのファイル内容
        progress_callback: 進捗を更新するコールバック関数
    
    Returns:
        処理結果のDataFrame、出力ファイル名、処理統計情報のタプル
    """
    try:
        # 統計情報を保存する辞書
        stats = {}
        
        # Step 1: 前処理済みファイルを読み込む
        logger.info("=== Step 1: 軽量ファイル読み込み開始 ===")
        if progress_callback:
            progress_callback(0.2, "Step 1/4: 前処理済みファイル読み込み中...")
        
        arrear_df = read_csv_with_encoding(arrear_file_content, "前処理済み滞納データ.csv", ARREAR_EXPECTED_COLUMNS)
        contract_df = read_csv_with_encoding(contract_file_content, "前処理済み契約データ.csv", CONTRACT_EXPECTED_COLUMNS)
        
        stats['arrear_total_rows'] = len(arrear_df)
        stats['contract_total_rows'] = len(contract_df)
        stats['light_version'] = True
        
        # Step 2: データ検証とクリーニング
        logger.info("=== Step 2: データ検証・クリーニング ===")
        if progress_callback:
            progress_callback(0.4, "Step 2/4: データ検証中...")
        
        arrear_data = validate_arrear_data(arrear_df)
        contract_data = validate_contract_data(contract_df)
        
        stats['arrear_validated'] = len(arrear_data)
        stats['contract_validated'] = len(contract_data)
        
        if len(contract_data) == 0:
            logger.warning("有効な契約データが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新_軽量版.csv"
            return empty_df, output_filename, stats
        
        # Step 3: データマッチング
        if progress_callback:
            progress_callback(0.7, "Step 3/4: データマッチング中...")
        
        merged_df = merge_data_light(contract_data, arrear_data)
        stats['match_success'] = merged_df['契約No'].notna().sum()
        stats['match_failed'] = merged_df['契約No'].isna().sum()
        
        # Step 4: 差分データ抽出と出力
        if progress_callback:
            progress_callback(0.9, "Step 4/4: 差分抽出・出力作成中...")
        
        changed_df = extract_changed_data_light(merged_df)
        stats['diff_total'] = len(merged_df)
        stats['diff_changed'] = len(changed_df)
        stats['diff_unchanged'] = stats['diff_total'] - stats['diff_changed']
        
        if len(changed_df) > 0:
            stats['diff_increased'] = len(changed_df[changed_df['滞納額合計'] > changed_df['滞納残債']])
            stats['diff_decreased'] = len(changed_df[changed_df['滞納額合計'] < changed_df['滞納残債']])
        else:
            stats['diff_increased'] = 0
            stats['diff_decreased'] = 0
        
        if len(changed_df) == 0:
            logger.warning("更新が必要なデータが存在しません")
            empty_df = pd.DataFrame(columns=OUTPUT_HEADERS)
            timestamp = datetime.now().strftime("%m%d")
            output_filename = f"{timestamp}カプコ残債の更新_軽量版.csv"
            return empty_df, output_filename, stats
        
        result_df = create_output_dataframe_light(changed_df)
        stats['output_count'] = len(result_df)
        
        # 出力ファイル名の生成
        timestamp = datetime.now().strftime("%m%d")
        output_filename = f"{timestamp}カプコ残債の更新_軽量版.csv"
        
        logger.info("=== 軽量版処理完了 ===")
        logger.info(f"出力ファイル名: {output_filename}")
        
        return result_df, output_filename, stats
        
    except Exception as e:
        logger.error(f"軽量版処理中にエラーが発生しました: {str(e)}")
        raise