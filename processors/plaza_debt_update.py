"""
プラザ残債更新処理

プラザの残債更新に必要な2つのCSVファイルを生成：
1. 管理前滞納額情報CSV
2. 交渉履歴CSV

入力：
- 前日のコールセンター回収委託情報（Excel）
- 当日のコールセンター回収委託情報（Excel）
- 1241件.csv（プラザ依頼分リスト）

処理フロー：
1. 前日と当日の延滞額合計を比較して入金額を算出
2. 会員番号と引継番号をマッチングして管理番号を取得
3. 2つの出力ファイルを生成
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List, Dict, Any
from processors.common.detailed_logger import DetailedLogger
from processors.sms_common.utils import read_csv_auto_encoding
from processors.common.plaza_debt_columns import PlazaDebtUpdateColumns as PDC


def process_plaza_debt_update(
    yesterday_file: bytes,
    today_file: bytes,
    plaza_list_file: bytes
) -> Tuple[List[pd.DataFrame], List[str], List[str], Dict[str, Any]]:
    """
    プラザ残債更新処理のメイン関数
    
    Args:
        yesterday_file: 前日のコールセンター回収委託情報（Excel）
        today_file: 当日のコールセンター回収委託情報（Excel）
        plaza_list_file: 1241件.csv（プラザ依頼分リスト）
    
    Returns:
        tuple: (出力DataFrameのリスト, ファイル名のリスト, ログリスト, 統計情報)
    """
    logs = []
    
    try:
        # === ファイル読み込み ===
        logs.append("📂 ファイル読み込み開始...")
        
        # 前日のExcelファイル読み込み
        df_yesterday = pd.read_excel(
            io.BytesIO(yesterday_file),
            usecols=[
                PDC.COLLECTION_REPORT['member_no']['name'],
                PDC.COLLECTION_REPORT['arrears_total']['name']
            ]
        )
        logs.append(f"前日データ読み込み: {len(df_yesterday)}件")
        
        # 当日のExcelファイル読み込み
        df_today = pd.read_excel(
            io.BytesIO(today_file),
            usecols=[
                PDC.COLLECTION_REPORT['member_no']['name'],
                PDC.COLLECTION_REPORT['arrears_total']['name'],
                PDC.COLLECTION_REPORT['report_source']['name'],
                PDC.COLLECTION_REPORT['cancellation_date']['name'],
                PDC.COLLECTION_REPORT['move_out_date']['name']
            ]
        )
        logs.append(f"当日データ読み込み: {len(df_today)}件")
        
        # 1241件.csvの読み込み
        df_plaza_list = read_csv_auto_encoding(plaza_list_file)
        logs.append(f"プラザリスト読み込み: {len(df_plaza_list)}件")
        
        # === データ処理 ===
        logs.append("🔄 データ処理開始...")
        
        # 前日データとのマージ（VLOOKUPの実装）
        df_merged = df_today.merge(
            df_yesterday[[
                PDC.COLLECTION_REPORT['member_no']['name'],
                PDC.COLLECTION_REPORT['arrears_total']['name']
            ]],
            on=PDC.COLLECTION_REPORT['member_no']['name'],
            how='left',
            suffixes=('_当日', '_前日')
        )
        
        # 入金額の計算
        arrears_today = PDC.COLLECTION_REPORT['arrears_total']['name'] + '_当日'
        arrears_yesterday = PDC.COLLECTION_REPORT['arrears_total']['name'] + '_前日'
        
        # 数値型に変換
        df_merged[arrears_today] = pd.to_numeric(df_merged[arrears_today], errors='coerce').fillna(0)
        df_merged[arrears_yesterday] = pd.to_numeric(df_merged[arrears_yesterday], errors='coerce').fillna(0)
        
        # 入金額計算（前日 - 当日）
        df_merged['入金額'] = df_merged[arrears_yesterday] - df_merged[arrears_today]
        
        # マッチング状況をログ
        matched_count = df_merged[arrears_yesterday].notna().sum()
        logs.append(f"前日データとのマッチング: {matched_count}/{len(df_merged)}件")
        
        # 管理番号の取得（1241件.csvとのマッチング）
        df_merged = df_merged.merge(
            df_plaza_list[[
                PDC.PLAZA_LIST['takeover_no']['name'],
                PDC.PLAZA_LIST['management_no']['name']
            ]],
            left_on=PDC.COLLECTION_REPORT['member_no']['name'],
            right_on=PDC.PLAZA_LIST['takeover_no']['name'],
            how='left'
        )
        
        # マッチング状況をログ
        management_matched = df_merged[PDC.PLAZA_LIST['management_no']['name']].notna().sum()
        logs.append(f"管理番号とのマッチング: {management_matched}/{len(df_merged)}件")
        
        # === 出力1: 管理前滞納額情報CSV ===
        output1 = pd.DataFrame({
            PDC.LATE_PAYMENT_OUTPUT_HEADERS[0]: df_merged[PDC.PLAZA_LIST['management_no']['name']].fillna(''),
            PDC.LATE_PAYMENT_OUTPUT_HEADERS[1]: df_merged[arrears_today].astype(int)
        })
        
        # === 出力2: 交渉履歴CSV ===
        # 交渉備考の生成
        today_str = datetime.now().strftime('%Y/%m/%d')
        
        def create_negotiation_note(row):
            """交渉備考の文字列を生成"""
            payment = int(row['入金額'])
            balance = int(row[arrears_today])
            return f"{today_str}　{payment:,}円入金あり（現残債{balance:,}円）"
        
        # 交渉履歴データの作成
        output2 = pd.DataFrame({
            PDC.NEGOTIATES_OUTPUT_HEADERS[0]: df_merged[PDC.PLAZA_LIST['management_no']['name']].fillna(''),
            PDC.NEGOTIATES_OUTPUT_HEADERS[1]: today_str,
            PDC.NEGOTIATES_OUTPUT_HEADERS[2]: PDC.NEGOTIATES_FIXED_VALUES['担当'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[3]: PDC.NEGOTIATES_FIXED_VALUES['相手'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[4]: PDC.NEGOTIATES_FIXED_VALUES['手段'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[5]: PDC.NEGOTIATES_FIXED_VALUES['回収ランク'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[6]: PDC.NEGOTIATES_FIXED_VALUES['結果'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[7]: PDC.NEGOTIATES_FIXED_VALUES['入金予定'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[8]: PDC.NEGOTIATES_FIXED_VALUES['予定金額'],
            PDC.NEGOTIATES_OUTPUT_HEADERS[9]: df_merged.apply(create_negotiation_note, axis=1)
        })
        
        # === ファイル名生成 ===
        date_str = datetime.now().strftime("%m%d")
        filename1 = f"{date_str}プラザ管理前滞納額.csv"
        filename2 = f"{date_str}プラザ交渉履歴.csv"
        
        # === 統計情報 ===
        stats = {
            'total_records': len(df_merged),
            'management_matched': management_matched,
            'total_payment': df_merged['入金額'].sum(),
            'positive_payments': (df_merged['入金額'] > 0).sum(),
            'zero_payments': (df_merged['入金額'] == 0).sum(),
            'negative_payments': (df_merged['入金額'] < 0).sum()
        }
        
        logs.append(f"✅ 処理完了: {len(df_merged)}件")
        logs.append(f"入金総額: {stats['total_payment']:,}円")
        logs.append(f"入金あり: {stats['positive_payments']}件、入金なし: {stats['zero_payments']}件、残債増加: {stats['negative_payments']}件")
        
        return [output1, output2], [filename1, filename2], logs, stats
        
    except Exception as e:
        error_msg = f"エラーが発生しました: {str(e)}"
        logs.append(f"❌ {error_msg}")
        raise Exception(error_msg)