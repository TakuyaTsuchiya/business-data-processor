"""
フェイス差込み用リスト作成プロセッサー
契約者、連帯保証人、緊急連絡人の3種類のリストを生成する
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple


def process_faith_notification(df: pd.DataFrame, target_type: str) -> Tuple[pd.DataFrame, str, str, list]:
    """
    フェイス差込み用リストを生成する
    
    Args:
        df: ContractList_*.csvのデータフレーム
        target_type: 'contractor', 'guarantor', 'contact' のいずれか
        
    Returns:
        (result_df, filename, message, logs)
    """
    logs = []
    
    try:
        logs.append(f"処理開始: {target_type}用リスト作成")
        logs.append(f"入力データ: {len(df)}件, {len(df.columns)}列")
        
        # 共通フィルタリング
        filtered_df, filter_logs = apply_common_filters(df)
        logs.extend(filter_logs)
        
        # タイプ別処理
        if target_type == 'contractor':
            result_df, process_logs = process_contractor(filtered_df)
            list_name = "契約者"
        elif target_type == 'guarantor':
            result_df, process_logs = process_guarantor(filtered_df)
            list_name = "連帯保証人"
        elif target_type == 'contact':
            result_df, process_logs = process_contact(filtered_df)
            list_name = "連絡人"
        else:
            raise ValueError(f"不明なターゲットタイプ: {target_type}")
        
        logs.extend(process_logs)
        
        # ファイル名生成
        now = datetime.now()
        filename = f"{now.strftime('%m%d')}フェイス差込み用リスト（{list_name}）.csv"
        
        message = f"フェイス差込み用リスト（{list_name}）を作成しました。出力件数: {len(result_df)}件"
        logs.append(f"処理完了: 出力件数 {len(result_df)}件")
        
        return result_df, filename, message, logs
        
    except Exception as e:
        logs.append(f"エラー発生: {str(e)}")
        raise Exception(f"処理中にエラーが発生しました: {str(e)}")


def apply_common_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """共通フィルタリング条件を適用"""
    logs = []
    initial_count = len(df)
    
    # 委託先法人ID（DO列 = 118列目）でフィルタ
    before_count = len(df)
    df = df[df.iloc[:, 118].isin([1, 2, 3, 4])]
    logs.append(f"委託先法人IDフィルタ（1,2,3,4のみ）: {before_count}件 → {len(df)}件")
    
    # 入金予定日（BU列 = 72列目）でフィルタ（本日と未来を除外）
    today = pd.Timestamp.now().normalize()
    
    # 入金予定日を日付型に変換（警告回避のためcopyを使用）
    df = df.copy()
    df['入金予定日_converted'] = pd.to_datetime(df.iloc[:, 72], errors='coerce')
    
    # 過去の日付または空欄（NaT）のみを残す
    before_count = len(df)
    df = df[(df['入金予定日_converted'] < today) | (df['入金予定日_converted'].isna())]
    df = df.drop('入金予定日_converted', axis=1)
    logs.append(f"入金予定日フィルタ（過去のみ）: {before_count}件 → {len(df)}件")
    
    # 入金予定金額（BV列 = 73列目）でフィルタ（2,3,5を除外）
    before_count = len(df)
    df = df[~df.iloc[:, 73].isin([2, 3, 5])]
    logs.append(f"入金予定金額フィルタ（2,3,5除外）: {before_count}件 → {len(df)}件")
    
    # 回収ランク（CI列 = 86列目）でフィルタ
    before_count = len(df)
    df = df[~df.iloc[:, 86].isin(['死亡決定', '弁護士介入'])]
    logs.append(f"回収ランクフィルタ（死亡決定・弁護士介入除外）: {before_count}件 → {len(df)}件")
    
    logs.append(f"共通フィルタリング完了: {initial_count}件 → {len(df)}件")
    
    return df, logs


def process_contractor(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """契約者用リストを作成"""
    logs = []
    before_count = len(df)
    
    # 住所フィルタ（W,X,Y,Z列 = 22,23,24,25列目）
    df = df[
        df.iloc[:, 22].notna() & (df.iloc[:, 22] != '') &  # 郵便番号
        df.iloc[:, 23].notna() & (df.iloc[:, 23] != '') &  # 現住所1
        df.iloc[:, 24].notna() & (df.iloc[:, 24] != '') &  # 現住所2
        df.iloc[:, 25].notna() & (df.iloc[:, 25] != '')    # 現住所3
    ]
    logs.append(f"契約者住所フィルタ（完全な住所のみ）: {before_count}件 → {len(df)}件")
    
    try:
        result_df = pd.DataFrame({
            '管理番号': df.iloc[:, 0],          # A列
            '契約者氏名': df.iloc[:, 20],       # U列
            '郵便番号': df.iloc[:, 22],         # W列
            '現住所1': df.iloc[:, 23],          # X列
            '現住所2': df.iloc[:, 24],          # Y列
            '現住所3': df.iloc[:, 25],          # Z列
            '滞納残債': df.iloc[:, 71],         # BT列
            '物件住所1': df.iloc[:, 92],        # CO列
            '物件住所2': df.iloc[:, 93],        # CP列
            '物件住所3': df.iloc[:, 94],        # CQ列
            '物件名': df.iloc[:, 95],           # CR列
            '物件番号': df.iloc[:, 96],         # CS列
            '回収口座銀行CD': df.iloc[:, 34],   # AI列
            '回収口座銀行名': df.iloc[:, 35],   # AJ列
            '回収口座支店CD': df.iloc[:, 36],   # AK列
            '回収口座支店名': df.iloc[:, 37],   # AL列
            '回収口座種類': df.iloc[:, 38],     # AM列
            '回収口座番号': df.iloc[:, 39],     # AN列
            '回収口座名義人': df.iloc[:, 40],   # AO列
        })
        logs.append(f"マッピング完了: {len(result_df)}列の出力データを生成")
    except Exception as e:
        logs.append(f"マッピングエラー: {str(e)}")
        raise
    
    return result_df, logs


def process_guarantor(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """連帯保証人用リストを作成"""
    logs = []
    results = []
    initial_count = len(df)
    
    # 保証人1の処理（AP-AW列 = 41-48列目）
    guarantor1_df = df[
        df.iloc[:, 42].notna() & (df.iloc[:, 42] != '') &  # 郵便番号（AQ）
        df.iloc[:, 43].notna() & (df.iloc[:, 43] != '') &  # 現住所1（AR）
        df.iloc[:, 44].notna() & (df.iloc[:, 44] != '') &  # 現住所2（AS）
        df.iloc[:, 45].notna() & (df.iloc[:, 45] != '') &  # 現住所3（AT）
        df.iloc[:, 41].notna() & (df.iloc[:, 41] != '')    # 保証人名（AP）
    ]
    
    if not guarantor1_df.empty:
        g1_result = pd.DataFrame({
            '管理番号': guarantor1_df.iloc[:, 0],
            '契約者氏名': guarantor1_df.iloc[:, 20],
            '連帯保証人名': guarantor1_df.iloc[:, 41],    # AP列
            '郵便番号': guarantor1_df.iloc[:, 42],        # AQ列
            '現住所1': guarantor1_df.iloc[:, 43],         # AR列
            '現住所2': guarantor1_df.iloc[:, 44],         # AS列
            '現住所3': guarantor1_df.iloc[:, 45],         # AT列
            '滞納残債': guarantor1_df.iloc[:, 71],
            '物件住所1': guarantor1_df.iloc[:, 92],
            '物件住所2': guarantor1_df.iloc[:, 93],
            '物件住所3': guarantor1_df.iloc[:, 94],
            '物件名': guarantor1_df.iloc[:, 95],
            '物件番号': guarantor1_df.iloc[:, 96],
            '回収口座銀行CD': guarantor1_df.iloc[:, 34],
            '回収口座銀行名': guarantor1_df.iloc[:, 35],
            '回収口座支店CD': guarantor1_df.iloc[:, 36],
            '回収口座支店名': guarantor1_df.iloc[:, 37],
            '回収口座種類': guarantor1_df.iloc[:, 38],
            '回収口座番号': guarantor1_df.iloc[:, 39],
            '回収口座名義人': guarantor1_df.iloc[:, 40],
            '番号': '①'
        })
        results.append(g1_result)
    
    # 保証人2の処理（AX-BE列 = 49-56列目）
    guarantor2_df = df[
        df.iloc[:, 50].notna() & (df.iloc[:, 50] != '') &  # 郵便番号（AY）
        df.iloc[:, 51].notna() & (df.iloc[:, 51] != '') &  # 現住所1（AZ）
        df.iloc[:, 52].notna() & (df.iloc[:, 52] != '') &  # 現住所2（BA）
        df.iloc[:, 53].notna() & (df.iloc[:, 53] != '') &  # 現住所3（BB）
        df.iloc[:, 49].notna() & (df.iloc[:, 49] != '')    # 保証人名（AX）
    ]
    
    if not guarantor2_df.empty:
        g2_result = pd.DataFrame({
            '管理番号': guarantor2_df.iloc[:, 0],
            '契約者氏名': guarantor2_df.iloc[:, 20],
            '連帯保証人名': guarantor2_df.iloc[:, 49],    # AX列
            '郵便番号': guarantor2_df.iloc[:, 50],        # AY列
            '現住所1': guarantor2_df.iloc[:, 51],         # AZ列
            '現住所2': guarantor2_df.iloc[:, 52],         # BA列
            '現住所3': guarantor2_df.iloc[:, 53],         # BB列
            '滞納残債': guarantor2_df.iloc[:, 71],
            '物件住所1': guarantor2_df.iloc[:, 92],
            '物件住所2': guarantor2_df.iloc[:, 93],
            '物件住所3': guarantor2_df.iloc[:, 94],
            '物件名': guarantor2_df.iloc[:, 95],
            '物件番号': guarantor2_df.iloc[:, 96],
            '回収口座銀行CD': guarantor2_df.iloc[:, 34],
            '回収口座銀行名': guarantor2_df.iloc[:, 35],
            '回収口座支店CD': guarantor2_df.iloc[:, 36],
            '回収口座支店名': guarantor2_df.iloc[:, 37],
            '回収口座種類': guarantor2_df.iloc[:, 38],
            '回収口座番号': guarantor2_df.iloc[:, 39],
            '回収口座名義人': guarantor2_df.iloc[:, 40],
            '番号': '②'
        })
        results.append(g2_result)
    
    # 結果を結合
    # 保証人1と2の処理結果をログに記録
    if len(results) > 0:
        logs.append(f"保証人1: {len(results[0]) if len(results) > 0 else 0}件")
    if len(results) > 1:
        logs.append(f"保証人2: {len(results[1])}件")
    
    if results:
        result_df = pd.concat(results, ignore_index=True)
        logs.append(f"保証人リスト生成完了: 合計{len(result_df)}件")
        return result_df, logs
    else:
        # 空のデータフレームを返す（ヘッダーだけ）
        logs.append("該当する保証人データがありません")
        return pd.DataFrame(columns=['管理番号', '契約者氏名', '連帯保証人名', '郵便番号', 
                                    '現住所1', '現住所2', '現住所3', '滞納残債',
                                    '物件住所1', '物件住所2', '物件住所3', '物件名', '物件番号',
                                    '回収口座銀行CD', '回収口座銀行名', '回収口座支店CD', 
                                    '回収口座支店名', '回収口座種類', '回収口座番号', 
                                    '回収口座名義人', '番号']), logs


def process_contact(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """緊急連絡人用リストを作成"""
    logs = []
    results = []
    
    # 緊急連絡人1の処理（BF-BL列 = 57-63列目）
    contact1_df = df[
        df.iloc[:, 58].notna() & (df.iloc[:, 58] != '') &  # 郵便番号（BG）→BC
        df.iloc[:, 59].notna() & (df.iloc[:, 59] != '') &  # 現住所1（BH）→BD
        df.iloc[:, 60].notna() & (df.iloc[:, 60] != '') &  # 現住所2（BI）→BE
        df.iloc[:, 61].notna() & (df.iloc[:, 61] != '') &  # 現住所3（BJ）→BF
        df.iloc[:, 57].notna() & (df.iloc[:, 57] != '')    # 連絡人名（BF）→BA
    ]
    
    if not contact1_df.empty:
        c1_result = pd.DataFrame({
            '管理番号': contact1_df.iloc[:, 0],
            '契約者氏名': contact1_df.iloc[:, 20],
            '緊急連絡人１氏名': contact1_df.iloc[:, 57],  # BF列→BA列
            '郵便番号': contact1_df.iloc[:, 58],          # BG列→BC列
            '現住所1': contact1_df.iloc[:, 59],           # BH列→BD列
            '現住所2': contact1_df.iloc[:, 60],           # BI列→BE列
            '現住所3': contact1_df.iloc[:, 61],           # BJ列→BF列
            '滞納残債': contact1_df.iloc[:, 71],
            '物件住所1': contact1_df.iloc[:, 92],
            '物件住所2': contact1_df.iloc[:, 93],
            '物件住所3': contact1_df.iloc[:, 94],
            '物件名': contact1_df.iloc[:, 95],
            '物件番号': contact1_df.iloc[:, 96],
            '回収口座銀行CD': contact1_df.iloc[:, 34],
            '回収口座銀行名': contact1_df.iloc[:, 35],
            '回収口座支店CD': contact1_df.iloc[:, 36],
            '回収口座支店名': contact1_df.iloc[:, 37],
            '回収口座種類': contact1_df.iloc[:, 38],
            '回収口座番号': contact1_df.iloc[:, 39],
            '回収口座名義人': contact1_df.iloc[:, 40],
            '番号': '①'
        })
        results.append(c1_result)
    
    # 緊急連絡人2の処理（BM-BS列 = 64-70列目）
    contact2_df = df[
        df.iloc[:, 65].notna() & (df.iloc[:, 65] != '') &  # 郵便番号（BN）
        df.iloc[:, 66].notna() & (df.iloc[:, 66] != '') &  # 現住所1（BO）
        df.iloc[:, 67].notna() & (df.iloc[:, 67] != '') &  # 現住所2（BP）
        df.iloc[:, 68].notna() & (df.iloc[:, 68] != '') &  # 現住所3（BQ）
        df.iloc[:, 64].notna() & (df.iloc[:, 64] != '')    # 連絡人名（BM）
    ]
    
    if not contact2_df.empty:
        c2_result = pd.DataFrame({
            '管理番号': contact2_df.iloc[:, 0],
            '契約者氏名': contact2_df.iloc[:, 20],
            '緊急連絡人１氏名': contact2_df.iloc[:, 64],  # BM列（緊急連絡人2氏名）
            '郵便番号': contact2_df.iloc[:, 65],          # BN列
            '現住所1': contact2_df.iloc[:, 66],           # BO列
            '現住所2': contact2_df.iloc[:, 67],           # BP列
            '現住所3': contact2_df.iloc[:, 68],           # BQ列
            '滞納残債': contact2_df.iloc[:, 71],
            '物件住所1': contact2_df.iloc[:, 92],
            '物件住所2': contact2_df.iloc[:, 93],
            '物件住所3': contact2_df.iloc[:, 94],
            '物件名': contact2_df.iloc[:, 95],
            '物件番号': contact2_df.iloc[:, 96],
            '回収口座銀行CD': contact2_df.iloc[:, 34],
            '回収口座銀行名': contact2_df.iloc[:, 35],
            '回収口座支店CD': contact2_df.iloc[:, 36],
            '回収口座支店名': contact2_df.iloc[:, 37],
            '回収口座種類': contact2_df.iloc[:, 38],
            '回収口座番号': contact2_df.iloc[:, 39],
            '回収口座名義人': contact2_df.iloc[:, 40],
            '番号': '②'
        })
        results.append(c2_result)
    
    # 連絡人1と2の処理結果をログに記録
    if len(results) > 0:
        logs.append(f"緊急連絡人1: {len(results[0]) if len(results) > 0 else 0}件")
    if len(results) > 1:
        logs.append(f"緊急連絡人2: {len(results[1])}件")
    
    # 結果を結合
    if results:
        result_df = pd.concat(results, ignore_index=True)
        logs.append(f"連絡人リスト生成完了: 合計{len(result_df)}件")
        return result_df, logs
    else:
        # 空のデータフレームを返す（ヘッダーだけ）
        logs.append("該当する連絡人データがありません")
        return pd.DataFrame(columns=['管理番号', '契約者氏名', '緊急連絡人１氏名', '郵便番号', 
                                    '現住所1', '現住所2', '現住所3', '滞納残債',
                                    '物件住所1', '物件住所2', '物件住所3', '物件名', '物件番号',
                                    '回収口座銀行CD', '回収口座銀行名', '回収口座支店CD', 
                                    '回収口座支店名', '回収口座種類', '回収口座番号', 
                                    '回収口座名義人', '番号']), logs


