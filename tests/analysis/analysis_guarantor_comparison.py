"""
フェイス催告書保証人データ比較分析スクリプト
3ファイルを比較して②（保証人2）の件数不一致原因を特定
"""

import pandas as pd
import io
import sys
from typing import Tuple

# ファイルパス
CONTRACT_LIST = "/Users/tchytky/Downloads/ContractList_20250925115419.csv"
HOSOYA_LIST = "/Users/tchytky/Downloads/フェイス退去済み保証人（細谷作成）.xlsx"
SYSTEM_OUTPUT = "/Users/tchytky/Downloads/0925フェイス差込み用リスト（連帯保証人【退去済み】）確認が必要.csv"


def read_csv_auto_encoding(file_path: str) -> pd.DataFrame:
    """CSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']

    for enc in encodings:
        try:
            df = pd.read_csv(file_path, encoding=enc, dtype=str)
            print(f"✓ {file_path.split('/')[-1]} を {enc} で読み込み成功")
            return df
        except Exception as e:
            continue

    raise ValueError(f"CSVファイルの読み込みに失敗: {file_path}")


def analyze_contract_list(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    元データ（ContractList）から退去済み保証人の理論値を算出
    """
    print("\n" + "="*80)
    print("【1】元データ分析: ContractList")
    print("="*80)

    original_count = len(df)
    print(f"総データ数: {original_count}件")

    # 列名の確認（最初の120列）
    print(f"\n列数: {len(df.columns)}列")

    # 入居ステータスの列を特定（O列 = 14列目、0始まりなので13）
    # ※実際のContractListの構造に合わせて調整が必要

    # 委託先法人ID（DO列 = 118列目）
    df_filtered = df.copy()
    df_filtered['委託先法人ID_num'] = pd.to_numeric(df.iloc[:, 118], errors='coerce')
    df_filtered = df_filtered[df_filtered['委託先法人ID_num'].isin([1, 2, 3, 4])]
    print(f"委託先法人ID（1-4）フィルタ後: {len(df_filtered)}件")

    # 入居ステータス（O列 = 14列目）で「退去済」を抽出
    # 列名がわからないので iloc で位置指定
    df_filtered = df_filtered[df_filtered.iloc[:, 14] == '退去済']
    print(f"入居ステータス「退去済」: {len(df_filtered)}件")

    # 回収ランク（CI列 = 86列目）で除外
    df_filtered = df_filtered[~df_filtered.iloc[:, 86].isin(['死亡決定', '破産決定', '弁護士介入'])]
    print(f"回収ランクフィルタ後: {len(df_filtered)}件")

    # 保証人1の住所完全性チェック（AP-AT列 = 41-45列目）
    guarantor1_complete = df_filtered[
        df_filtered.iloc[:, 42].notna() & (df_filtered.iloc[:, 42] != '') &  # 郵便番号
        df_filtered.iloc[:, 43].notna() & (df_filtered.iloc[:, 43] != '') &  # 現住所1
        df_filtered.iloc[:, 44].notna() & (df_filtered.iloc[:, 44] != '') &  # 現住所2
        df_filtered.iloc[:, 45].notna() & (df_filtered.iloc[:, 45] != '') &  # 現住所3
        df_filtered.iloc[:, 41].notna() & (df_filtered.iloc[:, 41] != '')    # 保証人名
    ]
    print(f"\n保証人1（住所完全）: {len(guarantor1_complete)}件")

    # 保証人2の住所完全性チェック（AX-BB列 = 49-53列目）
    guarantor2_complete = df_filtered[
        df_filtered.iloc[:, 50].notna() & (df_filtered.iloc[:, 50] != '') &  # 郵便番号
        df_filtered.iloc[:, 51].notna() & (df_filtered.iloc[:, 51] != '') &  # 現住所1
        df_filtered.iloc[:, 52].notna() & (df_filtered.iloc[:, 52] != '') &  # 現住所2
        df_filtered.iloc[:, 53].notna() & (df_filtered.iloc[:, 53] != '') &  # 現住所3
        df_filtered.iloc[:, 49].notna() & (df_filtered.iloc[:, 49] != '')    # 保証人名
    ]
    print(f"保証人2（住所完全）: {len(guarantor2_complete)}件")

    # 管理番号を取得（A列 = 0列目）
    g1_ids = set(guarantor1_complete.iloc[:, 0].astype(str).tolist())
    g2_ids = set(guarantor2_complete.iloc[:, 0].astype(str).tolist())

    # 重複（保証人1と2が両方とも完全な契約）
    both_complete = g1_ids & g2_ids
    print(f"\n【重要】保証人1と2が両方完全な契約数: {len(both_complete)}件")
    if len(both_complete) > 0:
        print(f"  管理番号例（最大5件）: {list(both_complete)[:5]}")

    results = {
        'total_evicted': len(df_filtered),
        'guarantor1_complete': len(guarantor1_complete),
        'guarantor2_complete': len(guarantor2_complete),
        'both_complete': len(both_complete),
        'g1_ids': g1_ids,
        'g2_ids': g2_ids,
        'both_ids': both_complete
    }

    return df_filtered, results


def analyze_hosoya_list(file_path: str) -> dict:
    """細谷様作成リストを分析"""
    print("\n" + "="*80)
    print("【2】細谷様作成リスト分析")
    print("="*80)

    df = pd.read_excel(file_path, dtype=str)
    print(f"総データ数: {len(df)}件")
    print(f"列数: {len(df.columns)}列")

    # 全列名を表示
    print(f"列名: {list(df.columns)[:5]}...{list(df.columns)[-3:]}")

    # Excelの最終列をチェック（番号列の可能性）
    last_col = df.columns[-1]
    print(f"\n最終列名: '{last_col}'")

    # 番号列を探す
    number_col = None
    if '番号' in df.columns:
        number_col = '番号'
        print("✓ '番号'列を発見")
    else:
        # 最終列に①②があるか確認
        if last_col in df.columns:
            unique_vals = df[last_col].dropna().unique()
            print(f"最終列のユニーク値: {unique_vals}")
            if any(v in ['①', '②', '1', '2'] for v in unique_vals):
                number_col = last_col
                print(f"✓ '{last_col}'列を番号列として使用")

    if number_col:
        df1 = df[df[number_col].isin(['①', '1'])]
        df2 = df[df[number_col].isin(['②', '2'])]

        print(f"\n①（保証人1）: {len(df1)}件")
        print(f"②（保証人2）: {len(df2)}件")

        g1_ids = set()
        g2_ids = set()
        g2_ids_list = []

        if '管理番号' in df.columns:
            if len(df1) > 0:
                g1_ids = set(df1['管理番号'].astype(str).tolist())
            if len(df2) > 0:
                g2_ids_list = df2['管理番号'].astype(str).tolist()
                g2_ids = set(g2_ids_list)

                print(f"\n②の管理番号リスト（全{len(g2_ids_list)}件）:")
                for i, gid in enumerate(sorted(g2_ids_list), 1):
                    print(f"  {i:2d}. {gid}")
        else:
            print("⚠️ '管理番号'列が見つかりません")

        return {
            'total': len(df),
            'g1_count': len(df1),
            'g2_count': len(df2),
            'g1_ids': g1_ids,
            'g2_ids': g2_ids,
            'g2_ids_list': g2_ids_list
        }
    else:
        print("⚠️ 番号列が見つかりません")
        # デバッグ用：最初の5行を表示
        print("\n最初の5行のサンプル:")
        print(df.head())
        return {'total': len(df), 'g1_count': 0, 'g2_count': 0, 'g1_ids': set(), 'g2_ids': set(), 'g2_ids_list': []}


def analyze_system_output(file_path: str) -> dict:
    """システム出力リストを分析"""
    print("\n" + "="*80)
    print("【3】システム出力リスト分析")
    print("="*80)

    df = read_csv_auto_encoding(file_path)
    print(f"総データ数: {len(df)}件")
    print(f"列: {list(df.columns)}")

    # 番号列で①②を分類
    if '番号' in df.columns:
        df1 = df[df['番号'] == '①']
        df2 = df[df['番号'] == '②']

        print(f"\n①（保証人1）: {len(df1)}件")
        print(f"②（保証人2）: {len(df2)}件")

        g1_ids = set()
        g2_ids = set()
        g2_ids_list = []

        if '管理番号' in df.columns:
            if len(df1) > 0:
                g1_ids = set(df1['管理番号'].astype(str).tolist())
            if len(df2) > 0:
                g2_ids_list = df2['管理番号'].astype(str).tolist()
                g2_ids = set(g2_ids_list)

                print(f"\n②の管理番号リスト（全{len(g2_ids_list)}件）:")
                for i, gid in enumerate(sorted(g2_ids_list), 1):
                    print(f"  {i:2d}. {gid}")

        return {
            'total': len(df),
            'g1_count': len(df1),
            'g2_count': len(df2),
            'g1_ids': g1_ids,
            'g2_ids': g2_ids,
            'g2_ids_list': g2_ids_list
        }
    else:
        print("⚠️ '番号'列が見つかりません")
        return {'total': len(df), 'g1_count': 0, 'g2_count': 0, 'g1_ids': set(), 'g2_ids': set(), 'g2_ids_list': []}


def generate_comparison_report(contract_results: dict, hosoya_results: dict, system_results: dict):
    """比較レポート生成"""
    print("\n" + "="*80)
    print("【4】比較レポート")
    print("="*80)

    print("\n■ 件数サマリー")
    print(f"{'':20} {'①（保証人1）':>15} {'②（保証人2）':>15} {'合計':>10}")
    print("-" * 65)
    print(f"{'元データ（理論値）':20} {contract_results['guarantor1_complete']:>15} {contract_results['guarantor2_complete']:>15} {contract_results['guarantor1_complete'] + contract_results['guarantor2_complete']:>10}")
    print(f"{'細谷様リスト':20} {hosoya_results['g1_count']:>15} {hosoya_results['g2_count']:>15} {hosoya_results['total']:>10}")
    print(f"{'システム出力':20} {system_results['g1_count']:>15} {system_results['g2_count']:>15} {system_results['total']:>10}")

    print("\n■ ②（保証人2）の差分分析")

    # 細谷様 vs システム
    hosoya_only = hosoya_results['g2_ids'] - system_results['g2_ids']
    system_only = system_results['g2_ids'] - hosoya_results['g2_ids']
    common = hosoya_results['g2_ids'] & system_results['g2_ids']

    print(f"\n細谷様リストのみにある②: {len(hosoya_only)}件")
    if len(hosoya_only) > 0:
        print(f"  管理番号: {sorted(list(hosoya_only))[:10]}")
        if len(hosoya_only) > 10:
            print(f"  ... 他{len(hosoya_only) - 10}件")

    print(f"\nシステム出力のみにある②: {len(system_only)}件")
    if len(system_only) > 0:
        print(f"  管理番号: {sorted(list(system_only))[:10]}")
        if len(system_only) > 10:
            print(f"  ... 他{len(system_only) - 10}件")

    print(f"\n両方に共通する②: {len(common)}件")

    # 保証人1と2の重複確認
    print("\n■ 【重要】保証人1と2の関係性分析")

    # 細谷様リストで保証人1と2が重複している契約
    hosoya_both = hosoya_results['g1_ids'] & hosoya_results['g2_ids']
    print(f"\n細谷様リスト: 保証人1と2が両方存在する契約: {len(hosoya_both)}件")

    # システム出力で保証人1と2が重複している契約
    system_both = system_results['g1_ids'] & system_results['g2_ids']
    print(f"システム出力: 保証人1と2が両方存在する契約: {len(system_both)}件")

    # 元データで保証人1と2が両方完全な契約
    print(f"元データ: 保証人1と2が両方完全な契約: {contract_results['both_complete']}件")

    # 結論
    print("\n" + "="*80)
    print("【結論】")
    print("="*80)

    if len(hosoya_both) == 0 and len(system_both) > 0:
        print("\n⚠️ 問題発見!")
        print("細谷様リストでは「保証人1が存在する契約は保証人2を出力しない」ルール")
        print(f"システム出力では「保証人1と2を両方出力」している: {len(system_both)}件")
        print("\n→ システムのロジックを修正する必要があります")
        print("   修正案: 保証人2は「保証人1が抽出されなかった契約のみ」から抽出")
    elif hosoya_results['g2_count'] != system_results['g2_count']:
        print(f"\n⚠️ ②の件数が不一致: 細谷様{hosoya_results['g2_count']}件 vs システム{system_results['g2_count']}件")
        print("詳細な差分管理番号を上記で確認してください")
    else:
        print("\n✓ ②の件数は一致しています")


def main():
    """メイン処理"""
    print("フェイス催告書保証人データ比較分析")
    print("="*80)

    try:
        # 1. 元データ分析
        df_contract = read_csv_auto_encoding(CONTRACT_LIST)
        df_filtered, contract_results = analyze_contract_list(df_contract)

        # 2. 細谷様リスト分析
        hosoya_results = analyze_hosoya_list(HOSOYA_LIST)

        # 3. システム出力分析
        system_results = analyze_system_output(SYSTEM_OUTPUT)

        # 4. 比較レポート生成
        generate_comparison_report(contract_results, hosoya_results, system_results)

        print("\n" + "="*80)
        print("分析完了")
        print("="*80)

    except Exception as e:
        print(f"\n❌ エラー発生: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
