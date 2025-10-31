"""
オートコール履歴プロセッサー

list_export*.csvからオートコール履歴データを生成する。
NegotiatesInfoSample.csv形式（10列）で出力。
"""

import pandas as pd
import io
from typing import Optional, Tuple, List
from datetime import datetime


class AutocallHistoryProcessor:
    """オートコール履歴データ処理クラス"""

    # 出力列定義（NegotiatesInfoSample.csv形式）
    OUTPUT_COLUMNS = [
        "管理番号",
        "交渉日時",
        "担当",
        "相手",
        "手段",
        "回収ランク",
        "結果",
        "入金予定日",
        "予定金額",
        "交渉備考"
    ]

    def __init__(self, target_person: str):
        """
        Args:
            target_person: 相手の種類（契約者、保証人、連絡人、勤務先）
        """
        self.target_person = target_person

    def process(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        list_export*.csvデータを処理してオートコール履歴形式に変換

        Args:
            input_df: list_export*.csvから読み込んだDataFrame

        Returns:
            NegotiatesInfoSample.csv形式のDataFrame
        """
        # 処理用にコピー
        df = input_df.copy()

        # 仕様1: 最終架電日の空白を前行でフォワードフィル
        df['最終架電日'] = df['最終架電日'].ffill()

        # 仕様2: 「架電結果」が「通話済」のものを除外
        df = df[df['架電結果'] != '通話済'].copy()

        # マッピング処理
        output_df = pd.DataFrame()
        output_df['管理番号'] = df['管理番号']
        # 交渉日時: ハイフンをスラッシュに変換、秒を削除（2025-10-31 12:08:55 → 2025/10/31 12:08）
        output_df['交渉日時'] = df['最終架電日'].str.replace('-', '/', regex=False).str[:16]
        output_df['担当'] = ''
        output_df['相手'] = self.target_person
        output_df['手段'] = '架電'
        output_df['回収ランク'] = ''
        output_df['結果'] = 'その他'
        output_df['入金予定日'] = ''
        output_df['予定金額'] = ''

        # 交渉備考: f"{架電番号}オートコール{架電結果}　残債：{残債}円"
        # 残債フォーマット: NaNは「不明」、数値はカンマ区切り（例: 10,000）
        def format_debt(value):
            if pd.isna(value):
                return '不明'
            return f'{int(value):,}'

        debt_str = df['残債'].apply(format_debt)
        phone_str = df['架電番号'].astype(str)
        result_str = df['架電結果'].fillna('').astype(str)
        output_df['交渉備考'] = phone_str + "オートコール" + result_str + "　残債：" + debt_str + "円"

        return output_df[self.OUTPUT_COLUMNS]

    def generate_output_filename(self, extension: str = 'csv') -> str:
        """
        出力ファイル名を生成

        Args:
            extension: ファイル拡張子（デフォルト: 'csv'）

        Returns:
            MMDDオートコール履歴.{extension}形式のファイル名
        """
        today = datetime.now()
        mmdd = today.strftime('%m%d')
        return f"{mmdd}オートコール履歴.{extension}"

    def generate_csv(self, df: pd.DataFrame) -> Tuple[bytes, List[str]]:
        """
        DataFrameをCSV形式に変換

        Args:
            df: 出力するDataFrame

        Returns:
            Tuple[bytes, List[str]]: CSVバイト列とログ
        """
        logs = []

        # CSVファイルを作成（CP932エンコーディング）
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='cp932')
        logs.append(f"オートコール履歴: {len(df)}件")

        csv_buffer.seek(0)
        return csv_buffer.getvalue(), logs
