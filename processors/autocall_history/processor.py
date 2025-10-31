"""
オートコール履歴プロセッサー

list_export*.csvからオートコール履歴データを生成する。
NegotiatesInfoSample.csvのフォーマットに従って出力。
"""

import pandas as pd
from typing import Optional
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
        output_df['交渉日時'] = df['最終架電日']
        output_df['担当'] = ''
        output_df['相手'] = self.target_person
        output_df['手段'] = '架電'
        output_df['回収ランク'] = ''
        output_df['結果'] = 'その他'
        output_df['入金予定日'] = ''
        output_df['予定金額'] = ''

        # 交渉備考: f"架電番号{架電番号}オートコール　残債{残債}円"
        output_df['交渉備考'] = df.apply(
            lambda row: f"架電番号{row['架電番号']}オートコール　残債{row['残債']}円",
            axis=1
        )

        return output_df[self.OUTPUT_COLUMNS]

    def generate_output_filename(self) -> str:
        """
        出力ファイル名を生成

        Returns:
            MMDDオートコール履歴.csv形式のファイル名
        """
        today = datetime.now()
        mmdd = today.strftime('%m%d')
        return f"{mmdd}オートコール履歴.csv"
