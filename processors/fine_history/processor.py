"""
ファイン履歴プロセッサー

携帯Mirail社納品データ_*.csvからファイン履歴データを生成する。
NegotiatesInfoSample.csv形式（10列）で出力。
"""

import pandas as pd
import io
from typing import Tuple, List
from datetime import datetime


class FineHistoryProcessor:
    """ファイン履歴データ処理クラス"""

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

    # 必須カラム定義
    REQUIRED_COLUMNS = ['管理番号', '架電先', '発信日', '発信時刻']

    def process(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        携帯Mirail社納品データをファイン履歴形式に変換

        Args:
            input_df: 携帯Mirail社納品データから読み込んだDataFrame

        Returns:
            NegotiatesInfoSample.csv形式のDataFrame

        Raises:
            ValueError: 必須カラムが存在しない場合
        """
        # 必須カラムの存在確認
        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in input_df.columns]
        if missing_columns:
            raise ValueError(
                f"必須カラムが見つかりません: {', '.join(missing_columns)}\n"
                f"入力ファイルは「携帯Mirail社納品データ_*.csv」を使用してください。"
            )

        # 処理用にコピー
        df = input_df.copy()

        # 空行を除外（管理番号が空の行を除外）
        df = df[df['管理番号'].notna() & (df['管理番号'] != '')].copy()

        # マッピング処理
        output_df = pd.DataFrame()
        output_df['管理番号'] = df['管理番号']

        # 交渉日時: 発信日 + 発信時刻を結合、秒を削除
        # 例: 2025/12/9 + 15:52:27 → 2025/12/9 15:52
        def format_datetime(row):
            date_str = str(row['発信日']) if pd.notna(row['発信日']) else ''
            time_str = str(row['発信時刻']) if pd.notna(row['発信時刻']) else ''
            if time_str:
                # 秒を削除（HH:MM:SS → HH:MM）
                time_parts = time_str.split(':')
                if len(time_parts) >= 2:
                    time_str = f"{time_parts[0]}:{time_parts[1]}"
            return f"{date_str} {time_str}".strip()

        output_df['交渉日時'] = df.apply(format_datetime, axis=1)
        output_df['担当'] = ''
        output_df['相手'] = self.target_person
        output_df['手段'] = '架電'
        output_df['回収ランク'] = ''
        output_df['結果'] = 'その他'
        output_df['入金予定日'] = ''
        output_df['予定金額'] = ''

        # 交渉備考: 架電先 + "オートコール"
        output_df['交渉備考'] = df['架電先'].fillna('').astype(str) + 'オートコール'

        return output_df[self.OUTPUT_COLUMNS]

    def generate_output_filename(self, extension: str = 'csv') -> str:
        """
        出力ファイル名を生成

        Args:
            extension: ファイル拡張子（デフォルト: 'csv'）

        Returns:
            MMDDファイン履歴.{extension}形式のファイル名
        """
        today = datetime.now()
        mmdd = today.strftime('%m%d')
        return f"{mmdd}ファイン履歴.{extension}"

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
        logs.append(f"ファイン履歴: {len(df)}件")

        csv_buffer.seek(0)
        return csv_buffer.getvalue(), logs
