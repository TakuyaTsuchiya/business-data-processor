"""
フェイスSMS緊急連絡人プロセッサの固有テスト

emergency_contact.pyの固有仕様:
- Filter 6: iloc[:, 56]（BE列）を使用
- 出力の電話番号にiloc[:, 56]をマッピング
- 連絡人=緊急連絡人１氏名, 保証人=''
- ファイル名: {MMDD}フェイスSMS連絡人.csv

共通フィルタ（Filter 1〜5）のテストは test_common_filters.py を参照。
"""

import pandas as pd
from datetime import datetime

from processors.faith_sms.emergency_contact import (
    process_faith_sms_emergencycontact_data,
)

from tests.processors.faith_sms.conftest import (
    create_faith_sms_dataframe,
    dataframe_to_csv_bytes,
    create_valid_faith_row,
)


def _run_emergency(rows, payment_deadline_date):
    df = create_faith_sms_dataframe(rows)
    csv_bytes = dataframe_to_csv_bytes(df)
    return process_faith_sms_emergencycontact_data(csv_bytes, payment_deadline_date)


class TestFaithSmsEmergencyContactIntegration:
    """緊急連絡人プロセッサの統合テスト"""

    def test_valid_data_produces_output(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        result_df, logs, filename, stats = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1
        assert stats["processed_rows"] == 1

    def test_output_has_59_columns(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df.columns) == 59

    def test_output_filename_format(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        _, _, filename, _ = _run_emergency(rows, payment_deadline_date)
        date_str = datetime.now().strftime("%m%d")
        assert filename == f"{date_str}フェイスSMS連絡人.csv"

    def test_output_mapping(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(
                **{
                    "緊急連絡人１氏名": "連絡山田",
                    "契約者氏名": "山田太郎",
                    "物件名": "サンプルマンション",
                    "物件番号": "202",
                    "管理番号": "F001",
                    "回収口座銀行名": "みずほ銀行",
                    "回収口座支店名": "渋谷支店",
                    "回収口座種類": "普通",
                    "回収口座番号": "9876543",
                    "回収口座名義人": "ヤマダタロウ",
                    "緊急連絡人TEL": "070-3333-4444",
                }
            )
        ]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)

        row = result_df.iloc[0]
        assert row["電話番号"] == "070-3333-4444"
        assert row["(info1)契約者名"] == "山田太郎"
        assert "サンプルマンション" in row["(info2)物件名"]
        assert row["(info3)金額"] == "10,000"
        assert "みずほ銀行" in row["(info4)銀行口座"]
        assert row["(info5)メモ"] == "F001"
        assert row["支払期限"] != ""
        assert row["連絡人"] == "連絡山田"
        assert pd.isna(row["保証人"]) or row["保証人"] == ""


class TestPhoneFilter:
    """緊急連絡人電話番号フィルター（BE列 iloc[:, 56]、090/080/070-XXXX-XXXX形式）"""

    def test_valid_090_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(緊急連絡人TEL="090-3333-4444")]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_080_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(緊急連絡人TEL="080-3333-4444")]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_070_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(緊急連絡人TEL="070-3333-4444")]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_landline_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 緊急連絡人TEL="090-3333-4444"),
            create_valid_faith_row(契約者氏名="除外", 緊急連絡人TEL="03-1234-5678"),
        ]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 緊急連絡人TEL="090-3333-4444"),
            create_valid_faith_row(契約者氏名="除外", 緊急連絡人TEL=""),
        ]
        result_df, _, _, _ = _run_emergency(rows, payment_deadline_date)
        assert len(result_df) == 1
