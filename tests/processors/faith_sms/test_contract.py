"""
フェイスSMS契約者プロセッサの固有テスト

contract.pyの固有仕様:
- Filter 6: TEL携帯（列名）を使用
- 出力の電話番号にTEL携帯をマッピング
- 保証人='', 連絡人=''
- ファイル名: {MMDD}フェイスSMS契約者.csv

共通フィルタ（Filter 1〜5）のテストは test_common_filters.py を参照。
"""

import pandas as pd
from datetime import datetime, date

from processors.faith_sms.contract import process_faith_sms_contract_data
from processors.sms_common.utils import format_payment_deadline

from tests.processors.faith_sms.conftest import (
    create_faith_sms_dataframe,
    dataframe_to_csv_bytes,
    create_valid_faith_row,
)


def _run_contract(rows, payment_deadline_date):
    df = create_faith_sms_dataframe(rows)
    csv_bytes = dataframe_to_csv_bytes(df)
    return process_faith_sms_contract_data(csv_bytes, payment_deadline_date)


class TestFaithSmsContractIntegration:
    """契約者プロセッサの統合テスト"""

    def test_valid_data_produces_output(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        result_df, logs, filename, stats = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1
        assert stats["processed_rows"] == 1

    def test_output_has_59_columns(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df.columns) == 59

    def test_output_filename_format(self, payment_deadline_date):
        rows = [create_valid_faith_row()]
        _, _, filename, _ = _run_contract(rows, payment_deadline_date)
        date_str = datetime.now().strftime("%m%d")
        assert filename == f"{date_str}フェイスSMS契約者.csv"

    def test_output_mapping(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(
                契約者氏名="山田太郎",
                物件名="サンプルマンション",
                物件番号="202",
                管理番号="F001",
                回収口座銀行名="みずほ銀行",
                回収口座支店名="渋谷支店",
                回収口座種類="普通",
                回収口座番号="9876543",
                回収口座名義人="ヤマダタロウ",
            )
        ]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)

        row = result_df.iloc[0]
        assert row["電話番号"] == "090-1234-5678"
        assert row["(info1)契約者名"] == "山田太郎"
        assert "サンプルマンション" in row["(info2)物件名"]
        assert "202" in row["(info2)物件名"]
        assert row["(info3)金額"] == "10,000"
        assert "みずほ銀行" in row["(info4)銀行口座"]
        assert "渋谷支店" in row["(info4)銀行口座"]
        assert row["(info5)メモ"] == "F001"
        assert row["支払期限"] == format_payment_deadline(date(2025, 12, 31))
        assert row["支払期限"] == "2025年12月31日"
        assert pd.isna(row["保証人"]) or row["保証人"] == ""
        assert pd.isna(row["連絡人"]) or row["連絡人"] == ""


class TestPhoneFilter:
    """TEL携帯フィルター（090/080/070-XXXX-XXXX形式）"""

    def test_valid_090_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(TEL携帯="090-1234-5678")]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_080_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(TEL携帯="080-1234-5678")]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_070_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(TEL携帯="070-1234-5678")]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_landline_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", TEL携帯="090-1234-5678"),
            create_valid_faith_row(契約者氏名="除外", TEL携帯="03-1234-5678"),
        ]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", TEL携帯="090-1234-5678"),
            create_valid_faith_row(契約者氏名="除外", TEL携帯=""),
        ]
        result_df, _, _, _ = _run_contract(rows, payment_deadline_date)
        assert len(result_df) == 1
