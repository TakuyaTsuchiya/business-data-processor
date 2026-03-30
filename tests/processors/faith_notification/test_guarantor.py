"""
process_guarantor のテスト
"""

from processors.faith_notification import process_guarantor, process_faith_notification
from tests.processors.faith_notification.conftest import (
    COL,
    create_notification_dataframe,
    _base_valid_row_data,
)


GUARANTOR_COLUMNS = [
    "管理番号",
    "契約者氏名",
    "連帯保証人名",
    "郵便番号",
    "現住所1",
    "現住所2",
    "現住所3",
    "滞納残債",
    "物件住所1",
    "物件住所2",
    "物件住所3",
    "物件名",
    "物件番号",
    "回収口座銀行CD",
    "回収口座銀行名",
    "回収口座支店CD",
    "回収口座支店名",
    "回収口座種類",
    "回収口座番号",
    "回収口座名義人",
    "番号",
]


class TestGuarantor1:
    """保証人1のテスト"""

    def test_guarantor1_complete_produces_output(self):
        """保証人1の全フィールドが揃っている場合、番号='①'で出力される"""
        row = _base_valid_row_data()
        # 保証人2を空にする
        row[COL["GUARANTOR2_NAME"]] = ""
        row[COL["GUARANTOR2_POSTAL"]] = ""
        row[COL["GUARANTOR2_ADDR1"]] = ""
        row[COL["GUARANTOR2_ADDR2"]] = ""
        row[COL["GUARANTOR2_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_guarantor(df)
        assert len(result) == 1
        assert result.iloc[0]["番号"] == "①"
        assert result.iloc[0]["連帯保証人名"] == "保証一郎"


class TestGuarantor2:
    """保証人2のテスト"""

    def test_guarantor2_complete_produces_output(self):
        """保証人2の全フィールドが揃っている場合、番号='②'で出力される"""
        row = _base_valid_row_data()
        # 保証人1を空にする
        row[COL["GUARANTOR1_NAME"]] = ""
        row[COL["GUARANTOR1_POSTAL"]] = ""
        row[COL["GUARANTOR1_ADDR1"]] = ""
        row[COL["GUARANTOR1_ADDR2"]] = ""
        row[COL["GUARANTOR1_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_guarantor(df)
        assert len(result) == 1
        assert result.iloc[0]["番号"] == "②"
        assert result.iloc[0]["連帯保証人名"] == "保証二郎"


class TestBothGuarantors:
    """両保証人のテスト"""

    def test_both_guarantors_concat(self):
        """保証人1と2の両方が揃っている場合、2行の結果が返る"""
        row = _base_valid_row_data()
        df = create_notification_dataframe([row])
        result, _ = process_guarantor(df)
        assert len(result) == 2
        numbers = result["番号"].tolist()
        assert "①" in numbers
        assert "②" in numbers


class TestNoGuarantors:
    """保証人データなしのテスト"""

    def test_no_guarantors_empty_df(self):
        """保証人データがない場合、正しい列名の空DataFrameが返る"""
        row = _base_valid_row_data()
        row[COL["GUARANTOR1_NAME"]] = ""
        row[COL["GUARANTOR1_POSTAL"]] = ""
        row[COL["GUARANTOR1_ADDR1"]] = ""
        row[COL["GUARANTOR1_ADDR2"]] = ""
        row[COL["GUARANTOR1_ADDR3"]] = ""
        row[COL["GUARANTOR2_NAME"]] = ""
        row[COL["GUARANTOR2_POSTAL"]] = ""
        row[COL["GUARANTOR2_ADDR1"]] = ""
        row[COL["GUARANTOR2_ADDR2"]] = ""
        row[COL["GUARANTOR2_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_guarantor(df)
        assert len(result) == 0
        assert list(result.columns) == GUARANTOR_COLUMNS

    def test_guarantor1_missing_name_excluded(self):
        """保証人1の氏名が空の場合、保証人1は除外される"""
        row = _base_valid_row_data()
        row[COL["GUARANTOR1_NAME"]] = ""
        # 保証人2も空にする
        row[COL["GUARANTOR2_NAME"]] = ""
        row[COL["GUARANTOR2_POSTAL"]] = ""
        row[COL["GUARANTOR2_ADDR1"]] = ""
        row[COL["GUARANTOR2_ADDR2"]] = ""
        row[COL["GUARANTOR2_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_guarantor(df)
        assert len(result) == 0


class TestGuarantorE2E:
    """保証人E2Eテスト"""

    def test_e2e_guarantor(self):
        """process_faith_notification経由でguarantorを処理"""
        row = _base_valid_row_data()
        df = create_notification_dataframe([row])
        result_df, filename, message, logs = process_faith_notification(df, "guarantor")
        assert len(result_df) == 2
        assert "連帯保証人" in filename
        assert list(result_df.columns) == GUARANTOR_COLUMNS
