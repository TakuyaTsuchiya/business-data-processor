"""
process_contact のテスト
"""

from processors.faith_notification import process_contact, process_faith_notification
from tests.processors.faith_notification.conftest import (
    COL,
    create_notification_dataframe,
    _base_valid_row_data,
)


CONTACT_COLUMNS = [
    "管理番号",
    "契約者氏名",
    "緊急連絡人１氏名",
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


class TestContact1:
    """緊急連絡人1のテスト"""

    def test_contact1_complete_produces_output(self):
        """連絡人1の全フィールドが揃っている場合、番号='①'で出力される"""
        row = _base_valid_row_data()
        # 連絡人2を空にする
        row[COL["CONTACT2_NAME"]] = ""
        row[COL["CONTACT2_POSTAL"]] = ""
        row[COL["CONTACT2_ADDR1"]] = ""
        row[COL["CONTACT2_ADDR2"]] = ""
        row[COL["CONTACT2_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_contact(df)
        assert len(result) == 1
        assert result.iloc[0]["番号"] == "①"
        assert result.iloc[0]["緊急連絡人１氏名"] == "連絡一郎"


class TestContact2:
    """緊急連絡人2のテスト"""

    def test_contact2_complete_produces_output(self):
        """連絡人2の全フィールドが揃っている場合、番号='②'で出力される"""
        row = _base_valid_row_data()
        # 連絡人1を空にする
        row[COL["CONTACT1_NAME"]] = ""
        row[COL["CONTACT1_POSTAL"]] = ""
        row[COL["CONTACT1_ADDR1"]] = ""
        row[COL["CONTACT1_ADDR2"]] = ""
        row[COL["CONTACT1_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_contact(df)
        assert len(result) == 1
        assert result.iloc[0]["番号"] == "②"
        assert result.iloc[0]["緊急連絡人１氏名"] == "連絡二郎"


class TestBothContacts:
    """両連絡人のテスト"""

    def test_both_contacts_concat(self):
        """連絡人1と2の両方が揃っている場合、2行の結果が返る"""
        row = _base_valid_row_data()
        df = create_notification_dataframe([row])
        result, _ = process_contact(df)
        assert len(result) == 2
        numbers = result["番号"].tolist()
        assert "①" in numbers
        assert "②" in numbers


class TestNoContacts:
    """連絡人データなしのテスト"""

    def test_no_contacts_empty_df(self):
        """連絡人データがない場合、正しい列名の空DataFrameが返る"""
        row = _base_valid_row_data()
        row[COL["CONTACT1_NAME"]] = ""
        row[COL["CONTACT1_POSTAL"]] = ""
        row[COL["CONTACT1_ADDR1"]] = ""
        row[COL["CONTACT1_ADDR2"]] = ""
        row[COL["CONTACT1_ADDR3"]] = ""
        row[COL["CONTACT2_NAME"]] = ""
        row[COL["CONTACT2_POSTAL"]] = ""
        row[COL["CONTACT2_ADDR1"]] = ""
        row[COL["CONTACT2_ADDR2"]] = ""
        row[COL["CONTACT2_ADDR3"]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_contact(df)
        assert len(result) == 0
        assert list(result.columns) == CONTACT_COLUMNS


class TestContactE2E:
    """連絡人E2Eテスト"""

    def test_e2e_contact(self):
        """process_faith_notification経由でcontactを処理"""
        row = _base_valid_row_data()
        df = create_notification_dataframe([row])
        result_df, filename, message, logs = process_faith_notification(df, "contact")
        assert len(result_df) == 2
        assert "連絡人" in filename
        assert list(result_df.columns) == CONTACT_COLUMNS
