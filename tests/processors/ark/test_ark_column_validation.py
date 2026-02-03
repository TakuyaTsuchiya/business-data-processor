"""
アーク新規登録 列名チェック機能のテスト

TDD: Red-Green-Refactor
このテストは check_expected_columns 関数の実装前に作成
"""

import pandas as pd

from processors.ark_registration import check_expected_columns


class TestCheckExpectedColumns:
    """check_expected_columns関数のテスト"""

    def test_all_columns_exist_returns_empty_list(self):
        """全ての期待列が存在する場合、空のリストを返す"""
        df = pd.DataFrame(columns=["契約番号", "契約元帳: 主契約者", "携帯TEL1"])
        expected = ["契約番号", "契約元帳: 主契約者", "携帯TEL1"]

        result = check_expected_columns(df, expected)

        assert result == []

    def test_missing_column_returns_warning(self):
        """存在しない列がある場合、警告メッセージを返す"""
        df = pd.DataFrame(columns=["契約番号"])
        expected = ["契約番号", "携帯TEL2"]

        result = check_expected_columns(df, expected)

        assert len(result) == 1
        assert "携帯TEL2" in result[0]

    def test_alternative_column_exists_no_warning(self):
        """代替列名が存在する場合、警告なし"""
        df = pd.DataFrame(columns=["名前2"])  # 旧形式
        expected = ["氏名2"]  # 新形式
        alternatives = {"氏名2": "名前2"}

        result = check_expected_columns(df, expected, alternatives)

        assert result == []

    def test_neither_primary_nor_alternative_exists_returns_warning(self):
        """新形式も旧形式も存在しない場合、警告を返す"""
        df = pd.DataFrame(columns=["その他の列"])
        expected = ["氏名2"]
        alternatives = {"氏名2": "名前2"}

        result = check_expected_columns(df, expected, alternatives)

        assert len(result) == 1
        assert "氏名2" in result[0]

    def test_empty_dataframe_returns_all_warnings(self):
        """空のDataFrameの場合、全ての期待列について警告"""
        df = pd.DataFrame()
        expected = ["契約番号", "携帯TEL1"]

        result = check_expected_columns(df, expected)

        assert len(result) == 2

    def test_multiple_missing_columns(self):
        """複数の列が見つからない場合、それぞれの警告を返す"""
        df = pd.DataFrame(columns=["契約番号"])
        expected = ["契約番号", "携帯TEL2", "自宅住所2", "生年月日2"]

        result = check_expected_columns(df, expected)

        assert len(result) == 3
        assert any("携帯TEL2" in w for w in result)
        assert any("自宅住所2" in w for w in result)
        assert any("生年月日2" in w for w in result)

    def test_warning_message_format(self):
        """警告メッセージのフォーマットを検証"""
        df = pd.DataFrame(columns=[])
        expected = ["携帯TEL2"]

        result = check_expected_columns(df, expected)

        assert len(result) == 1
        # 警告メッセージに列名が含まれていることを確認
        assert "携帯TEL2" in result[0]
        # 「見つかりません」という文言が含まれていることを確認
        assert "見つかりません" in result[0]

    def test_alternatives_none_treated_as_no_alternatives(self):
        """column_alternativesがNoneの場合、代替列名なしとして処理"""
        df = pd.DataFrame(columns=["契約番号"])
        expected = ["契約番号", "氏名2"]

        result = check_expected_columns(df, expected, None)

        assert len(result) == 1
        assert "氏名2" in result[0]

    def test_empty_expected_columns_returns_empty_list(self):
        """期待列リストが空の場合、空のリストを返す"""
        df = pd.DataFrame(columns=["契約番号", "携帯TEL1"])
        expected = []

        result = check_expected_columns(df, expected)

        assert result == []

    def test_partial_alternative_match(self):
        """一部の列のみ代替列名で見つかる場合"""
        df = pd.DataFrame(columns=["名前2", "氏名3"])  # 名前2は旧形式、氏名3は新形式
        expected = ["氏名2", "氏名3"]  # 両方新形式で期待
        alternatives = {"氏名2": "名前2", "氏名3": "名前3"}

        result = check_expected_columns(df, expected, alternatives)

        # 氏名2は名前2として存在、氏名3は直接存在 → 警告なし
        assert result == []
