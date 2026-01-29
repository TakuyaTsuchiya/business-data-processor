#!/usr/bin/env python3
"""
アーク新規登録 保証人/緊急連絡人処理のテスト

テスト対象:
- 列名の新旧両形式対応（名前2/氏名2、全角/半角）
- 名前2が空で名前3に値がある場合の処理
- 名前2と名前3の両方に値がある場合の処理
"""

import pytest
import pandas as pd
import sys
import os

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from processors.ark_registration import DataConverter


class TestProcessGuarantorEmergency:
    """process_guarantor_emergency関数のテスト"""

    @pytest.fixture
    def converter(self):
        """DataConverterインスタンスを作成"""
        return DataConverter()

    # ===========================================
    # 新形式（氏名2、半角括弧、半角数字）のテスト
    # ===========================================

    def test_new_format_guarantor(self, converter):
        """新形式: 氏名2に保証人がある場合、保証人1に出力される"""
        row = pd.Series({
            "氏名2": "田中太郎",
            "氏名2(カナ)": "タナカタロウ",
            "種別／続柄2": "保証人/父母",
            "生年月日2": "1960/01/01",
            "自宅住所2": "100-0001 東京都千代田区千代田1-1",
            "自宅TEL2": "",
            "携帯TEL2": "090-1234-5678",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["guarantor1"]["氏名"] == "田中太郎"
        assert result["guarantor1"]["カナ"] == "タナカタロウ"
        assert result["guarantor1"]["携帯TEL"] == "090-1234-5678"
        assert result["emergency1"] == {}

    def test_new_format_emergency(self, converter):
        """新形式: 氏名2に緊急連絡先がある場合、緊急連絡人1に出力される"""
        row = pd.Series({
            "氏名2": "鈴木花子",
            "氏名2(カナ)": "スズキハナコ",
            "種別／続柄2": "緊急連絡先/父母",
            "生年月日2": "1965/05/15",
            "自宅住所2": "150-0001 東京都渋谷区神宮前1-1",
            "自宅TEL2": "",
            "携帯TEL2": "080-9876-5432",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["emergency1"]["氏名"] == "鈴木花子"
        assert result["emergency1"]["カナ"] == "スズキハナコ"
        assert result["guarantor1"] == {}

    # ===========================================
    # 旧形式（名前2、全角括弧、全角数字）のテスト
    # ===========================================

    def test_old_format_guarantor(self, converter):
        """旧形式: 名前2に保証人がある場合、保証人1に出力される"""
        row = pd.Series({
            "名前2": "山田次郎",
            "名前2（カナ）": "ヤマダジロウ",
            "種別／続柄２": "保証人/兄弟",
            "生年月日2": "1970/03/20",
            "自宅住所2": "530-0001 大阪府大阪市北区梅田1-1",
            "自宅TEL2": "",
            "携帯TEL2": "070-1111-2222",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["guarantor1"]["氏名"] == "山田次郎"
        assert result["guarantor1"]["カナ"] == "ヤマダジロウ"
        assert result["guarantor1"]["携帯TEL"] == "070-1111-2222"

    def test_old_format_emergency(self, converter):
        """旧形式: 名前2に緊急連絡先がある場合、緊急連絡人1に出力される"""
        row = pd.Series({
            "名前2": "佐藤三郎",
            "名前2（カナ）": "サトウサブロウ",
            "種別／続柄２": "緊急連絡先/その他",
            "生年月日2": "1955/12/25",
            "自宅住所2": "600-8001 京都府京都市下京区四条通1-1",
            "自宅TEL2": "",
            "携帯TEL2": "090-3333-4444",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["emergency1"]["氏名"] == "佐藤三郎"
        assert result["emergency1"]["カナ"] == "サトウサブロウ"

    # ===========================================
    # 名前3のフォールバック処理のテスト
    # ===========================================

    def test_name3_fallback_when_name2_empty(self, converter):
        """名前2が空で名前3に緊急連絡先がある場合、名前3から緊急連絡人1に出力される"""
        row = pd.Series({
            # 名前2は空
            "氏名2": "",
            "氏名2(カナ)": "",
            "種別／続柄2": "",
            "名前2": "",
            "名前2（カナ）": "",
            "種別／続柄２": "",
            # 名前3に緊急連絡先
            "氏名3": "川森郁代",
            "氏名3(カナ)": "カワモリイクヨ",
            "種別/続柄3": "緊急連絡先/父母",
            "生年月日3": "1953/06/28",
            "自宅住所3": "061-1423 北海道恵庭市柏木町3-1-22",
            "自宅TEL3": "",
            "携帯TEL3": "090-8373-0712",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["emergency1"]["氏名"] == "川森郁代"
        assert result["emergency1"]["カナ"] == "カワモリイクヨ"
        assert result["emergency1"]["携帯TEL"] == "090-8373-0712"

    def test_name3_ignored_when_name2_has_data(self, converter):
        """名前2に保証人がある場合、名前3は無視される"""
        row = pd.Series({
            # 名前2に保証人
            "氏名2": "丹羽進二",
            "氏名2(カナ)": "ニワシンジ",
            "種別／続柄2": "保証人/父母",
            "生年月日2": "1972/05/24",
            "自宅住所2": "003-0003 北海道札幌市白石区東札幌三条4-5-25-401",
            "自宅TEL2": "",
            "携帯TEL2": "090-6876-5024",
            # 名前3に緊急連絡先（無視されるべき）
            "氏名3": "丹羽花子",
            "氏名3(カナ)": "ニワハナコ",
            "種別/続柄3": "緊急連絡先/配偶者",
            "生年月日3": "1975/08/10",
            "自宅住所3": "003-0003 北海道札幌市白石区東札幌三条4-5-25-401",
            "自宅TEL3": "",
            "携帯TEL3": "090-1111-2222",
        })

        result = converter.process_guarantor_emergency(row)

        # 保証人1には名前2のデータ
        assert result["guarantor1"]["氏名"] == "丹羽進二"
        # 緊急連絡人1は空（名前3は無視）
        assert result["emergency1"] == {}

    # ===========================================
    # エッジケースのテスト
    # ===========================================

    def test_empty_row(self, converter):
        """全て空の場合、両方とも空辞書が返される"""
        row = pd.Series({})

        result = converter.process_guarantor_emergency(row)

        assert result["guarantor1"] == {}
        assert result["emergency1"] == {}

    def test_relationship_type_without_name(self, converter):
        """種別/続柄があっても名前が空の場合、処理されない"""
        row = pd.Series({
            "氏名2": "",
            "氏名2(カナ)": "",
            "種別／続柄2": "保証人/父母",
            "生年月日2": "1960/01/01",
            "自宅住所2": "",
            "自宅TEL2": "",
            "携帯TEL2": "",
        })

        result = converter.process_guarantor_emergency(row)

        assert result["guarantor1"] == {}
        assert result["emergency1"] == {}


class TestGetColumnValue:
    """get_column_value関数のテスト（新旧両形式対応）"""

    @pytest.fixture
    def converter(self):
        return DataConverter()

    def test_new_format_preferred(self, converter):
        """新形式と旧形式の両方がある場合、新形式が優先される"""
        row = pd.Series({
            "氏名2": "新形式の名前",
            "名前2": "旧形式の名前",
        })

        # get_column_valueが実装されたら有効化
        # value = converter.get_column_value(row, "氏名2", "名前2")
        # assert value == "新形式の名前"
        pass

    def test_fallback_to_old_format(self, converter):
        """新形式が空の場合、旧形式にフォールバック"""
        row = pd.Series({
            "氏名2": "",
            "名前2": "旧形式の名前",
        })

        # get_column_valueが実装されたら有効化
        # value = converter.get_column_value(row, "氏名2", "名前2")
        # assert value == "旧形式の名前"
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
