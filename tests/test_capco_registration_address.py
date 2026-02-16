#!/usr/bin/env python3
"""
カプコ新規登録 住所分割テスト

テスト対象:
- DataConverter.split_address() メソッド
- 市名に「村」「町」「市」を含む住所の正しい分割
- 政令指定都市、東京23区、郡パターンの対応
- 建物名・部屋名の組み立て（address3, property_address3）
"""

from processors.capco_registration import DataConverter


class TestCapcoSplitAddressMuraCity:
    """市名に「村」を含む住所の分割テスト（バグ修正対象）"""

    def setup_method(self):
        self.converter = DataConverter()

    def test_higashimurayama(self):
        """東村山市: 市名に「村」を含む"""
        result = self.converter.split_address(
            "東京都東村山市恩多町5-25-2", "コーポ江広", "203"
        )
        assert result["prefecture"] == "東京都"
        assert result["city"] == "東村山市"
        assert "恩多町5-25-2" in result["address3"]
        assert "コーポ江広" in result["address3"]
        assert "203" in result["address3"]
        assert result["property_address3"] == "恩多町5-25-2"

    def test_musashimurayama(self):
        """武蔵村山市: 市名に「村」を含む"""
        result = self.converter.split_address(
            "東京都武蔵村山市学園3-38-1", "藤ハイツ(武蔵村山市)", "101"
        )
        assert result["prefecture"] == "東京都"
        assert result["city"] == "武蔵村山市"
        assert "学園3-38-1" in result["address3"]
        assert result["property_address3"] == "学園3-38-1"

    def test_hamura(self):
        """羽村市: 市名に「村」を含む"""
        result = self.converter.split_address(
            "東京都羽村市羽西1-10-10", "木村ハイツ", "102"
        )
        assert result["prefecture"] == "東京都"
        assert result["city"] == "羽村市"
        assert "羽西1-10-10" in result["address3"]
        assert result["property_address3"] == "羽西1-10-10"


class TestCapcoSplitAddressSpecialCityNames:
    """市名に「町」「市」を含む住所の分割テスト"""

    def setup_method(self):
        self.converter = DataConverter()

    def test_machida(self):
        """町田市: 市名に「町」を含む"""
        result = self.converter.split_address("東京都町田市成瀬が丘1-10-24", "", "")
        assert result["prefecture"] == "東京都"
        assert result["city"] == "町田市"
        assert result["property_address3"] == "成瀬が丘1-10-24"

    def test_ichikawa(self):
        """市川市: 市名が「市」で始まる"""
        result = self.converter.split_address("千葉県市川市八幡2-1-1", "テストビル", "")
        assert result["prefecture"] == "千葉県"
        assert result["city"] == "市川市"
        assert "八幡2-1-1" in result["address3"]

    def test_ichihara(self):
        """市原市: 市名が「市」で始まる"""
        result = self.converter.split_address("千葉県市原市五井中央西1-1-25", "", "")
        assert result["prefecture"] == "千葉県"
        assert result["city"] == "市原市"
        assert result["property_address3"] == "五井中央西1-1-25"


class TestCapcoSplitAddressDesignatedCity:
    """政令指定都市の分割テスト"""

    def setup_method(self):
        self.converter = DataConverter()

    def test_yokohama_kohoku(self):
        """横浜市港北区: 政令指定都市"""
        result = self.converter.split_address(
            "神奈川県横浜市港北区新吉田東6-61-10", "ベルピア", ""
        )
        assert result["prefecture"] == "神奈川県"
        assert result["city"] == "横浜市港北区"
        assert "新吉田東6-61-10" in result["address3"]

    def test_kawasaki_takatsu(self):
        """川崎市高津区: 政令指定都市"""
        result = self.converter.split_address(
            "神奈川県川崎市高津区二子3-33-28", "グランデージュ栄", ""
        )
        assert result["prefecture"] == "神奈川県"
        assert result["city"] == "川崎市高津区"

    def test_sagamihara_midori(self):
        """相模原市緑区: 政令指定都市"""
        result = self.converter.split_address(
            "神奈川県相模原市緑区町屋3-2-2", "オラシオン", ""
        )
        assert result["prefecture"] == "神奈川県"
        assert result["city"] == "相模原市緑区"


class TestCapcoSplitAddressNormalAndEdge:
    """通常パターン・エッジケースのテスト"""

    def setup_method(self):
        self.converter = DataConverter()

    def test_normal_city(self):
        """通常の市: 厚木市"""
        result = self.converter.split_address(
            "神奈川県厚木市飯山2240-1", "コーポコジマ", ""
        )
        assert result["prefecture"] == "神奈川県"
        assert result["city"] == "厚木市"
        assert "飯山2240-1" in result["address3"]

    def test_empty_address(self):
        """空住所"""
        result = self.converter.split_address("", "", "")
        assert result["prefecture"] == ""
        assert result["city"] == ""
        assert result["address3"] == ""

    def test_none_address(self):
        """None住所"""
        result = self.converter.split_address(None, None, None)
        assert result["prefecture"] == ""
        assert result["city"] == ""

    def test_building_and_room_assembly(self):
        """address3に建物名・部屋名が全角スペースで結合されること"""
        result = self.converter.split_address(
            "東京都新宿区西新宿1-1-1", "テストビル", "301"
        )
        assert result["address3"] == "西新宿1-1-1　テストビル　301"
        assert result["property_address3"] == "西新宿1-1-1"

    def test_no_building_no_room(self):
        """建物名・部屋名なしの場合、address3はremainingのみ"""
        result = self.converter.split_address("東京都新宿区西新宿1-1-1", "", "")
        assert result["address3"] == "西新宿1-1-1"
        assert result["property_address3"] == "西新宿1-1-1"
