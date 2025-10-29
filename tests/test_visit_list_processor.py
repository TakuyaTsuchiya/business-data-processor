"""
訪問リスト作成プロセッサのユニットテスト
Business Data Processor

TDDで実装: Red-Green-Refactorサイクル
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import io


class TestAddressCombination:
    """住所結合処理のテスト"""

    def test_combine_address_3つ全て結合(self):
        """現住所1+2+3がすべて存在する場合、正しく結合されることを確認"""
        from processors.visit_list.processor import combine_address
        result = combine_address("北海道", "札幌市", "北区1-1-1")
        assert result == "北海道札幌市北区1-1-1"

    def test_combine_address_現住所3が空白(self):
        """現住所3が空白の場合、現住所1+2のみ結合されることを確認"""
        from processors.visit_list.processor import combine_address
        result = combine_address("北海道", "札幌市", "")
        assert result == "北海道札幌市"

    def test_combine_address_すべて空白(self):
        """すべて空白の場合、空文字が返されることを確認"""
        from processors.visit_list.processor import combine_address
        result = combine_address("", "", "")
        assert result == ""


class TestPrefectureOrder:
    """都道府県順序のテスト"""

    def test_prefecture_order_北海道が0番(self):
        """北海道が0番であることを確認"""
        from processors.common.prefecture_order import get_prefecture_order
        assert get_prefecture_order("北海道") == 0

    def test_prefecture_order_沖縄県が46番(self):
        """沖縄県が46番であることを確認"""
        from processors.common.prefecture_order import get_prefecture_order
        assert get_prefecture_order("沖縄県") == 46

    def test_extract_prefecture_from_address(self):
        """住所から都道府県を正しく抽出できることを確認"""
        from processors.common.prefecture_order import extract_prefecture_from_address
        assert extract_prefecture_from_address("北海道札幌市北区") == "北海道"
        assert extract_prefecture_from_address("東京都新宿区") == "東京都"
        assert extract_prefecture_from_address("不明な住所") == ""


class TestFiltering:
    """フィルタリング処理のテスト"""

    def test_filter_records_5条件統合(self):
        """5つのフィルタ条件を満たすレコードのみが残ることを確認"""
        from processors.visit_list.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成（必要な列のみ実データ）
        data = {i: [None] * 6 for i in range(121)}
        data[0] = [1001, 1002, 1003, 1004, 1005, 1006]  # 管理番号
        data[86] = ["交渉困難", "死亡決定", "弁護士介入", "交渉継続中", "交渉困難", "追跡調査中"]  # 回収ランク
        data[72] = [today - timedelta(days=1), today, today + timedelta(days=1),
                    today - timedelta(days=5), today - timedelta(days=2), today + timedelta(days=3)]  # 入金予定日
        data[73] = [10, 2, 5, 10, 3, 15]  # 入金予定金額
        data[118] = ['5', None, '10', '5', '', '3']  # 委託先法人ID
        data[23] = ["北海道", "東京都", "", "大阪府", "神奈川県", "沖縄県"]  # 現住所1

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # 期待: 1001のみ（交渉困難, 昨日, 10, '5', 北海道）
        assert len(filtered_df) == 1
        assert filtered_df.iloc[0, 0] == 1001


class TestDataExtraction:
    """データ抽出と22列マッピングのテスト"""

    def test_create_output_row_契約者(self):
        """ContractListの1行から22列の出力行を作成できることを確認"""
        from processors.visit_list.processor import create_output_row, VisitListConfig

        # 121列のダミーデータ作成
        data = {i: [None] for i in range(121)}
        data[0] = [1001]  # 管理番号
        data[2] = ["レントワン"]  # 最新契約種類
        data[14] = ["入居中"]  # 入居ステータス
        data[15] = ["滞納"]  # 滞納ステータス
        data[17] = [0]  # 退去手続き（実費）
        data[19] = ["営業太郎"]  # 営業担当者
        data[20] = ["田中太郎"]  # 契約者氏名
        data[23] = ["北海道"]  # 現住所1
        data[24] = ["札幌市"]  # 現住所2
        data[25] = ["北区1-1-1"]  # 現住所3
        data[71] = [50000]  # 滞納残債
        data[72] = ["2025-01-15"]  # 入金予定日
        data[73] = [10000]  # 入金予定金額
        data[84] = [80000]  # 月額賃料合計
        data[86] = ["交渉困難"]  # 回収ランク
        data[97] = ["C001"]  # クライアントCD
        data[98] = ["テスト管理会社"]  # クライアント名
        data[118] = ["5"]  # 委託先法人ID
        data[119] = ["テスト委託先"]  # 委託先法人名
        data[120] = ["2025-03-31"]  # 解約日

        df = pd.DataFrame(data)
        row = df.iloc[0]

        config = VisitListConfig.PERSON_TYPES["contractor"]
        output = create_output_row(row, "contractor", config)

        # 22列の検証
        assert output["管理番号"] == 1001
        assert output["最新契約種類"] == "レントワン"
        assert output["入居ステータス"] == "入居中"
        assert output["[人物]氏名"] == "田中太郎"  # シートによって変わるプレースホルダー
        assert output["現住所1"] == "北海道"
        assert output["回収ランク"] == "交渉困難"

    def test_create_output_row_結合住所(self):
        """結合住所列が正しく生成されることを確認"""
        from processors.visit_list.processor import create_output_row, VisitListConfig

        data = {i: [None] for i in range(121)}
        data[0] = [1001]
        data[23] = ["北海道"]
        data[24] = ["札幌市"]
        data[25] = ["北区1-1-1"]

        df = pd.DataFrame(data)
        row = df.iloc[0]

        config = VisitListConfig.PERSON_TYPES["contractor"]
        output = create_output_row(row, "contractor", config)

        # 9列目の空白ヘッダー（結合住所）を確認
        output_list = list(output.items())
        # 9列目（インデックス8）が結合住所であることを確認
        assert output_list[8][0] == ""  # 空白ヘッダー
        assert output_list[8][1] == "北海道札幌市北区1-1-1"  # 結合住所
