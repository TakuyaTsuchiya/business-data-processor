"""
訪問リスト作成（バックレント用）プロセッサのユニットテスト
Business Data Processor

TDDで実装: Red-Green-Refactorサイクル
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import io


class TestFilteringBackrent:
    """バックレント用フィルタリング処理のテスト"""

    def test_filter_records_入居ステータス_入居中と退去済のみ(self):
        """入居ステータス「入居中」「退去済」のみが対象になることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 4 for i in range(121)}
        data[0] = [1001, 1002, 1003, 1004]  # 管理番号
        data[13] = ["契約中", "契約中", "契約中", "契約中"]  # 受託状況
        data[14] = ["入居中", "退去済", "退去予定", "明渡済"]  # 入居ステータス
        data[86] = ["追跡調査中", "追跡調査中", "追跡調査中", "追跡調査中"]  # 回収ランク
        data[72] = [today - timedelta(days=1)] * 4  # 入金予定日
        data[73] = [10] * 4  # 入金予定金額
        data[118] = ['5'] * 4  # 委託先法人ID
        data[71] = ["10,000"] * 4  # 滞納残債
        data[97] = ["1000"] * 4  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # 期待: 1001（入居中）と1002（退去済）のみ
        result_ids = filtered_df.iloc[:, 0].tolist()
        assert 1001 in result_ids
        assert 1002 in result_ids
        assert 1003 not in result_ids  # 退去予定は除外
        assert 1004 not in result_ids  # 明渡済は除外

    def test_filter_records_委託先法人ID_2_3_4_5_空白のみ対象(self):
        """委託先法人ID「2」「3」「4」「5」「空白」のみが対象になることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 7 for i in range(121)}
        data[0] = [2001, 2002, 2003, 2004, 2005, 2006, 2007]  # 管理番号
        data[13] = ["契約中"] * 7  # 受託状況
        data[14] = ["入居中"] * 7  # 入居ステータス
        data[86] = ["追跡調査中"] * 7  # 回収ランク
        data[72] = [today - timedelta(days=1)] * 7  # 入金予定日
        data[73] = [10] * 7  # 入金予定金額
        data[118] = ['1', '2', '3', '4', '5', '6', '']  # 委託先法人ID
        data[71] = ["10,000"] * 7  # 滞納残債
        data[97] = ["1000"] * 7  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # 期待: 2002（ID=2）、2003（ID=3）、2004（ID=4）、2005（ID=5）、2007（空白）のみ
        result_ids = filtered_df.iloc[:, 0].tolist()
        assert 2001 not in result_ids  # ID=1は除外
        assert 2002 in result_ids      # ID=2は対象
        assert 2003 in result_ids      # ID=3は対象
        assert 2004 in result_ids      # ID=4は対象
        assert 2005 in result_ids      # ID=5は対象
        assert 2006 not in result_ids  # ID=6は除外
        assert 2007 in result_ids      # 空白は対象

    def test_filter_records_クライアントCD_7と9268を除外(self):
        """クライアントCD「7」「9268」が除外されることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 4 for i in range(121)}
        data[0] = [3001, 3002, 3003, 3004]  # 管理番号
        data[13] = ["契約中"] * 4  # 受託状況
        data[14] = ["入居中"] * 4  # 入居ステータス
        data[86] = ["追跡調査中"] * 4  # 回収ランク
        data[72] = [today - timedelta(days=1)] * 4  # 入金予定日
        data[73] = [10] * 4  # 入金予定金額
        data[118] = ['5'] * 4  # 委託先法人ID
        data[71] = ["10,000"] * 4  # 滞納残債
        data[97] = ["1000", "7", "9268", "8000"]  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # 期待: 3001（CD=1000）と3004（CD=8000）のみ
        result_ids = filtered_df.iloc[:, 0].tolist()
        assert 3001 in result_ids      # CD=1000は対象
        assert 3002 not in result_ids  # CD=7は除外
        assert 3003 not in result_ids  # CD=9268は除外
        assert 3004 in result_ids      # CD=8000は対象

    def test_filter_records_8条件統合(self):
        """8つのフィルタ条件を満たすレコードのみが残ることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 8 for i in range(121)}
        data[0] = [4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008]  # 管理番号
        data[13] = ["契約中", "契約中", "契約中", "解約", "契約中", "契約中", "契約中", "契約中"]  # 受託状況
        data[14] = ["入居中", "退去予定", "入居中", "入居中", "入居中", "入居中", "入居中", "入居中"]  # 入居ステータス
        data[86] = ["追跡調査中", "追跡調査中", "死亡決定", "追跡調査中", "追跡調査中", "追跡調査中", "追跡調査中", "追跡調査中"]  # 回収ランク
        data[72] = [today - timedelta(days=1), today - timedelta(days=1), today - timedelta(days=1),
                    today - timedelta(days=1), today + timedelta(days=5), today - timedelta(days=1),
                    today - timedelta(days=1), today - timedelta(days=1)]  # 入金予定日
        data[73] = [10, 10, 10, 10, 10, 2, 10, 10]  # 入金予定金額
        data[118] = ['5', '5', '5', '5', '5', '5', '1', '5']  # 委託先法人ID
        data[71] = ["10,000", "10,000", "10,000", "10,000", "10,000", "10,000", "10,000", "0"]  # 滞納残債
        data[97] = ["1000", "1000", "1000", "1000", "1000", "1000", "1000", "7"]  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # 期待: 4001のみ（全条件を満たす）
        # 4002: 入居ステータス=退去予定で除外
        # 4003: 回収ランク=死亡決定で除外
        # 4004: 受託状況=解約で除外
        # 4005: 入金予定日が未来で除外
        # 4006: 入金予定金額=2で除外
        # 4007: 委託先法人ID=1で除外
        # 4008: クライアントCD=7で除外
        assert len(filtered_df) == 1
        assert filtered_df.iloc[0, 0] == 4001

    def test_filter_records_回収ランク除外の詳細ログ(self):
        """回収ランクフィルタのログに除外内訳が表示されることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 5 for i in range(121)}
        data[0] = [5001, 5002, 5003, 5004, 5005]  # 管理番号
        data[13] = ["契約中"] * 5  # 受託状況
        data[14] = ["入居中"] * 5  # 入居ステータス
        data[86] = ["交渉困難", "死亡決定", "弁護士介入", "追跡調査中", "追跡調査中"]  # 回収ランク
        data[72] = [today - timedelta(days=1)] * 5  # 入金予定日
        data[73] = [10] * 5  # 入金予定金額
        data[118] = ['5'] * 5  # 委託先法人ID
        data[71] = ["10,000"] * 5  # 滞納残債
        data[97] = ["1000"] * 5  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # ログに除外内訳が含まれていることを確認
        rank_log = [log for log in logs if "回収ランクフィルタ" in log][0]
        assert "交渉困難: 1件" in rank_log
        assert "死亡決定: 1件" in rank_log
        assert "弁護士介入: 1件" in rank_log

    def test_filter_records_入居ステータスの詳細ログ(self):
        """入居ステータスフィルタのログに残存内訳が表示されることを確認"""
        from processors.visit_list_backrent.processor import filter_records

        today = datetime.now().date()

        # 121列のダミーデータ作成
        data = {i: [None] * 4 for i in range(121)}
        data[0] = [6001, 6002, 6003, 6004]  # 管理番号
        data[13] = ["契約中"] * 4  # 受託状況
        data[14] = ["入居中", "入居中", "退去済", "退去予定"]  # 入居ステータス
        data[86] = ["追跡調査中"] * 4  # 回収ランク
        data[72] = [today - timedelta(days=1)] * 4  # 入金予定日
        data[73] = [10] * 4  # 入金予定金額
        data[118] = ['5'] * 4  # 委託先法人ID
        data[71] = ["10,000"] * 4  # 滞納残債
        data[97] = ["1000"] * 4  # クライアントCD

        df = pd.DataFrame(data)
        filtered_df, logs = filter_records(df)

        # ログに残存内訳が含まれていることを確認
        status_log = [log for log in logs if "入居ステータスフィルタ" in log][0]
        assert "入居中: 2件" in status_log
        assert "退去済: 1件" in status_log


class TestAddressCombinationBackrent:
    """住所結合処理のテスト"""

    def test_combine_address_3つ全て結合(self):
        """現住所1+2+3がすべて存在する場合、正しく結合されることを確認"""
        from processors.visit_list_backrent.processor import combine_address
        result = combine_address("北海道", "札幌市", "北区1-1-1")
        assert result == "北海道札幌市北区1-1-1"


class TestDataExtractionBackrent:
    """データ抽出と22列マッピングのテスト"""

    def test_create_output_row_契約者(self):
        """ContractListの1行から22列の出力行を作成できることを確認"""
        from processors.visit_list_backrent.processor import create_output_row, VisitListBackrentConfig

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
        data[86] = ["追跡調査中"]  # 回収ランク
        data[97] = ["C001"]  # クライアントCD
        data[98] = ["テスト管理会社"]  # クライアント名
        data[118] = ["5"]  # 委託先法人ID
        data[119] = ["テスト委託先"]  # 委託先法人名
        data[120] = ["2025-03-31"]  # 解約日

        df = pd.DataFrame(data)
        row = df.iloc[0]

        config = VisitListBackrentConfig.PERSON_TYPES["contractor"]
        output = create_output_row(row, "contractor", config)

        # 22列の検証
        assert output["管理番号"] == 1001
        assert output["最新契約種類"] == "レントワン"
        assert output["入居ステータス"] == "入居中"
        assert output["契約者氏名"] == "田中太郎"
        assert output["現住所1"] == "北海道"
        assert output["回収ランク"] == "追跡調査中"
