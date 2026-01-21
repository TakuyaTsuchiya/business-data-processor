#!/usr/bin/env python3
"""
ガレージバンク新規登録プロセッサのテスト
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import patch, Mock


class TestGBConfig:
    """GBConfigクラスのテスト"""

    def test_output_columns_count(self):
        """OUTPUT_COLUMNSが111列あることを確認"""
        from processors.gb_registration import GBConfig
        assert len(GBConfig.OUTPUT_COLUMNS) == 111

    def test_output_columns_structure(self):
        """OUTPUT_COLUMNSの最初と最後の列名を確認"""
        from processors.gb_registration import GBConfig
        assert GBConfig.OUTPUT_COLUMNS[0] == "引継番号"
        assert GBConfig.OUTPUT_COLUMNS[-1] == "登録フラグ"
        assert GBConfig.OUTPUT_COLUMNS[47] == "クライアントCD"

    def test_fixed_values_required_keys(self):
        """FIXED_VALUESに必須キーが含まれることを確認"""
        from processors.gb_registration import GBConfig
        required_keys = [
            "クライアントCD",
            "委託先法人ID",
            "入居ステータス",
            "滞納ステータス",
            "受託状況",
            "契約種類",
            "回収口座金融機関CD",
            "回収口座金融機関名",
            # "回収口座種類" は入力データからマッピングするため固定値ではない
            "物件名",
            "物件住所郵便番号",
            "物件住所1",
            "物件住所2",
            "物件住所3",
        ]
        for key in required_keys:
            assert key in GBConfig.FIXED_VALUES

    def test_fixed_values_content(self):
        """FIXED_VALUESの値が正しいことを確認"""
        from processors.gb_registration import GBConfig
        assert GBConfig.FIXED_VALUES["クライアントCD"] == "9288"
        assert GBConfig.FIXED_VALUES["委託先法人ID"] == "7"
        assert GBConfig.FIXED_VALUES["回収口座金融機関CD"] == "310"
        assert GBConfig.FIXED_VALUES["回収口座金融機関名"] == "GMOあおぞらネット銀行"
        assert GBConfig.FIXED_VALUES["契約種類"] == "バックレント"
        assert GBConfig.FIXED_VALUES["物件名"] == "カシャリ"
        assert GBConfig.FIXED_VALUES["物件住所郵便番号"] == "000-0000"
        assert GBConfig.FIXED_VALUES["物件住所1"] == "東京都"
        assert GBConfig.FIXED_VALUES["物件住所2"] == "リース"
        assert GBConfig.FIXED_VALUES["物件住所3"] == "債権"
        assert GBConfig.FIXED_VALUES["退去済手数料"] == "20"
        assert GBConfig.FIXED_VALUES["入居中滞納手数料"] == "20"
        assert GBConfig.FIXED_VALUES["入居中正常手数料"] == "20"
        assert GBConfig.FIXED_VALUES["月額賃料"] == "0"

    def test_excel_skiprows(self):
        """EXCEL_SKIPROWSが1であることを確認"""
        from processors.gb_registration import GBConfig
        assert GBConfig.EXCEL_SKIPROWS == 1


class TestFormatFunctions:
    """フォーマット関数のテスト"""

    def test_format_zipcode_7digits(self):
        """7桁郵便番号のフォーマットテスト"""
        from processors.gb_registration import format_zipcode
        assert format_zipcode("1000001") == "100-0001"
        assert format_zipcode("0070848") == "007-0848"
        assert format_zipcode("5798003") == "579-8003"

    def test_format_zipcode_already_formatted(self):
        """既にフォーマット済みの郵便番号テスト"""
        from processors.gb_registration import format_zipcode
        assert format_zipcode("100-0001") == "100-0001"
        assert format_zipcode("007-0848") == "007-0848"

    def test_format_zipcode_empty(self):
        """空の郵便番号テスト"""
        from processors.gb_registration import format_zipcode
        assert format_zipcode("") == ""
        assert format_zipcode(None) == ""
        assert format_zipcode(pd.NA) == ""

    def test_format_phone_11digits_mobile(self):
        """11桁携帯電話番号のフォーマットテスト"""
        from processors.gb_registration import format_phone
        assert format_phone("09012345678") == "090-1234-5678"
        assert format_phone("08012345678") == "080-1234-5678"
        assert format_phone("07012345678") == "070-1234-5678"

    def test_format_phone_missing_leading_zero(self):
        """先頭0欠損（Excel数値化）の電話番号テスト"""
        from processors.gb_registration import format_phone
        # 10桁で9始まり → 携帯番号と判定して先頭に0を追加
        assert format_phone("9020743886") == "090-2074-3886"
        assert format_phone("8012345678") == "080-1234-5678"
        assert format_phone("7012345678") == "070-1234-5678"

    def test_format_phone_already_formatted(self):
        """既にフォーマット済みの電話番号テスト"""
        from processors.gb_registration import format_phone
        assert format_phone("090-1234-5678") == "090-1234-5678"
        assert format_phone("080-1234-5678") == "080-1234-5678"

    def test_format_phone_empty(self):
        """空の電話番号テスト"""
        from processors.gb_registration import format_phone
        assert format_phone("") == ""
        assert format_phone(None) == ""
        assert format_phone(pd.NA) == ""

    def test_format_date_iso_format(self):
        """ISO形式日付のフォーマットテスト"""
        from processors.gb_registration import format_date
        assert format_date("1994-03-11") == "1994/3/11"
        assert format_date("2000-01-01") == "2000/1/1"
        assert format_date("1983-11-24") == "1983/11/24"

    def test_format_date_already_formatted(self):
        """既にフォーマット済みの日付テスト"""
        from processors.gb_registration import format_date
        assert format_date("1994/3/11") == "1994/3/11"
        assert format_date("2000/1/1") == "2000/1/1"

    def test_format_date_empty(self):
        """空の日付テスト"""
        from processors.gb_registration import format_date
        assert format_date("") == ""
        assert format_date(None) == ""
        assert format_date(pd.NA) == ""

    def test_remove_spaces(self):
        """スペース除去のテスト"""
        from processors.gb_registration import remove_spaces
        assert remove_spaces("秋場 雄喜") == "秋場雄喜"
        assert remove_spaces("アキバ ユウキ") == "アキバユウキ"
        assert remove_spaces("山田　太郎") == "山田太郎"  # 全角スペース
        assert remove_spaces("田中花子") == "田中花子"  # スペースなし

    def test_format_account_type(self):
        """口座種別のフォーマットテスト"""
        from processors.gb_registration import format_account_type
        assert format_account_type("普通預金") == "普通"
        assert format_account_type("普通") == "普通"
        assert format_account_type("当座預金") == "当座"
        assert format_account_type("当座") == "当座"


class TestAddressSplitter:
    """住所分割のテスト"""

    def test_split_address_designated_city(self):
        """政令指定都市の住所分割テスト"""
        from processors.gb_registration import split_address
        # 札幌市東区
        city, rest = split_address("札幌市東区北四十八条東5丁目2-5")
        assert city == "札幌市東区"
        assert rest == "北四十八条東5丁目2-5"

    def test_split_address_regular_city(self):
        """一般市の住所分割テスト"""
        from processors.gb_registration import split_address
        # 東大阪市
        city, rest = split_address("東大阪市日下町1-1-91")
        assert city == "東大阪市"
        assert rest == "日下町1-1-91"

    def test_split_address_tokyo_ward(self):
        """東京23区の住所分割テスト"""
        from processors.gb_registration import split_address
        # 千代田区
        city, rest = split_address("千代田区丸の内1-1-1")
        assert city == "千代田区"
        assert rest == "丸の内1-1-1"

    def test_split_address_town(self):
        """町の住所分割テスト"""
        from processors.gb_registration import split_address
        # 古賀市（市）
        city, rest = split_address("古賀市谷山759-7")
        assert city == "古賀市"
        assert rest == "谷山759-7"

    def test_combine_address_with_building(self):
        """住所と建物名の結合テスト"""
        from processors.gb_registration import combine_address
        result = combine_address("北四十八条東5丁目2-5", "アレックスN48-202")
        assert result == "北四十八条東5丁目2-5　アレックスN48-202"

    def test_combine_address_without_building(self):
        """建物名なしの住所結合テスト"""
        from processors.gb_registration import combine_address
        result = combine_address("日下町1-1-91", "")
        assert result == "日下町1-1-91"
        result = combine_address("日下町1-1-91", None)
        assert result == "日下町1-1-91"


class TestDuplicateChecker:
    """DuplicateCheckerクラスのテスト"""

    @pytest.fixture
    def sample_contract_df(self):
        """ContractListのサンプルデータ"""
        return pd.DataFrame({
            "引継番号": ["8850", "12509", "13769"],
            "委託先法人ID": ["7", "7", "7"],
            "契約者氏名": ["秋場雄喜", "近藤茉衣", "西山大貴"]
        })

    @pytest.fixture
    def sample_excel_df(self):
        """Excelファイルのサンプルデータ"""
        return pd.DataFrame({
            "ユーザーID（社内管理用）": ["8850", "99999", "88888"],
            "ユーザー名": ["秋場 雄喜", "新規太郎", "新規花子"]
        })

    def test_check_duplicates_new_records(self, sample_excel_df, sample_contract_df):
        """新規レコードの重複チェックテスト"""
        from processors.gb_registration import DuplicateChecker
        checker = DuplicateChecker()

        new_data, existing_data, stats = checker.check_duplicates(
            sample_excel_df, sample_contract_df
        )

        # 8850は既存、99999と88888は新規
        assert len(new_data) == 2
        assert len(existing_data) == 1
        assert "99999" in new_data["ユーザーID（社内管理用）"].values
        assert "88888" in new_data["ユーザーID（社内管理用）"].values
        assert "8850" in existing_data["ユーザーID（社内管理用）"].values

    def test_check_duplicates_all_existing(self, sample_contract_df):
        """全て既存の場合のテスト"""
        from processors.gb_registration import DuplicateChecker
        checker = DuplicateChecker()

        excel_df = pd.DataFrame({
            "ユーザーID（社内管理用）": ["8850", "12509"],
            "ユーザー名": ["秋場 雄喜", "近藤 茉衣"]
        })

        new_data, existing_data, stats = checker.check_duplicates(
            excel_df, sample_contract_df
        )

        assert len(new_data) == 0
        assert len(existing_data) == 2
        assert stats["new_records"] == 0
        assert stats["existing_records"] == 2

    def test_check_duplicates_all_new(self):
        """全て新規の場合のテスト"""
        from processors.gb_registration import DuplicateChecker
        checker = DuplicateChecker()

        excel_df = pd.DataFrame({
            "ユーザーID（社内管理用）": ["99999", "88888"],
            "ユーザー名": ["新規太郎", "新規花子"]
        })
        contract_df = pd.DataFrame({
            "引継番号": ["11111", "22222"],
            "委託先法人ID": ["7", "7"]
        })

        new_data, existing_data, stats = checker.check_duplicates(
            excel_df, contract_df
        )

        assert len(new_data) == 2
        assert len(existing_data) == 0
        assert stats["new_percentage"] == 100.0


class TestDataMapper:
    """DataMapperクラスのテスト"""

    @pytest.fixture
    def sample_excel_df(self):
        """Excelファイルのサンプルデータ"""
        return pd.DataFrame({
            "ユーザーID（社内管理用）": ["8850"],
            "ユーザー名": ["秋場 雄喜"],
            "カナ": ["アキバ ユウキ"],
            "生年月日": ["1994-03-11"],
            "郵便番号": ["0070848"],
            "住所_都道府県": ["北海道"],
            "住所_1": ["札幌市東区北四十八条東5丁目2-5"],
            "住所_2": ["アレックスN48-202"],
            "電話番号": ["09020743886"],
            "メールアドレス": ["yuuki0311ab@gmail.com"],
            "請求金額": ["5916"],
            "振込先銀行名": ["GMOあおぞらネット銀行"],
            "振込先支店名": ["ｱｶﾏﾂ"],
            "振込先口座種別": ["普通預金"],
            "振込先口座番号": ["6609575"],
            "振込先口座名義人": ["ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"]
        })

    def test_map_contractor_info(self, sample_excel_df):
        """契約者情報マッピングテスト"""
        from processors.gb_registration import DataMapper, GBConfig
        mapper = DataMapper(GBConfig())
        output_df = mapper.create_output_dataframe(sample_excel_df)
        mapper.map_contractor_info(output_df, sample_excel_df)

        assert output_df["引継番号"].iloc[0] == "8850"
        assert output_df["契約者氏名"].iloc[0] == "秋場雄喜"  # スペース除去
        assert output_df["契約者カナ"].iloc[0] == "アキバユウキ"  # スペース除去
        assert output_df["契約者生年月日"].iloc[0] == "1994/3/11"  # 日付フォーマット
        assert output_df["契約者TEL携帯"].iloc[0] == "090-2074-3886"  # 電話フォーマット
        assert output_df["契約者現住所郵便番号"].iloc[0] == "007-0848"  # 郵便番号フォーマット
        assert output_df["契約者現住所1"].iloc[0] == "北海道"
        assert output_df["契約者現住所2"].iloc[0] == "札幌市東区"
        assert output_df["契約者現住所3"].iloc[0] == "北四十八条東5丁目2-5　アレックスN48-202"

    def test_map_hikitsugi_info(self, sample_excel_df):
        """引継情報マッピングテスト"""
        from processors.gb_registration import DataMapper, GBConfig
        mapper = DataMapper(GBConfig())
        output_df = mapper.create_output_dataframe(sample_excel_df)
        mapper.map_hikitsugi_info(output_df, sample_excel_df)

        # 引継情報のフォーマット確認
        today = datetime.now().strftime("%Y/%m/%d")
        expected = f"{today}　ガレージバンク一括登録　●メールアドレス：yuuki0311ab@gmail.com"
        assert output_df["引継情報"].iloc[0] == expected

    def test_map_financial_info(self, sample_excel_df):
        """金融情報マッピングテスト"""
        from processors.gb_registration import DataMapper, GBConfig
        mapper = DataMapper(GBConfig())
        output_df = mapper.create_output_dataframe(sample_excel_df)
        mapper.map_financial_info(output_df, sample_excel_df)

        assert output_df["管理前滞納額"].iloc[0] == "5916"
        assert output_df["回収口座支店名"].iloc[0] == "ｱｶﾏﾂ"
        assert output_df["回収口座種類"].iloc[0] == "普通"  # 「普通預金」→「普通」
        assert output_df["回収口座番号"].iloc[0] == "6609575"
        assert output_df["回収口座名義"].iloc[0] == "ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"

    def test_apply_fixed_values(self, sample_excel_df):
        """固定値適用テスト"""
        from processors.gb_registration import DataMapper, GBConfig
        mapper = DataMapper(GBConfig())
        output_df = mapper.create_output_dataframe(sample_excel_df)
        mapper.apply_fixed_values(output_df)

        # 管理受託日（実行日）
        today = datetime.now().strftime("%Y/%m/%d")
        assert output_df["管理受託日"].iloc[0] == today

        # 固定値
        assert output_df["クライアントCD"].iloc[0] == "9288"
        assert output_df["委託先法人ID"].iloc[0] == "7"
        assert output_df["入居ステータス"].iloc[0] == "入居中"
        assert output_df["滞納ステータス"].iloc[0] == "未精算"
        assert output_df["受託状況"].iloc[0] == "契約中"
        assert output_df["契約種類"].iloc[0] == "バックレント"
        assert output_df["回収口座金融機関CD"].iloc[0] == "310"
        assert output_df["回収口座金融機関名"].iloc[0] == "GMOあおぞらネット銀行"
        assert output_df["物件名"].iloc[0] == "カシャリ"
        assert output_df["物件住所郵便番号"].iloc[0] == "000-0000"
        assert output_df["物件住所1"].iloc[0] == "東京都"
        assert output_df["物件住所2"].iloc[0] == "リース"
        assert output_df["物件住所3"].iloc[0] == "債権"
        assert output_df["退去済手数料"].iloc[0] == "20"
        assert output_df["入居中滞納手数料"].iloc[0] == "20"
        assert output_df["入居中正常手数料"].iloc[0] == "20"
        assert output_df["月額賃料"].iloc[0] == "0"


class TestProcessGBDataE2E:
    """process_gb_data関数のE2E統合テスト"""

    @pytest.fixture
    def sample_excel_file(self):
        """Excelファイルのバイナリデータを作成"""
        from openpyxl import Workbook

        # skiprows=1で読み込む構造：
        # 行1: スキップされる行（説明行）
        # 行2: ヘッダー行（列名）
        # 行3以降: データ

        wb = Workbook()
        ws = wb.active
        ws.title = 'zokusei'

        # 行1: スキップされる行
        ws.append(['説明行', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])

        # 行2: ヘッダー（列名）
        headers = [
            "ユーザーID（社内管理用）", "ユーザーID（対顧客表示）", "ユーザー名", "カナ",
            "生年月日", "郵便番号", "住所_都道府県", "住所_1", "住所_2", "電話番号",
            "メールアドレス", "請求金額", "元金", "リース料未消込残高",
            "振込先銀行名", "振込先支店名", "振込先口座種別", "振込先口座番号", "振込先口座名義人"
        ]
        ws.append(headers)

        # 行3: データ1（新規: 99999）
        ws.append([
            "99999", "TEST1", "新規 太郎", "シンキ タロウ", "1990-01-15",
            "1000001", "東京都", "千代田区丸の内1-1-1", "テストビル101", "09011112222",
            "test@example.com", "10000", "9500", "",
            "GMOあおぞらネット銀行", "ｲｺｲ", "普通預金", "1234567", "ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"
        ])

        # 行4: データ2（既存: 8850）
        ws.append([
            "8850", "TEST2", "秋場 雄喜", "アキバ ユウキ", "1994-03-11",
            "0070848", "北海道", "札幌市東区北四十八条東5丁目2-5", "アレックスN48-202", "09020743886",
            "yuuki0311ab@gmail.com", "5916", "5840", "",
            "GMOあおぞらネット銀行", "ｱｶﾏﾂ", "普通預金", "6609575", "ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"
        ])

        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)
        return excel_buffer.read()

    @pytest.fixture
    def sample_contract_csv(self):
        """ContractListのCSVバイナリデータを作成"""
        csv_content = """引継番号,委託先法人ID,契約者氏名
8850,7,秋場雄喜
12509,7,近藤茉衣
13769,7,西山大貴"""
        return csv_content.encode('cp932')

    def test_process_gb_data_success(self, sample_excel_file, sample_contract_csv):
        """process_gb_data関数の正常動作テスト"""
        from processors.gb_registration import process_gb_data

        output_df, logs, filename = process_gb_data(sample_excel_file, sample_contract_csv)

        # 新規データのみ出力される（99999のみ）
        assert len(output_df) == 1
        assert output_df["引継番号"].iloc[0] == "99999"

        # ファイル名確認
        assert "ガレージバンク新規登録" in filename
        assert filename.endswith(".csv")

        # 111列の確認
        assert len(output_df.columns) == 111

        # データマッピング確認
        assert output_df["契約者氏名"].iloc[0] == "新規太郎"
        assert output_df["クライアントCD"].iloc[0] == "9288"
        assert output_df["委託先法人ID"].iloc[0] == "7"

    def test_process_gb_data_all_existing(self, sample_contract_csv):
        """全て既存データの場合のテスト"""
        from processors.gb_registration import process_gb_data
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = 'zokusei'

        # 行1: スキップされる行
        ws.append(['説明行'] + [''] * 18)

        # 行2: ヘッダー
        headers = [
            "ユーザーID（社内管理用）", "ユーザーID（対顧客表示）", "ユーザー名", "カナ",
            "生年月日", "郵便番号", "住所_都道府県", "住所_1", "住所_2", "電話番号",
            "メールアドレス", "請求金額", "元金", "リース料未消込残高",
            "振込先銀行名", "振込先支店名", "振込先口座種別", "振込先口座番号", "振込先口座名義人"
        ]
        ws.append(headers)

        # 行3: 既存データ1（8850）
        ws.append([
            "8850", "", "秋場 雄喜", "アキバ ユウキ", "1994-03-11",
            "0070848", "北海道", "札幌市東区北四十八条東5丁目2-5", "アレックスN48-202", "09020743886",
            "yuuki0311ab@gmail.com", "5916", "5840", "",
            "GMOあおぞらネット銀行", "ｱｶﾏﾂ", "普通預金", "6609575", "ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"
        ])

        # 行4: 既存データ2（12509）
        ws.append([
            "12509", "", "近藤 茉衣", "コンドウ マイ", "1997-03-04",
            "5798003", "大阪府", "東大阪市日下町1-1-91", "", "07031032545",
            "test@example.com", "24487", "24140", "",
            "GMOあおぞらネット銀行", "ｲｺｲ", "普通預金", "9861437", "ｶﾞﾚ-ｼﾞﾊﾞﾝｸ(ｶ"
        ])

        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)
        excel_bytes = excel_buffer.read()

        output_df, logs, filename = process_gb_data(excel_bytes, sample_contract_csv)

        # 新規データがないため空のDataFrame
        assert len(output_df) == 0
        assert any("新規: 0件" in log for log in logs)

    def test_process_gb_data_output_columns_format(self, sample_excel_file, sample_contract_csv):
        """出力列フォーマットの詳細確認テスト"""
        from processors.gb_registration import process_gb_data

        output_df, logs, filename = process_gb_data(sample_excel_file, sample_contract_csv)

        # 111列の存在確認
        assert len(output_df.columns) == 111
        assert "引継番号" in output_df.columns
        assert "契約者氏名" in output_df.columns
        assert "契約者カナ" in output_df.columns
        assert "物件住所郵便番号" in output_df.columns
        assert "クライアントCD" in output_df.columns
        assert "委託先法人ID" in output_df.columns
        assert "管理受託日" in output_df.columns
        assert "登録フラグ" in output_df.columns

        # 固定値が適用されているか
        assert output_df["クライアントCD"].iloc[0] == "9288"
        assert output_df["回収口座金融機関名"].iloc[0] == "GMOあおぞらネット銀行"
        assert output_df["物件名"].iloc[0] == "カシャリ"

        # 管理受託日が今日の日付であること
        today = datetime.now().strftime("%Y/%m/%d")
        assert output_df["管理受託日"].iloc[0] == today
