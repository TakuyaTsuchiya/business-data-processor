#!/usr/bin/env python3
"""
ナップ新規登録プロセッサのテスト
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import patch, Mock
from processors.nap_registration import (
    NapConfig,
    FileReader,
    DuplicateChecker,
    DataMapper,
    process_nap_data,
    format_zipcode,
    format_phone
)


class TestNapConfig:
    """NapConfigクラスのテスト"""

    def test_output_columns_count(self):
        """OUTPUT_COLUMNSが111列あることを確認"""
        assert len(NapConfig.OUTPUT_COLUMNS) == 111

    def test_output_columns_structure(self):
        """OUTPUT_COLUMNSの最初と最後の列名を確認"""
        assert NapConfig.OUTPUT_COLUMNS[0] == "引継番号"
        assert NapConfig.OUTPUT_COLUMNS[-1] == "登録フラグ"
        assert NapConfig.OUTPUT_COLUMNS[47] == "クライアントCD"

    def test_fixed_values_required_keys(self):
        """FIXED_VALUESに必須キーが含まれることを確認"""
        required_keys = [
            "クライアントCD",
            "委託先法人ID",
            "入居ステータス",
            "滞納ステータス",
            "受託状況",
            "契約種類",
            "回収口座金融機関CD",
            "回収口座金融機関名",
            "回収口座種類",
            "回収口座名義"
        ]
        for key in required_keys:
            assert key in NapConfig.FIXED_VALUES

    def test_fixed_values_content(self):
        """FIXED_VALUESの値が正しいことを確認"""
        assert NapConfig.FIXED_VALUES["クライアントCD"] == "9268"
        assert NapConfig.FIXED_VALUES["委託先法人ID"] == "5"
        assert NapConfig.FIXED_VALUES["回収口座金融機関名"] == "みずほ銀行"
        assert NapConfig.FIXED_VALUES["回収口座名義"] == "ナップ賃貸保証株式会社"
        assert NapConfig.FIXED_VALUES["その他費用2"] == "0"

    def test_target_corporation_id(self):
        """TARGET_CORPORATION_IDが5であることを確認"""
        assert NapConfig.TARGET_CORPORATION_ID == "5"

    def test_excel_skiprows(self):
        """EXCEL_SKIPROWSが13であることを確認"""
        assert NapConfig.EXCEL_SKIPROWS == 13


class TestFormatFunctions:
    """フォーマット関数のテスト"""

    def test_format_zipcode_7digits(self):
        """7桁郵便番号のフォーマットテスト"""
        assert format_zipcode("1000001") == "100-0001"
        assert format_zipcode("1500044") == "150-0044"
        assert format_zipcode("2000001") == "200-0001"

    def test_format_zipcode_already_formatted(self):
        """既にフォーマット済みの郵便番号テスト"""
        assert format_zipcode("100-0001") == "100-0001"
        assert format_zipcode("150-0044") == "150-0044"

    def test_format_zipcode_empty(self):
        """空の郵便番号テスト"""
        assert format_zipcode("") == ""
        assert format_zipcode(None) == ""
        assert format_zipcode(pd.NA) == ""

    def test_format_zipcode_invalid_length(self):
        """7桁以外の郵便番号テスト"""
        assert format_zipcode("123") == "123"
        assert format_zipcode("12345") == "12345"

    def test_format_phone_11digits_mobile(self):
        """11桁携帯電話番号のフォーマットテスト"""
        assert format_phone("09012345678") == "090-1234-5678"
        assert format_phone("08012345678") == "080-1234-5678"
        assert format_phone("07012345678") == "070-1234-5678"
        assert format_phone("05031774629") == "050-3177-4629"

    def test_format_phone_10digits_03_06(self):
        """10桁固定電話（03/06市外局番）のフォーマットテスト"""
        assert format_phone("0312345678") == "03-1234-5678"
        assert format_phone("0359088055") == "03-5908-8055"
        assert format_phone("0612345678") == "06-1234-5678"

    def test_format_phone_10digits_3digit_area(self):
        """10桁固定電話（3桁市外局番）のフォーマットテスト"""
        assert format_phone("0421112222") == "042-111-2222"
        assert format_phone("0451234567") == "045-123-4567"
        assert format_phone("0521234567") == "052-123-4567"

    def test_format_phone_toll_free(self):
        """フリーダイヤル・ナビダイヤルのフォーマットテスト"""
        assert format_phone("0120123456") == "0120-123-456"
        assert format_phone("0570123456") == "0570-123-456"
        assert format_phone("0800123456") == "0800-123-456"

    def test_format_phone_already_formatted(self):
        """既にフォーマット済みの電話番号テスト"""
        assert format_phone("090-1234-5678") == "090-1234-5678"
        assert format_phone("03-1234-5678") == "03-1234-5678"
        assert format_phone("042-111-2222") == "042-111-2222"

    def test_format_phone_empty(self):
        """空の電話番号テスト"""
        assert format_phone("") == ""
        assert format_phone(None) == ""
        assert format_phone(pd.NA) == ""

    def test_format_phone_invalid_length(self):
        """10/11桁以外の電話番号テスト"""
        assert format_phone("123") == "123"
        assert format_phone("123456789") == "123456789"

    def test_format_phone_missing_leading_zero(self):
        """先頭0欠損（Excel数値化）の電話番号テスト"""
        # 10桁で9始まり → 携帯番号と判定して先頭に0を追加
        assert format_phone("9037978313") == "090-3797-8313"
        assert format_phone("8012345678") == "080-1234-5678"
        assert format_phone("7012345678") == "070-1234-5678"


class TestFileReader:
    """FileReaderクラスのテスト"""

    @pytest.fixture
    def file_reader(self):
        """FileReaderインスタンスを作成"""
        return FileReader()

    def test_read_excel_file_success(self, file_reader):
        """Excelファイルの正常読み込みテスト"""
        # 簡易的なExcelファイルを作成
        df_sample = pd.DataFrame({
            "承認番号": ["NAP001", "NAP002"],
            "契約者氏名": ["山田太郎", "佐藤花子"]
        })
        excel_buffer = io.BytesIO()
        df_sample.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        # 読み込み（skiprows=0で正常読み込み）
        result = file_reader.read_excel_file(excel_buffer, skiprows=0)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "承認番号" in result.columns
        assert "契約者氏名" in result.columns

    def test_read_excel_file_with_skiprows(self, file_reader):
        """skiprows指定でのExcel読み込みテスト"""
        # ヘッダー行の前に13行のダミー行があるケース
        df_sample = pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者氏名": ["山田太郎"]
        })
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # ダミー行を追加
            dummy_rows = pd.DataFrame({"dummy": [f"Row {i}" for i in range(13)]})
            dummy_rows.to_excel(writer, index=False, header=False)
            # 実データを書き込み（13行後）
            df_sample.to_excel(writer, index=False, startrow=13)

        excel_buffer.seek(0)
        result = file_reader.read_excel_file(excel_buffer, skiprows=13)

        assert isinstance(result, pd.DataFrame)
        assert "承認番号" in result.columns

    def test_read_excel_file_dtype_str(self, file_reader):
        """全列が文字列型として読み込まれることを確認"""
        df_sample = pd.DataFrame({
            "番号": [1, 2, 3],
            "金額": [10000.0, 20000.0, 30000.0]
        })
        excel_buffer = io.BytesIO()
        df_sample.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        result = file_reader.read_excel_file(excel_buffer, skiprows=0)

        # すべての列がobject型（文字列）であることを確認
        assert all(result.dtypes == 'object')

    def test_read_excel_file_invalid_file(self, file_reader):
        """不正なファイルでのエラー処理テスト"""
        invalid_content = b"This is not an Excel file"

        with pytest.raises(ValueError, match="Excelファイルの読み込みに失敗しました"):
            file_reader.read_excel_file(invalid_content, skiprows=0)

    def test_read_csv_file_cp932(self, file_reader):
        """CP932エンコーディングのCSV読み込みテスト"""
        csv_content = "引継番号,氏名\nNAP001,山田太郎\nNAP002,佐藤花子\n"
        csv_bytes = csv_content.encode('cp932')

        result = file_reader.read_csv_file(csv_bytes)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "引継番号" in result.columns
        assert "氏名" in result.columns

    def test_read_csv_file_utf8(self, file_reader):
        """UTF-8エンコーディングのCSV読み込みテスト"""
        csv_content = "引継番号,氏名\nNAP001,山田太郎\nNAP002,佐藤花子\n"
        csv_bytes = csv_content.encode('utf-8-sig')

        result = file_reader.read_csv_file(csv_bytes)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_read_csv_file_auto_encoding_detection(self, file_reader):
        """エンコーディング自動判定のテスト（Shift_JIS → UTF-8フォールバック）"""
        # Shift_JISでエンコード
        csv_content = "引継番号,氏名\nNAP001,山田太郎\n"
        csv_bytes = csv_content.encode('shift_jis')

        result = file_reader.read_csv_file(csv_bytes)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_read_csv_file_all_encodings_fail(self, file_reader):
        """全てのエンコーディングで失敗する場合のエラーテスト"""
        # 完全に不正なバイナリデータ
        invalid_bytes = b'\xff\xfe\xfd\xfc\xfb'

        with pytest.raises(ValueError, match="CSVファイルの読み込みに失敗しました"):
            file_reader.read_csv_file(invalid_bytes)


class TestDuplicateChecker:
    """DuplicateCheckerクラスのテスト"""

    @pytest.fixture
    def duplicate_checker(self):
        """DuplicateCheckerインスタンスを作成"""
        return DuplicateChecker()

    @pytest.fixture
    def sample_contract_df(self):
        """ContractListのサンプルデータ"""
        return pd.DataFrame({
            "引継番号": ["NAP001", "NAP002", "NAP003"],
            "委託先法人ID": ["5", "5", "3"],
            "契約者氏名": ["山田太郎", "佐藤花子", "田中次郎"]
        })

    @pytest.fixture
    def sample_excel_df(self):
        """Excelファイルのサンプルデータ"""
        return pd.DataFrame({
            "承認番号": ["NAP001", "NAP004", "NAP005"],
            "契約者氏名": ["山田太郎", "鈴木一郎", "高橋美香"]
        })

    def test_filter_contract_list(self, duplicate_checker, sample_contract_df):
        """委託先法人IDによるフィルタリングテスト"""
        filtered_df = duplicate_checker.filter_contract_list(sample_contract_df, target_id="5")

        # 委託先法人ID=5のみ抽出（2件）
        assert len(filtered_df) == 2
        assert all(filtered_df["委託先法人ID"] == "5")

    def test_filter_contract_list_no_column(self, duplicate_checker):
        """委託先法人ID列が存在しない場合のエラーテスト"""
        df_no_column = pd.DataFrame({"引継番号": ["NAP001"]})

        with pytest.raises(ValueError, match="ContractListに「委託先法人ID」列が存在しません"):
            duplicate_checker.filter_contract_list(df_no_column, target_id="5")

    def test_check_duplicates_new_records(self, duplicate_checker, sample_excel_df, sample_contract_df):
        """新規レコードの重複チェックテスト"""
        new_data, existing_data, stats, logs = duplicate_checker.check_duplicates(
            sample_excel_df, sample_contract_df
        )

        # NAP001は既存、NAP004とNAP005は新規
        assert len(new_data) == 2
        assert len(existing_data) == 1
        assert "NAP004" in new_data["承認番号"].values
        assert "NAP005" in new_data["承認番号"].values
        assert "NAP001" in existing_data["承認番号"].values

    def test_check_duplicates_all_existing(self, duplicate_checker, sample_contract_df):
        """全て既存の場合のテスト"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP001", "NAP002"],
            "契約者氏名": ["山田太郎", "佐藤花子"]
        })

        new_data, existing_data, stats, logs = duplicate_checker.check_duplicates(
            excel_df, sample_contract_df
        )

        assert len(new_data) == 0
        assert len(existing_data) == 2
        assert stats["new_records"] == 0
        assert stats["existing_records"] == 2

    def test_check_duplicates_all_new(self, duplicate_checker):
        """全て新規の場合のテスト"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP100", "NAP101"],
            "契約者氏名": ["新規太郎", "新規花子"]
        })
        contract_df = pd.DataFrame({
            "引継番号": ["NAP001", "NAP002"],
            "委託先法人ID": ["5", "5"]
        })

        new_data, existing_data, stats, logs = duplicate_checker.check_duplicates(
            excel_df, contract_df
        )

        assert len(new_data) == 2
        assert len(existing_data) == 0
        assert stats["new_percentage"] == 100.0

    def test_check_duplicates_no_approval_column(self, duplicate_checker, sample_contract_df):
        """承認番号列が存在しない場合のエラーテスト"""
        excel_df = pd.DataFrame({"契約者氏名": ["山田太郎"]})

        with pytest.raises(ValueError, match="Excel入力データに「承認番号」列が存在しません"):
            duplicate_checker.check_duplicates(excel_df, sample_contract_df)

    def test_check_duplicates_no_handover_column(self, duplicate_checker, sample_excel_df):
        """引継番号列が存在しない場合のエラーテスト"""
        contract_df = pd.DataFrame({"委託先法人ID": ["5"]})

        with pytest.raises(ValueError, match="ContractListに「引継番号」列が存在しません"):
            duplicate_checker.check_duplicates(sample_excel_df, contract_df)

    def test_check_duplicates_float_conversion(self, duplicate_checker):
        """float型の承認番号が正しく処理されるテスト"""
        excel_df = pd.DataFrame({
            "承認番号": [1001.0, 1002.0],
            "契約者氏名": ["山田太郎", "佐藤花子"]
        })
        contract_df = pd.DataFrame({
            "引継番号": ["1001", "1003"],
            "委託先法人ID": ["5", "5"]
        })

        new_data, existing_data, stats, logs = duplicate_checker.check_duplicates(
            excel_df, contract_df
        )

        # 1001は既存（1001.0 → "1001"に変換される）
        assert len(new_data) == 1
        assert len(existing_data) == 1
        assert "1002" in new_data["承認番号"].values


class TestDataMapper:
    """DataMapperクラスのテスト"""

    @pytest.fixture
    def data_mapper(self):
        """DataMapperインスタンスを作成"""
        return DataMapper(NapConfig())

    @pytest.fixture
    def sample_excel_df(self):
        """Excelファイルのサンプルデータ"""
        return pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者氏名": ["山田太郎"],
            "契約者氏名かな": ["ヤマダタロウ"],
            "契約者生年月日": ["1990/01/01"],
            "契約者電話": ["03-1234-5678"],
            "契約者携帯1": ["090-1234-5678"],
            "契約者郵便番号": ["100-0001"],
            "契約者１住所１": ["東京都千代田区"],
            "契約者１住所２": ["丸の内1-1"],
            "契約者１住所３": ["丸ビル"],
            "契約者住所アパート等": ["203号室"],
            "物件名": ["テストマンション"],
            "部屋番号": ["101"],
            "物件郵便番号": ["200-0001"],
            "物件住所１": ["神奈川県横浜市"],
            "物件住所２": ["みなとみらい1-1"],
            "物件住所３": ["タワー101"],
            "賃料": ["85000"],
            "賃料合計額": ["100000"],
            "管理費公益費": ["10000"],
            "バーチャル口座: 名称": ["4389306"],
            "水道代": ["3000"],
            "駐車場": ["15000"],
            "その他費用": ["2000"],
            "加盟店: 加盟店名": ["株式会社テスト不動産"],
            "連保人1氏名": ["山田花子"],
            "連保人1氏名かな": ["ヤマダハナコ"],
            "連帯保証人関係": ["配偶者"],
            "連保人1生年月日": ["1992/02/02"],
            "連保人1郵便番号": ["200-0001"],
            "連保人1住所１": ["東京都立川市"],
            "連保人1住所２": ["曙町1-1-1"],
            "連保人1住所３": ["マンションA"],
            "連保人1住所アパート等": ["201号室"],
            "連保人1電話": ["042-111-2222"],
            "連保人1携帯番号": ["080-1111-2222"],
            "緊急連絡人氏名": ["佐藤次郎"],
            "緊急連絡人氏名かな": ["サトウジロウ"],
            "緊急連絡人関係": ["友人"],
            "緊急連絡人郵便番号": ["300-0001"],
            "緊急連絡人住所１": ["茨城県土浦市"],
            "緊急連絡人住所２": ["中央1-1-1"],
            "緊急連絡人住所３": [""],
            "緊急連絡人電話": ["029-111-2222"],
            "緊急連絡人携帯１": ["070-1111-2222"]
        })

    def test_create_output_dataframe(self, data_mapper, sample_excel_df):
        """111列の出力DataFrame作成テスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)

        assert isinstance(output_df, pd.DataFrame)
        assert len(output_df.columns) == 111
        assert len(output_df) == len(sample_excel_df)
        assert output_df.columns[0] == "引継番号"
        assert output_df.columns[-1] == "登録フラグ"
        # 初期値は全て空文字列
        assert all(output_df.iloc[0] == "")

    def test_map_contractor_info(self, data_mapper, sample_excel_df):
        """契約者情報マッピングテスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)
        data_mapper.map_contractor_info(output_df, sample_excel_df)

        assert output_df["引継番号"].iloc[0] == "NAP001"
        assert output_df["契約者氏名"].iloc[0] == "山田太郎"
        assert output_df["契約者カナ"].iloc[0] == "ヤマダタロウ"
        assert output_df["契約者生年月日"].iloc[0] == "1990/01/01"
        # 実装側でマッピングされている列のみチェック
        assert output_df["契約者TEL自宅"].iloc[0] == "03-1234-5678"
        assert output_df["契約者TEL携帯"].iloc[0] == "090-1234-5678"
        assert output_df["契約者現住所郵便番号"].iloc[0] == "100-0001"
        assert output_df["契約者現住所1"].iloc[0] == "東京都千代田区"
        assert output_df["契約者現住所2"].iloc[0] == "丸の内1-1"
        assert output_df["契約者現住所3"].iloc[0] == "丸ビル　203号室"  # 全角空白で結合

    def test_map_contractor_address3_with_fullwidth_space(self, data_mapper):
        """契約者現住所3: 番地とアパート等の間に全角空白が入ることを確認"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者１住所３": ["神谷町20-2"],
            "契約者住所アパート等": ["エステ横浜神奈川町106号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_contractor_info(output_df, excel_df)

        # 番地とアパート等の間に全角空白
        assert output_df["契約者現住所3"].iloc[0] == "神谷町20-2　エステ横浜神奈川町106号室"

    def test_map_contractor_address3_only_addr3(self, data_mapper):
        """契約者現住所3: 番地のみの場合（アパート等が空）"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者１住所３": ["神谷町20-2"],
            "契約者住所アパート等": [""],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_contractor_info(output_df, excel_df)

        # アパート等が空なら末尾に余計な空白が入らない
        assert output_df["契約者現住所3"].iloc[0] == "神谷町20-2"

    def test_map_contractor_address3_only_apt(self, data_mapper):
        """契約者現住所3: アパート等のみの場合（番地が空）"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者１住所３": [""],
            "契約者住所アパート等": ["エステ横浜神奈川町106号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_contractor_info(output_df, excel_df)

        # 番地が空なら先頭に余計な空白が入らない
        assert output_df["契約者現住所3"].iloc[0] == "エステ横浜神奈川町106号室"

    def test_map_contractor_address3_both_empty(self, data_mapper):
        """契約者現住所3: 両方空の場合"""
        excel_df = pd.DataFrame({
            "承認番号": ["NAP001"],
            "契約者１住所３": [""],
            "契約者住所アパート等": [""],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_contractor_info(output_df, excel_df)

        # 両方空なら空文字列
        assert output_df["契約者現住所3"].iloc[0] == ""

    def test_map_contractor_info_phone_fallback(self, data_mapper):
        """契約者情報マッピングテスト（携帯1が空白で電話のみの場合）"""
        # 携帯1が空白で契約者電話のみ存在するケース
        excel_df = pd.DataFrame({
            "承認番号": ["NAP002"],
            "契約者氏名": ["田中花子"],
            "契約者氏名かな": ["タナカハナコ"],
            "契約者生年月日": ["1985/05/15"],
            "契約者電話": ["03-9999-8888"],
            "契約者携帯1": [""],  # 空白
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_contractor_info(output_df, excel_df)

        # 携帯1が空白なので、電話を携帯に使用し、自宅は空白
        assert output_df["契約者TEL携帯"].iloc[0] == "03-9999-8888"
        assert output_df["契約者TEL自宅"].iloc[0] == ""

    def test_map_property_info(self, data_mapper, sample_excel_df):
        """物件情報マッピングテスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)
        data_mapper.map_property_info(output_df, sample_excel_df)

        # 実装側でマッピングされている列のみチェック
        assert output_df["物件名"].iloc[0] == "テストマンション"
        assert output_df["部屋番号"].iloc[0] == "101"
        assert output_df["物件住所郵便番号"].iloc[0] == "200-0001"
        assert output_df["物件住所1"].iloc[0] == "神奈川県横浜市"
        assert output_df["物件住所2"].iloc[0] == "みなとみらい1-1"
        assert output_df["物件住所3"].iloc[0] == "タワー101"  # 番地のみ（物件名・部屋番号は含まない）
        assert output_df["月額賃料"].iloc[0] == "85000"  # 賃料列を使用
        assert output_df["退去手続き（実費）"].iloc[0] == "85000"
        assert output_df["管理費"].iloc[0] == "10000"
        assert output_df["水道代"].iloc[0] == "3000"
        assert output_df["駐車場代"].iloc[0] == "15000"
        assert output_df["その他費用1"].iloc[0] == "2000"
        assert output_df["管理会社"].iloc[0] == "株式会社テスト不動産"
        assert output_df["回収口座番号"].iloc[0] == "4389306"

    def test_map_property_address3_only_addr(self, data_mapper):
        """物件住所3: 番地のみが出力されることを確認（物件名・部屋番号は含まない）"""
        excel_df = pd.DataFrame({
            "物件住所３": ["栄町20-2"],
            "物件名": ["ユナイト横浜アルボラン"],
            "部屋番号": ["106号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_property_info(output_df, excel_df)

        # 物件住所3は番地のみ（物件名・部屋番号は含まない）
        assert output_df["物件住所3"].iloc[0] == "栄町20-2"

    def test_map_property_address3_empty(self, data_mapper):
        """物件住所3: 物件住所３が空の場合"""
        excel_df = pd.DataFrame({
            "物件住所３": [""],
            "物件名": ["ユナイト横浜アルボラン"],
            "部屋番号": ["106号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_property_info(output_df, excel_df)

        # 物件住所３が空なら空文字列
        assert output_df["物件住所3"].iloc[0] == ""

    def test_map_guarantor_info(self, data_mapper, sample_excel_df):
        """保証人情報マッピングテスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)
        data_mapper.map_guarantor_info(output_df, sample_excel_df)

        assert output_df["保証人１氏名"].iloc[0] == "山田花子"
        assert output_df["保証人１カナ"].iloc[0] == "ヤマダハナコ"
        assert output_df["保証人１契約者との関係"].iloc[0] == "他"  # 値がある場合は全て「他」
        assert output_df["保証人１生年月日"].iloc[0] == "1992/02/02"
        assert output_df["保証人１郵便番号"].iloc[0] == "200-0001"
        assert output_df["保証人１住所1"].iloc[0] == "東京都立川市"
        assert output_df["保証人１住所2"].iloc[0] == "曙町1-1-1"
        assert output_df["保証人１住所3"].iloc[0] == "マンションA　201号室"  # 全角空白で結合
        assert output_df["保証人１TEL自宅"].iloc[0] == "042-111-2222"
        assert output_df["保証人１TEL携帯"].iloc[0] == "080-1111-2222"

    def test_map_guarantor_address3_with_fullwidth_space(self, data_mapper):
        """保証人１住所3: 住所３とアパート等の間に全角空白が入ることを確認"""
        excel_df = pd.DataFrame({
            "連保人1氏名": ["山田花子"],
            "連保人1住所３": ["マンションA"],
            "連保人1住所アパート等": ["201号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_guarantor_info(output_df, excel_df)

        # 住所３とアパート等の間に全角空白
        assert output_df["保証人１住所3"].iloc[0] == "マンションA　201号室"

    def test_map_guarantor_address3_only_addr3(self, data_mapper):
        """保証人１住所3: 住所３のみの場合（アパート等が空）"""
        excel_df = pd.DataFrame({
            "連保人1氏名": ["山田花子"],
            "連保人1住所３": ["マンションA"],
            "連保人1住所アパート等": [""],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_guarantor_info(output_df, excel_df)

        # アパート等が空なら末尾に余計な空白が入らない
        assert output_df["保証人１住所3"].iloc[0] == "マンションA"

    def test_map_guarantor_address3_only_apt(self, data_mapper):
        """保証人１住所3: アパート等のみの場合（住所３が空）"""
        excel_df = pd.DataFrame({
            "連保人1氏名": ["山田花子"],
            "連保人1住所３": [""],
            "連保人1住所アパート等": ["201号室"],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_guarantor_info(output_df, excel_df)

        # 住所３が空なら先頭に余計な空白が入らない
        assert output_df["保証人１住所3"].iloc[0] == "201号室"

    def test_map_guarantor_address3_both_empty(self, data_mapper):
        """保証人１住所3: 両方空の場合"""
        excel_df = pd.DataFrame({
            "連保人1氏名": ["山田花子"],
            "連保人1住所３": [""],
            "連保人1住所アパート等": [""],
        })

        output_df = data_mapper.create_output_dataframe(excel_df)
        data_mapper.map_guarantor_info(output_df, excel_df)

        # 両方空なら空文字列
        assert output_df["保証人１住所3"].iloc[0] == ""

    def test_map_emergency_contact_info(self, data_mapper, sample_excel_df):
        """緊急連絡先情報マッピングテスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)
        data_mapper.map_emergency_contact_info(output_df, sample_excel_df)

        assert output_df["緊急連絡人１氏名"].iloc[0] == "佐藤次郎"
        assert output_df["緊急連絡人１カナ"].iloc[0] == "サトウジロウ"
        assert output_df["緊急連絡人１契約者との関係"].iloc[0] == "他"  # 値がある場合は全て「他」
        assert output_df["緊急連絡人１郵便番号"].iloc[0] == "300-0001"
        assert output_df["緊急連絡人１現住所1"].iloc[0] == "茨城県土浦市"
        assert output_df["緊急連絡人１現住所2"].iloc[0] == "中央1-1-1"
        assert output_df["緊急連絡人１TEL自宅"].iloc[0] == "029-111-2222"
        assert output_df["緊急連絡人１TEL携帯"].iloc[0] == "070-1111-2222"

    def test_apply_fixed_values(self, data_mapper, sample_excel_df):
        """固定値適用テスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)
        data_mapper.apply_fixed_values(output_df)

        # 管理受託日（実行日）
        today = datetime.now().strftime("%Y/%m/%d")
        assert output_df["管理受託日"].iloc[0] == today

        # 固定値
        assert output_df["クライアントCD"].iloc[0] == "9268"
        assert output_df["委託先法人ID"].iloc[0] == "5"
        assert output_df["入居ステータス"].iloc[0] == "入居中"
        assert output_df["滞納ステータス"].iloc[0] == "未精算"
        assert output_df["受託状況"].iloc[0] == "契約中"
        assert output_df["契約種類"].iloc[0] == "レントワン"
        assert output_df["回収口座金融機関CD"].iloc[0] == "1"
        assert output_df["回収口座金融機関名"].iloc[0] == "みずほ銀行"
        assert output_df["回収口座種類"].iloc[0] == "普通"
        assert output_df["回収口座名義"].iloc[0] == "ナップ賃貸保証株式会社"
        assert output_df["その他費用2"].iloc[0] == "0"

    def test_map_all_info_integration(self, data_mapper, sample_excel_df):
        """全マッピングメソッドの統合テスト"""
        output_df = data_mapper.create_output_dataframe(sample_excel_df)

        # 全マッピング実行
        data_mapper.map_contractor_info(output_df, sample_excel_df)
        data_mapper.map_property_info(output_df, sample_excel_df)
        data_mapper.map_guarantor_info(output_df, sample_excel_df)
        data_mapper.map_emergency_contact_info(output_df, sample_excel_df)
        data_mapper.apply_fixed_values(output_df)

        # 最終的に111列すべてが何らかの値を持つ（空文字列または実データ）
        assert len(output_df.columns) == 111
        assert isinstance(output_df["引継番号"].iloc[0], str)
        assert output_df["引継番号"].iloc[0] == "NAP001"
        assert output_df["クライアントCD"].iloc[0] == "9268"


class TestProcessNapDataE2E:
    """process_nap_data関数のE2E統合テスト"""

    @pytest.fixture
    def sample_excel_file(self):
        """Excelファイルのバイナリデータを作成"""
        # ダミー行13行 + ヘッダー + データ2行
        df_sample = pd.DataFrame({
            "承認番号": ["NAP100", "NAP001"],  # NAP001は既存想定
            "契約者氏名": ["新規太郎", "既存花子"],
            "契約者氏名かな": ["シンキタロウ", "キゾンハナコ"],
            "契約者生年月日": ["1990/01/01", "1991/02/02"],
            "契約者電話番号": ["03-1111-2222", "03-3333-4444"],
            "契約者携帯番号": ["090-1111-2222", "090-3333-4444"],
            "契約者現住所郵便番号": ["100-0001", "150-0001"],
            "契約者現住所1": ["東京都千代田区", "東京都渋谷区"],
            "契約者現住所2": ["丸の内1-1", "渋谷1-1"],
            "物件郵便番号": ["200-0001", "250-0001"],
            "物件住所１": ["神奈川県横浜市", "神奈川県川崎市"],
            "物件住所２": ["みなとみらい1-1", "川崎駅前1-1"],
            "物件住所３": ["タワー101", "ビル202"],
            "賃料合計額": ["120000", "100000"],
            "管理費公益費": ["15000", "10000"],
            "水道代": ["3000", "2500"],
            "駐車場": ["20000", "15000"],
            "その他費用": ["5000", "3000"],
            "加盟店: 加盟店名": ["テスト不動産A", "テスト不動産B"],
            "連保人1氏名": ["保証太郎", "保証花子"],
            "連保人1氏名かな": ["ホショウタロウ", "ホショウハナコ"],
            "連帯保証人関係": ["親", "配偶者"],
            "連保人1生年月日": ["1960/01/01", "1965/02/02"],
            "連保人1郵便番号": ["300-0001", "400-0001"],
            "連保人1住所１": ["茨城県つくば市", "山梨県甲府市"],
            "連保人1住所２": ["研究学園1-1", "甲府駅前1-1"],
            "連保人1住所３": ["", "マンションA"],
            "連保人1電話": ["029-111-2222", "055-111-2222"],
            "連保人1携帯番号": ["080-1111-2222", "080-3333-4444"],
            "緊急連絡人氏名": ["緊急太郎", "緊急花子"],
            "緊急連絡人氏名かな": ["キンキュウタロウ", "キンキュウハナコ"],
            "緊急連絡人関係": ["友人", "兄弟"],
            "緊急連絡人郵便番号": ["500-0001", "600-0001"],
            "緊急連絡人住所１": ["岐阜県岐阜市", "京都府京都市"],
            "緊急連絡人住所２": ["岐阜駅前1-1", "京都駅前1-1"],
            "緊急連絡人住所３": ["", ""],
            "緊急連絡人電話": ["058-111-2222", "075-111-2222"],
            "緊急連絡人携帯１": ["070-1111-2222", "070-3333-4444"]
        })

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # 13行のダミー行を追加
            dummy_rows = pd.DataFrame({"dummy": [f"Row {i}" for i in range(13)]})
            dummy_rows.to_excel(writer, index=False, header=False)
            # 実データを13行後に書き込み
            df_sample.to_excel(writer, index=False, startrow=13)

        excel_buffer.seek(0)
        return excel_buffer.read()

    @pytest.fixture
    def sample_contract_csv(self):
        """ContractListのCSVバイナリデータを作成"""
        csv_content = """引継番号,委託先法人ID,契約者氏名
NAP001,5,既存花子
NAP002,5,他の契約者
NAP003,3,除外対象"""
        return csv_content.encode('cp932')

    def test_process_nap_data_success(self, sample_excel_file, sample_contract_csv):
        """process_nap_data関数の正常動作テスト"""
        output_df, logs, filename = process_nap_data(sample_excel_file, sample_contract_csv)

        # 新規データのみ出力される（NAP100のみ）
        assert len(output_df) == 1
        assert output_df["引継番号"].iloc[0] == "NAP100"

        # ファイル名確認
        assert "ナップ新規登録" in filename
        assert filename.endswith(".csv")

        # ログ確認（実際のログ形式に合わせる）
        assert len(logs) > 0  # ログが生成されていることを確認
        assert any("2件" in log for log in logs)  # フィルタ結果
        assert any("1件" in log for log in logs)  # 新規件数

        # 111列の確認
        assert len(output_df.columns) == 111

        # データマッピング確認
        assert output_df["契約者氏名"].iloc[0] == "新規太郎"
        assert output_df["クライアントCD"].iloc[0] == "9268"
        assert output_df["委託先法人ID"].iloc[0] == "5"

    def test_process_nap_data_all_existing(self, sample_contract_csv):
        """全て既存データの場合のテスト"""
        # 既存データのみのExcelを作成
        df_existing = pd.DataFrame({
            "承認番号": ["NAP001", "NAP002"],
            "契約者氏名": ["既存花子", "他の契約者"],
            "契約者氏名かな": ["キゾンハナコ", "ホカノケイヤクシャ"]
        })

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            dummy_rows = pd.DataFrame({"dummy": [f"Row {i}" for i in range(13)]})
            dummy_rows.to_excel(writer, index=False, header=False)
            df_existing.to_excel(writer, index=False, startrow=13)

        excel_buffer.seek(0)
        excel_bytes = excel_buffer.read()

        output_df, logs, filename = process_nap_data(excel_bytes, sample_contract_csv)

        # 新規データがないため空のDataFrame
        assert len(output_df) == 0
        assert any("新規契約: 0件" in log for log in logs)

    def test_process_nap_data_output_columns_format(self, sample_excel_file, sample_contract_csv):
        """出力列フォーマットの詳細確認テスト"""
        output_df, logs, filename = process_nap_data(sample_excel_file, sample_contract_csv)

        # 111列の存在確認
        assert len(output_df.columns) == 111
        assert "引継番号" in output_df.columns
        assert "契約者氏名" in output_df.columns
        assert "契約者カナ" in output_df.columns
        assert "物件住所郵便番号" in output_df.columns
        assert "保証人１氏名" in output_df.columns
        assert "緊急連絡人１氏名" in output_df.columns
        assert "クライアントCD" in output_df.columns
        assert "委託先法人ID" in output_df.columns
        assert "管理受託日" in output_df.columns
        assert "登録フラグ" in output_df.columns

        # 固定値が適用されているか
        assert output_df["クライアントCD"].iloc[0] == "9268"
        assert output_df["回収口座金融機関名"].iloc[0] == "みずほ銀行"
        assert output_df["回収口座名義"].iloc[0] == "ナップ賃貸保証株式会社"

        # 管理受託日が今日の日付であること
        today = datetime.now().strftime("%Y/%m/%d")
        assert output_df["管理受託日"].iloc[0] == today
