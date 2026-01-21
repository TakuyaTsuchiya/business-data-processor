"""
ガレージバンクSMS契約者プロセッサのテスト
TDD: テストを先に書いて、実装を進める
"""

import pytest
import pandas as pd
from datetime import date, datetime, timedelta
from io import BytesIO


class TestGbSmsContractFilters:
    """フィルタ条件のテスト"""

    @pytest.fixture
    def sample_csv_content(self):
        """テスト用CSVデータを生成"""
        # 121列のCSVを生成（実際のContractListと同じ列数）
        columns = [
            '管理番号', '引継番号', '最新契約種類', '契約確認日', '保証開始日',
            '保証契約', '承認番号', '月初契約種類', '月初レントワン 2ヵ月以下料金',
            '月初レントワン 3～6ヵ月料率', '月初レントワン 7ヵ月以上料率',
            '月初滞納残債(過入金なし)', '月初適用手数料', '受託状況', '入居ステータス',
            '滞納ステータス', '手数料率', '退去手続き（実費）', '更新契約手数料',
            '営業担当者', '契約者氏名', '契約者カナ', '郵便番号', '現住所1',
            '現住所2', '現住所3', 'TEL自宅', 'TEL携帯', '契約者勤務先名称',
            '契約者勤務先電話番号', '契約者勤務先郵便番号', '契約者勤務先現住所1',
            '契約者勤務先現住所2', '契約者勤務先現住所3', '回収口座銀行CD',
            '回収口座銀行名', '回収口座支店CD', '回収口座支店名', '回収口座種類',
            '回収口座番号', '回収口座名義人', '保証人１氏名', '郵便番号.1',
            '現住所1.1', '現住所2.1', '現住所3.1', 'TEL携帯.1', '契約者との関係',
            '保証人２氏名', '郵便番号.2', '現住所1.2', '現住所2.2', '現住所3.2',
            'TEL携帯.2', '契約者との関係.1', '緊急連絡人１氏名',
            '緊急連絡人１のTEL（携帯）', '郵便番号.3', '現住所1.3', '現住所2.3',
            '現住所3.3', '契約者との関係.2', '緊急連絡人２氏名', '郵便番号.4',
            '現住所1.4', '現住所2.4', '現住所3.4', '契約者との関係.3',
            '催告書契約者', '催告書保証人', '催告書緊急連絡人', '滞納残債',
            '入金予定日', '入金予定金額', '最終入金日', '最終入金額', '最終入金者',
            '月額賃料', '管理費', '共益費', '水道代', '駐車場代', 'その他費用1',
            'その他費用2', '月額賃料合計', '管理会社', '回収ランク',
            '住民票取得日契約者', '住民票取得日保証人１', '住民票取得日保証人２',
            '合意書得日', '物件住所郵便番号', '物件住所1', '物件住所2', '物件住所3',
            '物件名', '物件番号', 'クライアントCD', 'クライアント名',
            'クライアント情報の区分', '電話番号', 'FAX番号', 'クライアント住所郵便番号',
            'クライアント住所', '金融機関', '支店', '種別', '口座番号', '口座名義',
            'パートナーCD', 'パートナー名', '営業担当', 'メール', '家賃精算口座金融機関',
            '家賃精算口座支店', '家賃精算口座種別', '家賃精算口座番号',
            '家賃精算口座名義', '委託先法人ID', '委託先法人名', '解約日'
        ]
        return columns

    def create_test_csv(self, rows_data, columns):
        """テスト用CSVをbytes形式で生成"""
        df = pd.DataFrame(rows_data, columns=columns)
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding='cp932')
        buffer.seek(0)
        return buffer.read()

    def create_valid_row(self, columns, overrides=None):
        """有効なデータ行を生成（オーバーライド可能）"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')

        row = {col: '' for col in columns}
        row.update({
            '管理番号': '80001',
            '契約者氏名': 'テスト太郎',
            '契約者カナ': 'テストタロウ',
            'TEL携帯': '090-1234-5678',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テストタロウ',
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '10000',
            '回収ランク': '通常',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '委託先法人ID': '7',
        })

        if overrides:
            row.update(overrides)

        return row

    def test_filter_trustee_id_7_only(self, sample_csv_content):
        """委託先法人ID=7のみ残ることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'委託先法人ID': '7', '管理番号': '80001'}),
            self.create_valid_row(columns, {'委託先法人ID': '1', '管理番号': '80002'}),
            self.create_valid_row(columns, {'委託先法人ID': '5', '管理番号': '80003'}),
            self.create_valid_row(columns, {'委託先法人ID': '', '管理番号': '80004'}),
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 委託先法人ID=7の1件のみ残る
        assert len(result_df) == 1
        assert result_df.iloc[0]['(info5)メモ'] == '80001'

    def test_filter_payment_date_before_today(self, sample_csv_content):
        """入金予定日が前日以前のみ残ることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        today = datetime.now().strftime('%Y/%m/%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y/%m/%d')

        rows = [
            self.create_valid_row(columns, {'入金予定日': yesterday, '管理番号': '80001'}),
            self.create_valid_row(columns, {'入金予定日': today, '管理番号': '80002'}),
            self.create_valid_row(columns, {'入金予定日': tomorrow, '管理番号': '80003'}),
            self.create_valid_row(columns, {'入金予定日': '', '管理番号': '80004'}),  # NaN
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 前日以前とNaNの2件が残る
        assert len(result_df) == 2
        result_ids = result_df['(info5)メモ'].tolist()
        assert '80001' in result_ids  # 前日
        assert '80004' in result_ids  # NaN

    def test_filter_collection_rank_excludes(self, sample_csv_content):
        """回収ランクで弁護士介入・破産決定・死亡決定が除外されることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'回収ランク': '通常', '管理番号': '80001'}),
            self.create_valid_row(columns, {'回収ランク': '弁護士介入', '管理番号': '80002'}),
            self.create_valid_row(columns, {'回収ランク': '破産決定', '管理番号': '80003'}),
            self.create_valid_row(columns, {'回収ランク': '死亡決定', '管理番号': '80004'}),
            self.create_valid_row(columns, {'回収ランク': '', '管理番号': '80005'}),
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 通常と空の2件が残る
        assert len(result_df) == 2
        result_ids = result_df['(info5)メモ'].tolist()
        assert '80001' in result_ids
        assert '80005' in result_ids

    def test_filter_arrears_minimum_1_yen(self, sample_csv_content):
        """滞納残債が1円以上のみ残ることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'滞納残債': '10,000', '管理番号': '80001'}),
            self.create_valid_row(columns, {'滞納残債': '1', '管理番号': '80002'}),
            self.create_valid_row(columns, {'滞納残債': '0', '管理番号': '80003'}),
            self.create_valid_row(columns, {'滞納残債': '-100', '管理番号': '80004'}),
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 1円以上の2件が残る
        assert len(result_df) == 2
        result_ids = result_df['(info5)メモ'].tolist()
        assert '80001' in result_ids
        assert '80002' in result_ids

    def test_filter_payment_amount_excludes_2_3_5(self, sample_csv_content):
        """入金予定金額が2,3,5の場合除外されることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'入金予定金額': '10000', '管理番号': '80001'}),
            self.create_valid_row(columns, {'入金予定金額': '2', '管理番号': '80002'}),
            self.create_valid_row(columns, {'入金予定金額': '3', '管理番号': '80003'}),
            self.create_valid_row(columns, {'入金予定金額': '5', '管理番号': '80004'}),
            self.create_valid_row(columns, {'入金予定金額': '', '管理番号': '80005'}),
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 10000と空の2件が残る
        assert len(result_df) == 2
        result_ids = result_df['(info5)メモ'].tolist()
        assert '80001' in result_ids
        assert '80005' in result_ids

    def test_filter_mobile_phone_format(self, sample_csv_content):
        """TEL携帯が090/080/070形式のみ残ることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'TEL携帯': '090-1234-5678', '管理番号': '80001'}),
            self.create_valid_row(columns, {'TEL携帯': '080-1234-5678', '管理番号': '80002'}),
            self.create_valid_row(columns, {'TEL携帯': '070-1234-5678', '管理番号': '80003'}),
            self.create_valid_row(columns, {'TEL携帯': '03-1234-5678', '管理番号': '80004'}),
            self.create_valid_row(columns, {'TEL携帯': '09012345678', '管理番号': '80005'}),  # ハイフンなし
            self.create_valid_row(columns, {'TEL携帯': '', '管理番号': '80006'}),
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 正しい形式の3件が残る
        assert len(result_df) == 3
        result_ids = result_df['(info5)メモ'].tolist()
        assert '80001' in result_ids
        assert '80002' in result_ids
        assert '80003' in result_ids


class TestGbSmsContractOutput:
    """出力フォーマットのテスト"""

    @pytest.fixture
    def sample_csv_content(self):
        """テスト用CSV列を生成"""
        columns = [
            '管理番号', '引継番号', '最新契約種類', '契約確認日', '保証開始日',
            '保証契約', '承認番号', '月初契約種類', '月初レントワン 2ヵ月以下料金',
            '月初レントワン 3～6ヵ月料率', '月初レントワン 7ヵ月以上料率',
            '月初滞納残債(過入金なし)', '月初適用手数料', '受託状況', '入居ステータス',
            '滞納ステータス', '手数料率', '退去手続き（実費）', '更新契約手数料',
            '営業担当者', '契約者氏名', '契約者カナ', '郵便番号', '現住所1',
            '現住所2', '現住所3', 'TEL自宅', 'TEL携帯', '契約者勤務先名称',
            '契約者勤務先電話番号', '契約者勤務先郵便番号', '契約者勤務先現住所1',
            '契約者勤務先現住所2', '契約者勤務先現住所3', '回収口座銀行CD',
            '回収口座銀行名', '回収口座支店CD', '回収口座支店名', '回収口座種類',
            '回収口座番号', '回収口座名義人', '保証人１氏名', '郵便番号.1',
            '現住所1.1', '現住所2.1', '現住所3.1', 'TEL携帯.1', '契約者との関係',
            '保証人２氏名', '郵便番号.2', '現住所1.2', '現住所2.2', '現住所3.2',
            'TEL携帯.2', '契約者との関係.1', '緊急連絡人１氏名',
            '緊急連絡人１のTEL（携帯）', '郵便番号.3', '現住所1.3', '現住所2.3',
            '現住所3.3', '契約者との関係.2', '緊急連絡人２氏名', '郵便番号.4',
            '現住所1.4', '現住所2.4', '現住所3.4', '契約者との関係.3',
            '催告書契約者', '催告書保証人', '催告書緊急連絡人', '滞納残債',
            '入金予定日', '入金予定金額', '最終入金日', '最終入金額', '最終入金者',
            '月額賃料', '管理費', '共益費', '水道代', '駐車場代', 'その他費用1',
            'その他費用2', '月額賃料合計', '管理会社', '回収ランク',
            '住民票取得日契約者', '住民票取得日保証人１', '住民票取得日保証人２',
            '合意書得日', '物件住所郵便番号', '物件住所1', '物件住所2', '物件住所3',
            '物件名', '物件番号', 'クライアントCD', 'クライアント名',
            'クライアント情報の区分', '電話番号', 'FAX番号', 'クライアント住所郵便番号',
            'クライアント住所', '金融機関', '支店', '種別', '口座番号', '口座名義',
            'パートナーCD', 'パートナー名', '営業担当', 'メール', '家賃精算口座金融機関',
            '家賃精算口座支店', '家賃精算口座種別', '家賃精算口座番号',
            '家賃精算口座名義', '委託先法人ID', '委託先法人名', '解約日'
        ]
        return columns

    def create_test_csv(self, rows_data, columns):
        """テスト用CSVをbytes形式で生成"""
        df = pd.DataFrame(rows_data, columns=columns)
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding='cp932')
        buffer.seek(0)
        return buffer.read()

    def create_valid_row(self, columns, overrides=None):
        """有効なデータ行を生成"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')

        row = {col: '' for col in columns}
        row.update({
            '管理番号': '80001',
            '契約者氏名': 'テスト太郎',
            '契約者カナ': 'テストタロウ',
            'TEL携帯': '090-1234-5678',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テストタロウ',
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '10000',
            '回収ランク': '通常',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '委託先法人ID': '7',
        })

        if overrides:
            row.update(overrides)

        return row

    def test_output_phone_number_mapping(self, sample_csv_content):
        """電話番号が正しくマッピングされることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns, {'TEL携帯': '090-9999-8888'})]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        assert result_df.iloc[0]['電話番号'] == '090-9999-8888'

    def test_output_property_name_with_number(self, sample_csv_content):
        """物件名と物件番号が全角スペースで結合されることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns, {
            '物件名': 'サンプルマンション',
            '物件番号': '203'
        })]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # 全角スペースで結合
        assert result_df.iloc[0]['(info2)物件名'] == 'サンプルマンション　203'

    def test_output_amount_with_comma(self, sample_csv_content):
        """金額がカンマ区切りでフォーマットされることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns, {'滞納残債': '1234567'})]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        assert result_df.iloc[0]['(info3)金額'] == '1,234,567'

    def test_output_bank_account_combined(self, sample_csv_content):
        """銀行口座情報が全角スペースで結合されることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns, {
            '回収口座銀行名': 'みずほ銀行',
            '回収口座支店名': '渋谷支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'ヤマダタロウ'
        })]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        expected = 'みずほ銀行　渋谷支店　普通　1234567　ヤマダタロウ'
        assert result_df.iloc[0]['(info4)銀行口座'] == expected

    def test_output_payment_deadline_format(self, sample_csv_content):
        """支払期限がYYYY年MM月DD日形式になることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns)]

        csv_content = self.create_test_csv(rows, columns)
        payment_date = date(2026, 1, 31)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, payment_date
        )

        assert result_df.iloc[0]['支払期限'] == '2026年01月31日'

    def test_output_filename_format(self, sample_csv_content):
        """出力ファイル名が正しい形式であることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns)]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        # ファイル名は{MMDD}ガレージバンクSMS契約者.csv
        expected_date = datetime.now().strftime('%m%d')
        assert filename == f'{expected_date}ガレージバンクSMS契約者.csv'

    def test_output_column_count(self, sample_csv_content):
        """出力が59列であることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [self.create_valid_row(columns)]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        assert len(result_df.columns) == 59


class TestGbSmsContractStats:
    """統計情報のテスト"""

    @pytest.fixture
    def sample_csv_content(self):
        """テスト用CSV列を生成"""
        columns = [
            '管理番号', '引継番号', '最新契約種類', '契約確認日', '保証開始日',
            '保証契約', '承認番号', '月初契約種類', '月初レントワン 2ヵ月以下料金',
            '月初レントワン 3～6ヵ月料率', '月初レントワン 7ヵ月以上料率',
            '月初滞納残債(過入金なし)', '月初適用手数料', '受託状況', '入居ステータス',
            '滞納ステータス', '手数料率', '退去手続き（実費）', '更新契約手数料',
            '営業担当者', '契約者氏名', '契約者カナ', '郵便番号', '現住所1',
            '現住所2', '現住所3', 'TEL自宅', 'TEL携帯', '契約者勤務先名称',
            '契約者勤務先電話番号', '契約者勤務先郵便番号', '契約者勤務先現住所1',
            '契約者勤務先現住所2', '契約者勤務先現住所3', '回収口座銀行CD',
            '回収口座銀行名', '回収口座支店CD', '回収口座支店名', '回収口座種類',
            '回収口座番号', '回収口座名義人', '保証人１氏名', '郵便番号.1',
            '現住所1.1', '現住所2.1', '現住所3.1', 'TEL携帯.1', '契約者との関係',
            '保証人２氏名', '郵便番号.2', '現住所1.2', '現住所2.2', '現住所3.2',
            'TEL携帯.2', '契約者との関係.1', '緊急連絡人１氏名',
            '緊急連絡人１のTEL（携帯）', '郵便番号.3', '現住所1.3', '現住所2.3',
            '現住所3.3', '契約者との関係.2', '緊急連絡人２氏名', '郵便番号.4',
            '現住所1.4', '現住所2.4', '現住所3.4', '契約者との関係.3',
            '催告書契約者', '催告書保証人', '催告書緊急連絡人', '滞納残債',
            '入金予定日', '入金予定金額', '最終入金日', '最終入金額', '最終入金者',
            '月額賃料', '管理費', '共益費', '水道代', '駐車場代', 'その他費用1',
            'その他費用2', '月額賃料合計', '管理会社', '回収ランク',
            '住民票取得日契約者', '住民票取得日保証人１', '住民票取得日保証人２',
            '合意書得日', '物件住所郵便番号', '物件住所1', '物件住所2', '物件住所3',
            '物件名', '物件番号', 'クライアントCD', 'クライアント名',
            'クライアント情報の区分', '電話番号', 'FAX番号', 'クライアント住所郵便番号',
            'クライアント住所', '金融機関', '支店', '種別', '口座番号', '口座名義',
            'パートナーCD', 'パートナー名', '営業担当', 'メール', '家賃精算口座金融機関',
            '家賃精算口座支店', '家賃精算口座種別', '家賃精算口座番号',
            '家賃精算口座名義', '委託先法人ID', '委託先法人名', '解約日'
        ]
        return columns

    def create_test_csv(self, rows_data, columns):
        """テスト用CSVをbytes形式で生成"""
        df = pd.DataFrame(rows_data, columns=columns)
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding='cp932')
        buffer.seek(0)
        return buffer.read()

    def create_valid_row(self, columns, overrides=None):
        """有効なデータ行を生成"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')

        row = {col: '' for col in columns}
        row.update({
            '管理番号': '80001',
            '契約者氏名': 'テスト太郎',
            '契約者カナ': 'テストタロウ',
            'TEL携帯': '090-1234-5678',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テストタロウ',
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '10000',
            '回収ランク': '通常',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '委託先法人ID': '7',
        })

        if overrides:
            row.update(overrides)

        return row

    def test_stats_initial_and_processed_rows(self, sample_csv_content):
        """統計情報にinitial_rowsとprocessed_rowsが含まれることを確認"""
        from processors.gb_sms.contract import process_gb_sms_contract_data

        columns = sample_csv_content
        rows = [
            self.create_valid_row(columns, {'管理番号': '80001'}),
            self.create_valid_row(columns, {'管理番号': '80002'}),
            self.create_valid_row(columns, {'委託先法人ID': '1', '管理番号': '80003'}),  # 除外
        ]

        csv_content = self.create_test_csv(rows, columns)
        result_df, logs, filename, stats = process_gb_sms_contract_data(
            csv_content, date.today()
        )

        assert stats['initial_rows'] == 3
        assert stats['processed_rows'] == 2
