#!/usr/bin/env python3
"""
プラザ新規登録プロセッサのテスト

テスト対象:
1. 氏名のアルファベット判定・変換機能
2. 引継情報へのメールアドレス追加機能
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import patch


class TestIsAlphabetOnly:
    """is_alphabet_onlyメソッドのテスト

    氏名がアルファベット（全角・半角）のみで構成されているかを判定する
    """

    def test_halfwidth_uppercase_alphabet_returns_true(self):
        """半角大文字アルファベットのみ → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("VUHAININH") is True
        assert converter.is_alphabet_only("NGUYEN") is True
        assert converter.is_alphabet_only("ABC") is True

    def test_halfwidth_lowercase_alphabet_returns_true(self):
        """半角小文字アルファベットのみ → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("nguyen") is True
        assert converter.is_alphabet_only("abc") is True

    def test_halfwidth_mixed_case_alphabet_returns_true(self):
        """半角大小文字混在アルファベット → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("Nguyen") is True
        assert converter.is_alphabet_only("AbCdEf") is True

    def test_fullwidth_uppercase_alphabet_returns_true(self):
        """全角大文字アルファベットのみ → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("ＶＵＨＡＩＮＩＮＨ") is True
        assert converter.is_alphabet_only("ＡＢＣＤ") is True

    def test_fullwidth_lowercase_alphabet_returns_true(self):
        """全角小文字アルファベットのみ → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("ａｂｃ") is True

    def test_fullwidth_mixed_case_alphabet_returns_true(self):
        """全角大小文字混在アルファベット → True"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("Ａｂｃ") is True

    def test_japanese_kanji_returns_false(self):
        """日本語（漢字）→ False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("田中太郎") is False
        assert converter.is_alphabet_only("山田") is False

    def test_japanese_katakana_returns_false(self):
        """日本語（カタカナ）→ False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("タナカタロウ") is False

    def test_japanese_hiragana_returns_false(self):
        """日本語（ひらがな）→ False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("たなかたろう") is False

    def test_mixed_japanese_and_alphabet_returns_false(self):
        """日本語とアルファベット混在 → False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("田中TARO") is False
        assert converter.is_alphabet_only("山田Ａ") is False

    def test_empty_string_returns_false(self):
        """空文字 → False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("") is False

    def test_none_returns_false(self):
        """None → False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only(None) is False

    def test_alphabet_with_numbers_returns_false(self):
        """アルファベットと数字混在 → False"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("ABC123") is False
        assert converter.is_alphabet_only("NGUYEN1") is False

    def test_alphabet_with_space_returns_false(self):
        """アルファベットとスペース混在 → False（スペースはremove_all_spacesで除去済み前提）"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.is_alphabet_only("NGUYEN VAN") is False
        assert converter.is_alphabet_only("ABC ") is False


class TestConvertFullwidthAlphaToHalfwidthUpper:
    """convert_fullwidth_alpha_to_halfwidth_upperメソッドのテスト

    全角アルファベットを半角大文字に変換する
    """

    def test_fullwidth_uppercase_to_halfwidth_uppercase(self):
        """全角大文字 → 半角大文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("ＶＵＨＡＩＮＩＮＨ") == "VUHAININH"
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("ＡＢＣＤ") == "ABCD"

    def test_fullwidth_lowercase_to_halfwidth_uppercase(self):
        """全角小文字 → 半角大文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("ａｂｃ") == "ABC"

    def test_fullwidth_mixed_case_to_halfwidth_uppercase(self):
        """全角大小文字混在 → 半角大文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("Ａｂｃ") == "ABC"

    def test_halfwidth_lowercase_to_halfwidth_uppercase(self):
        """半角小文字 → 半角大文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("nguyen") == "NGUYEN"
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("abc") == "ABC"

    def test_halfwidth_uppercase_unchanged(self):
        """半角大文字 → 半角大文字（変化なし）"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("NGUYEN") == "NGUYEN"
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("ABC") == "ABC"

    def test_halfwidth_mixed_case_to_halfwidth_uppercase(self):
        """半角大小文字混在 → 半角大文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("AbCdEf") == "ABCDEF"
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("Nguyen") == "NGUYEN"

    def test_empty_string_returns_empty(self):
        """空文字 → 空文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper("") == ""

    def test_none_returns_empty(self):
        """None → 空文字"""
        from processors.plaza_registration import DataConverter
        converter = DataConverter()
        assert converter.convert_fullwidth_alpha_to_halfwidth_upper(None) == ""


class TestHikitsugiInfoFormat:
    """引継情報フォーマットのテスト

    フォーマット: {処理日}　プラザ一括登録　●メールアドレス：{メール}
    """

    def test_hikitsugi_info_with_email(self):
        """メールアドレスあり → 引継情報に含まれる"""
        from processors.plaza_registration import DataConverter, PlazaProcessor

        # テスト用のデータを作成
        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['田中太郎'],
            'フリガナ': ['タナカタロウ'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': ['test@example.com'],
            '業態区分名': ['勤め人'],
            '国籍': ['日本'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': ['山田花子'],
            '連帯保証人　フリガナ': ['ヤマダハナコ'],
            '連帯保証人　続柄': ['母'],
            '連帯保証人　電話番号': ['08011112222'],
            '緊急連絡人　氏名（漢字）': ['鈴木一郎'],
            '緊急連絡人　フリガナ': ['スズキイチロウ'],
            '緊急連絡人　続柄': ['父'],
            '緊急連絡人　電話番号': ['07033334444'],
            '勤務先名': ['テスト株式会社'],
            '勤務先住所': ['東京都新宿区西新宿1-1-1'],
            '勤務先TEL': ['0312345678'],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 引継情報にメールアドレスが含まれることを確認
        hikitsugi = output_df.iloc[0]['引継情報']
        assert '●メールアドレス：test@example.com' in hikitsugi
        assert 'プラザ一括登録' in hikitsugi

    def test_hikitsugi_info_without_email(self):
        """メールアドレスなし → 引継情報に●メールアドレス：（空）が含まれる"""
        from processors.plaza_registration import DataConverter, PlazaProcessor

        # テスト用のデータを作成（メールが空）
        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['田中太郎'],
            'フリガナ': ['タナカタロウ'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': [''],  # 空のメール
            '業態区分名': ['勤め人'],
            '国籍': ['日本'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': ['山田花子'],
            '連帯保証人　フリガナ': ['ヤマダハナコ'],
            '連帯保証人　続柄': ['母'],
            '連帯保証人　電話番号': ['08011112222'],
            '緊急連絡人　氏名（漢字）': ['鈴木一郎'],
            '緊急連絡人　フリガナ': ['スズキイチロウ'],
            '緊急連絡人　続柄': ['父'],
            '緊急連絡人　電話番号': ['07033334444'],
            '勤務先名': ['テスト株式会社'],
            '勤務先住所': ['東京都新宿区西新宿1-1-1'],
            '勤務先TEL': ['0312345678'],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 引継情報に●メールアドレス：（空）が含まれることを確認
        hikitsugi = output_df.iloc[0]['引継情報']
        assert '●メールアドレス：' in hikitsugi
        assert 'プラザ一括登録' in hikitsugi
        # メールアドレスの後に何もないことを確認（末尾が「：」で終わる）
        assert hikitsugi.endswith('●メールアドレス：')


class TestNameConversion:
    """氏名変換の統合テスト

    契約者氏名・保証人１氏名・緊急連絡人１氏名がアルファベットのみの場合、
    半角大文字に変換される
    """

    def test_contractor_name_fullwidth_alphabet_converted(self):
        """契約者氏名: 全角アルファベット → 半角大文字"""
        from processors.plaza_registration import PlazaProcessor

        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['ＶＵＨＡＩＮＩＮＨ'],  # 全角アルファベット
            'フリガナ': ['ブハイニン'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': [''],
            '業態区分名': ['勤め人'],
            '国籍': ['ベトナム'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': [''],
            '連帯保証人　フリガナ': [''],
            '連帯保証人　続柄': [''],
            '連帯保証人　電話番号': [''],
            '緊急連絡人　氏名（漢字）': [''],
            '緊急連絡人　フリガナ': [''],
            '緊急連絡人　続柄': [''],
            '緊急連絡人　電話番号': [''],
            '勤務先名': [''],
            '勤務先住所': [''],
            '勤務先TEL': [''],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 契約者氏名が半角大文字に変換されていることを確認
        assert output_df.iloc[0]['契約者氏名'] == 'VUHAININH'

    def test_contractor_name_japanese_not_converted(self):
        """契約者氏名: 日本語 → 変換なし"""
        from processors.plaza_registration import PlazaProcessor

        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['田中太郎'],  # 日本語
            'フリガナ': ['タナカタロウ'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': [''],
            '業態区分名': ['勤め人'],
            '国籍': ['日本'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': [''],
            '連帯保証人　フリガナ': [''],
            '連帯保証人　続柄': [''],
            '連帯保証人　電話番号': [''],
            '緊急連絡人　氏名（漢字）': [''],
            '緊急連絡人　フリガナ': [''],
            '緊急連絡人　続柄': [''],
            '緊急連絡人　電話番号': [''],
            '勤務先名': [''],
            '勤務先住所': [''],
            '勤務先TEL': [''],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 契約者氏名がそのままであることを確認
        assert output_df.iloc[0]['契約者氏名'] == '田中太郎'

    def test_guarantor_name_fullwidth_alphabet_converted(self):
        """保証人１氏名: 全角アルファベット → 半角大文字"""
        from processors.plaza_registration import PlazaProcessor

        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['田中太郎'],
            'フリガナ': ['タナカタロウ'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': [''],
            '業態区分名': ['勤め人'],
            '国籍': ['日本'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': ['ＮＧＵＹＥＮ'],  # 全角アルファベット
            '連帯保証人　フリガナ': ['グエン'],
            '連帯保証人　続柄': ['他'],
            '連帯保証人　電話番号': ['08011112222'],
            '緊急連絡人　氏名（漢字）': [''],
            '緊急連絡人　フリガナ': [''],
            '緊急連絡人　続柄': [''],
            '緊急連絡人　電話番号': [''],
            '勤務先名': [''],
            '勤務先住所': [''],
            '勤務先TEL': [''],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 保証人１氏名が半角大文字に変換されていることを確認
        assert output_df.iloc[0]['保証人１氏名'] == 'NGUYEN'

    def test_emergency_contact_name_fullwidth_alphabet_converted(self):
        """緊急連絡人１氏名: 全角アルファベット → 半角大文字"""
        from processors.plaza_registration import PlazaProcessor

        test_data = {
            'コールセンター送信日': ['20260113'],
            '文章タイプ': ['7'],
            '部屋受付番号': ['123456'],
            '会員番号': ['10000001'],
            '号室': ['101'],
            'AP番号': ['12345'],
            '氏名（漢字）': ['田中太郎'],
            'フリガナ': ['タナカタロウ'],
            '生年月日': ['19800101'],
            '郵便番号': ['100-0001'],
            '住所': ['東京都千代田区丸の内1-1-1'],
            '物件名': ['テストマンション'],
            '電話番号': ['09012345678'],
            'メール': [''],
            '業態区分名': ['勤め人'],
            '国籍': ['日本'],
            '契約日': ['20200101'],
            '入居日': ['20200115'],
            '利用料合計': ['50000'],
            '未納利用料合計': ['100000'],
            '支払年保金額': ['0'],
            '未納年保金額': ['0'],
            '支払更新料': ['0'],
            '未納更新料': ['0'],
            '未納事務手数料': ['0'],
            '延滞合計': ['10000'],
            '事務手数料': ['5000'],
            'バーチャル口座支店番号': ['503'],
            'バーチャル口座支店名': ['テスト支店'],
            'バーチャル口座番号': ['1234567'],
            'マイペイメントurl': [''],
            '支払期日': ['20260201'],
            '連帯保証人　名（漢字）': [''],
            '連帯保証人　フリガナ': [''],
            '連帯保証人　続柄': [''],
            '連帯保証人　電話番号': [''],
            '緊急連絡人　氏名（漢字）': ['ＴＲＡＮ'],  # 全角アルファベット
            '緊急連絡人　フリガナ': ['チャン'],
            '緊急連絡人　続柄': ['他'],
            '緊急連絡人　電話番号': ['07033334444'],
            '勤務先名': [''],
            '勤務先住所': [''],
            '勤務先TEL': [''],
            '滞納スパン': ['-2']
        }
        df = pd.DataFrame(test_data)

        processor = PlazaProcessor()
        output_df = processor.convert_to_output_format(df)

        # 緊急連絡人１氏名が半角大文字に変換されていることを確認
        assert output_df.iloc[0]['緊急連絡人１氏名'] == 'TRAN'
