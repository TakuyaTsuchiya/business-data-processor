"""
IOG新規登録プロセッサのユニットテスト
Business Data Processor

主要な関数の動作を自動テストで検証
"""

import pytest
import pandas as pd
from processors.iog_registration import DataConverter, normalize_name


class TestDataConverter:
    """DataConverterクラスのテスト"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        self.converter = DataConverter()

    # ========================================
    # normalize_for_client_system() のテスト
    # ========================================

    def test_normalize_全角英数字を半角に変換(self):
        """全角英数字が半角に変換されることを確認"""
        result = self.converter.normalize_for_client_system("ＡＢＣ１２３")
        assert result == "ABC123"

    def test_normalize_半角カナを全角カナに変換(self):
        """半角カナが全角カナに変換されることを確認"""
        result = self.converter.normalize_for_client_system("ｶﾀｶﾅ")
        assert result == "カタカナ"

    def test_normalize_混在パターン(self):
        """英数字とカナの混在パターンが正しく変換されることを確認"""
        result = self.converter.normalize_for_client_system("ＡＢＣ１２３　ｶﾀｶﾅ")
        assert result == "ABC123 カタカナ"

    def test_normalize_空文字は空文字のまま(self):
        """空文字を渡しても例外が出ず、空文字が返ることを確認"""
        result = self.converter.normalize_for_client_system("")
        assert result == ""

    def test_normalize_Noneは空文字を返す(self):
        """Noneを渡しても例外が出ず、空文字が返ることを確認"""
        result = self.converter.normalize_for_client_system(None)
        assert result == ""

    def test_normalize_電話番号の変換(self):
        """全角数字の電話番号が半角に変換されることを確認"""
        result = self.converter.normalize_for_client_system("０９０－１２３４－５６７８")
        assert result == "090-1234-5678"

    # ========================================
    # clean_property_name() のテスト
    # ========================================

    def test_clean_property_name_半角数字コード除去(self):
        """物件名から半角数字コードが除去されることを確認"""
        result = self.converter.clean_property_name("02180 サン・ガーデン 303")
        assert result == "サン・ガーデン 303"

    def test_clean_property_name_全角数字コード除去(self):
        """物件名から全角数字コードが除去されることを確認"""
        result = self.converter.clean_property_name("０２１８０　サン・ガーデン　３０３")
        assert result == "サン・ガーデン　３０３"

    def test_clean_property_name_コードなし(self):
        """コードがない物件名はそのまま返されることを確認"""
        result = self.converter.clean_property_name("サン・ガーデン 303")
        assert result == "サン・ガーデン 303"

    def test_clean_property_name_空文字(self):
        """空文字を渡しても例外が出ないことを確認"""
        result = self.converter.clean_property_name("")
        assert result == ""

    def test_clean_property_name_数字のみ除去されない(self):
        """スペースのない数字のみは除去されないことを確認"""
        result = self.converter.clean_property_name("12345サン・ガーデン")
        assert result == "12345サン・ガーデン"

    # ========================================
    # extract_room_from_property_name() のテスト
    # ========================================

    def test_extract_room_正常系_半角数字コード付き(self):
        """物件名と部屋番号が正しく分割されることを確認（半角コード付き）"""
        name, room = self.converter.extract_room_from_property_name("02180 サン・ガーデン 303")
        assert name == "サン・ガーデン"
        assert room == "303"

    def test_extract_room_正常系_全角数字コード付き(self):
        """物件名と部屋番号が正しく分割されることを確認（全角コード付き）"""
        name, room = self.converter.extract_room_from_property_name("０２１８０　サン・ガーデン　３０３")
        # 正規化後の部屋番号は半角になる
        assert name == "サン・ガーデン"
        assert room == "３０３"  # clean_property_name後の状態

    def test_extract_room_コードなし(self):
        """コードなしでも部屋番号が抽出されることを確認"""
        name, room = self.converter.extract_room_from_property_name("サン・ガーデン 303")
        assert name == "サン・ガーデン"
        assert room == "303"

    def test_extract_room_部屋番号なし(self):
        """部屋番号がない場合、物件名のみ返されることを確認"""
        name, room = self.converter.extract_room_from_property_name("サン・ガーデン")
        assert name == "サン・ガーデン"
        assert room == ""

    def test_extract_room_空文字(self):
        """空文字を渡しても例外が出ないことを確認"""
        name, room = self.converter.extract_room_from_property_name("")
        assert name == ""
        assert room == ""

    # ========================================
    # safe_str_convert() のテスト
    # ========================================

    def test_safe_str_convert_正常な文字列(self):
        """通常の文字列が正しく変換されることを確認"""
        result = self.converter.safe_str_convert("テスト")
        assert result == "テスト"

    def test_safe_str_convert_数値(self):
        """数値が文字列に変換されることを確認"""
        result = self.converter.safe_str_convert(12345)
        assert result == "12345"

    def test_safe_str_convert_NaN(self):
        """NaNが空文字に変換されることを確認"""
        result = self.converter.safe_str_convert(pd.NA)
        assert result == ""

    def test_safe_str_convert_None(self):
        """Noneが空文字に変換されることを確認"""
        result = self.converter.safe_str_convert(None)
        assert result == ""

    def test_safe_str_convert_空白文字を除去(self):
        """前後の空白が除去されることを確認"""
        result = self.converter.safe_str_convert("  テスト  ")
        assert result == "テスト"

    # ========================================
    # remove_all_spaces() のテスト
    # ========================================

    def test_remove_all_spaces_半角スペース除去(self):
        """半角スペースが除去されることを確認"""
        result = self.converter.remove_all_spaces("山田 太郎")
        assert result == "山田太郎"

    def test_remove_all_spaces_全角スペース除去(self):
        """全角スペースが除去されることを確認"""
        result = self.converter.remove_all_spaces("山田　太郎")
        assert result == "山田太郎"

    def test_remove_all_spaces_混在スペース除去(self):
        """混在スペースが全て除去されることを確認"""
        result = self.converter.remove_all_spaces("山田 太郎　次郎")
        assert result == "山田太郎次郎"

    def test_remove_all_spaces_空文字(self):
        """空文字を渡しても例外が出ないことを確認"""
        result = self.converter.remove_all_spaces("")
        assert result == ""

    # ========================================
    # normalize_phone_number() のテスト
    # ========================================

    def test_normalize_phone_number_ハイフン付き(self):
        """ハイフン付き電話番号が正規化されることを確認"""
        result = self.converter.normalize_phone_number("03-1234-5678")
        assert result == "03-1234-5678"

    def test_normalize_phone_number_全角数字(self):
        """全角数字が半角に変換されることを確認"""
        result = self.converter.normalize_phone_number("０３－１２３４－５６７８")
        assert result == "03-1234-5678"

    def test_normalize_phone_number_括弧付き(self):
        """括弧付き電話番号が正規化されることを確認"""
        result = self.converter.normalize_phone_number("（03）1234-5678")
        assert result == "(03)1234-5678"

    def test_normalize_phone_number_空文字(self):
        """空文字を渡すと空文字が返ることを確認"""
        result = self.converter.normalize_phone_number("")
        assert result == ""

    def test_normalize_phone_number_電話無(self):
        """「電話無」を渡すと空文字が返ることを確認"""
        result = self.converter.normalize_phone_number("電話無")
        assert result == ""


class TestNormalizeName:
    """normalize_name() 関数のテスト"""

    def test_normalize_name_全角スペースを半角に(self):
        """全角スペースが半角スペースに変換されることを確認"""
        result = normalize_name("山田　太郎")
        assert result == "山田 太郎"

    def test_normalize_name_連続スペースを1つに(self):
        """連続スペースが1つに統一されることを確認"""
        result = normalize_name("山田  太郎")
        assert result == "山田 太郎"

    def test_normalize_name_前後の空白削除(self):
        """前後の空白が削除されることを確認"""
        result = normalize_name("  山田 太郎  ")
        assert result == "山田 太郎"

    def test_normalize_name_空文字(self):
        """空文字を渡すと空文字が返ることを確認"""
        result = normalize_name("")
        assert result == ""

    def test_normalize_name_None(self):
        """Noneを渡すと空文字が返ることを確認"""
        result = normalize_name(None)
        assert result == ""


class TestIntegration:
    """統合テスト（複数の関数を組み合わせた動作確認）"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        self.converter = DataConverter()

    def test_物件名処理の完全フロー(self):
        """物件名クリーニング → 部屋番号抽出 → 正規化の完全フローを確認"""
        # 入力: 全角コード付き物件名
        raw_name = "０２１８０　サン・ガーデン　３０３"

        # Step1: 部屋番号抽出（内部でclean_property_nameが呼ばれる）
        property_name, room_number = self.converter.extract_room_from_property_name(raw_name)

        # Step2: 正規化
        normalized_property = self.converter.normalize_for_client_system(property_name)
        normalized_room = self.converter.normalize_for_client_system(room_number)

        # 検証
        assert normalized_property == "サン・ガーデン"
        assert normalized_room == "303"

    def test_契約者情報の完全フロー(self):
        """契約者情報の変換フローを確認"""
        # 入力データ
        raw_name = "山田　太郎"
        raw_kana = "ﾔﾏﾀﾞ　ﾀﾛｳ"

        # Step1: スペース除去
        name = self.converter.remove_all_spaces(raw_name)
        kana = self.converter.remove_all_spaces(raw_kana)

        # Step2: 正規化
        normalized_name = self.converter.normalize_for_client_system(name)
        normalized_kana = self.converter.normalize_for_client_system(kana)

        # 検証
        assert normalized_name == "山田太郎"
        assert normalized_kana == "ヤマダタロウ"

    def test_譲渡一覧の氏名を優先(self):
        """旧姓表記がある場合、譲渡一覧の賃借人氏名を優先することを確認"""
        # 譲渡一覧の氏名（新姓）とIOGデータの氏名（旧姓表記あり）が異なる場合を想定
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["田中 花子（旧姓佐藤 花子）"],  # 旧姓表記あり
            "賃借人氏名": ["田中 花子"],  # 新姓（譲渡一覧から）
            "フリガナ": ["タナカ ハナコ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # 譲渡一覧の氏名（新姓）が使われることを確認
        assert result_df.loc[0, "契約者氏名"] == "田中花子"

    def test_譲渡一覧なしの場合はIOG氏名を使用(self):
        """譲渡一覧データがない場合、対象者名を使用することを確認"""
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["佐藤 花子"],
            "フリガナ": ["サトウ ハナコ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=False)

        # IOGデータの対象者名が使われることを確認
        assert result_df.loc[0, "契約者氏名"] == "佐藤花子"

    def test_譲渡一覧の氏名が空の場合はIOG氏名を使用(self):
        """譲渡一覧データがあるが賃借人氏名が空の場合、対象者名を使用することを確認"""
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["佐藤 花子"],
            "賃借人氏名": [""],  # 空
            "フリガナ": ["サトウ ハナコ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # 賃借人氏名が空なのでIOGデータの対象者名が使われることを確認
        assert result_df.loc[0, "契約者氏名"] == "佐藤花子"


class TestOldNameNotation:
    """旧姓表記処理のテスト（Issue #62対応）"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        self.converter = DataConverter()

    # ========================================
    # has_old_name_notation() のテスト
    # ========================================

    def test_旧姓表記検出_全角括弧(self):
        """全角括弧付き旧姓表記を検出"""
        assert self.converter.has_old_name_notation("井上紗樹（旧姓谷紗樹）") is True

    def test_旧姓表記検出_半角括弧(self):
        """半角括弧付き旧姓表記を検出"""
        assert self.converter.has_old_name_notation("井上紗樹(旧姓谷紗樹)") is True

    def test_旧姓表記検出_括弧なし(self):
        """括弧なし旧姓表記を検出"""
        assert self.converter.has_old_name_notation("井上紗樹 旧姓谷紗樹") is True

    def test_旧姓表記検出_短縮形(self):
        """短縮形の旧姓表記を検出"""
        assert self.converter.has_old_name_notation("井上紗樹（旧谷）") is True

    def test_旧姓表記なし(self):
        """旧姓表記がない場合はFalseを返す"""
        assert self.converter.has_old_name_notation("井上紗樹") is False

    def test_旧姓表記検出_空文字(self):
        """空文字の場合はFalseを返す"""
        assert self.converter.has_old_name_notation("") is False

    def test_旧姓表記検出_None(self):
        """Noneの場合はFalseを返す"""
        assert self.converter.has_old_name_notation(None) is False

    # ========================================
    # remove_old_name_notation() のテスト
    # ========================================

    def test_旧姓除去_全角括弧(self):
        """全角括弧付き旧姓表記を除去"""
        result = self.converter.remove_old_name_notation("井上紗樹（旧姓谷紗樹）")
        assert result == "井上紗樹"

    def test_旧姓除去_半角括弧(self):
        """半角括弧付き旧姓表記を除去"""
        result = self.converter.remove_old_name_notation("井上紗樹(旧姓谷紗樹)")
        assert result == "井上紗樹"

    def test_旧姓除去_括弧なし(self):
        """括弧なし旧姓表記を除去"""
        result = self.converter.remove_old_name_notation("井上紗樹 旧姓谷紗樹")
        assert result == "井上紗樹"

    def test_旧姓除去_短縮形(self):
        """短縮形の旧姓表記を除去"""
        result = self.converter.remove_old_name_notation("井上紗樹（旧谷）")
        assert result == "井上紗樹"

    def test_旧姓除去_前後の空白トリム(self):
        """除去後の前後の空白をトリム"""
        result = self.converter.remove_old_name_notation("井上紗樹　（旧姓谷紗樹）")
        assert result == "井上紗樹"

    def test_旧姓除去_旧姓表記なし(self):
        """旧姓表記がない場合はそのまま返す"""
        result = self.converter.remove_old_name_notation("井上紗樹")
        assert result == "井上紗樹"

    def test_旧姓除去_空文字(self):
        """空文字の場合は空文字を返す"""
        result = self.converter.remove_old_name_notation("")
        assert result == ""

    # ========================================
    # convert_jid_data_with_transfer() の統合テスト
    # ========================================

    def test_旧姓表記あり_譲渡一覧あり(self):
        """旧姓表記がある場合、譲渡一覧の氏名を優先"""
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["井上紗樹（旧姓谷紗樹）"],
            "賃借人氏名": ["井上紗樹"],
            "フリガナ": ["イノウエサキ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # 譲渡一覧の氏名が使われることを確認
        assert result_df.loc[0, "契約者氏名"] == "井上紗樹"
        # ログに旧姓処理が記録されることを確認
        assert any("旧姓表記を検出・処理: 1件" in log for log in logs)

    def test_旧姓表記あり_譲渡一覧なし(self):
        """旧姓表記がある場合、旧姓部分を除去"""
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["井上紗樹（旧姓谷紗樹）"],
            "賃借人氏名": [""],
            "フリガナ": ["イノウエサキ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # 旧姓部分が除去されることを確認
        assert result_df.loc[0, "契約者氏名"] == "井上紗樹"
        # ログに旧姓削除が記録されることを確認
        assert any("IOGデータから旧姓削除: 1件" in log for log in logs)

    def test_旧姓表記なし_譲渡一覧なし(self):
        """旧姓表記がない場合、IOGの氏名をそのまま使用"""
        test_data = pd.DataFrame({
            "契約番号": ["001"],
            "対象者名": ["田中太郎"],
            "賃借人氏名": [""],
            "フリガナ": ["タナカタロウ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # IOGの氏名がそのまま使われることを確認
        assert result_df.loc[0, "契約者氏名"] == "田中太郎"
        # 旧姓処理のログは出力されないことを確認
        assert not any("旧姓表記を検出" in log for log in logs)

    def test_旧姓表記複数件処理(self):
        """複数件の旧姓表記を正しく処理"""
        test_data = pd.DataFrame({
            "契約番号": ["001", "002", "003"],
            "対象者名": [
                "井上紗樹（旧姓谷紗樹）",
                "佐藤花子(旧姓田中花子)",
                "鈴木一郎"
            ],
            "賃借人氏名": ["井上紗樹", "", ""],
            "フリガナ": ["イノウエサキ", "サトウハナコ", "スズキイチロウ"]
        })

        result_df, logs = self.converter.convert_jid_data_with_transfer(test_data, has_transfer=True)

        # 各氏名が正しく処理されることを確認
        assert result_df.loc[0, "契約者氏名"] == "井上紗樹"  # 譲渡一覧から
        assert result_df.loc[1, "契約者氏名"] == "佐藤花子"  # 旧姓削除
        assert result_df.loc[2, "契約者氏名"] == "鈴木一郎"  # そのまま

        # ログに2件の旧姓処理が記録されることを確認
        assert any("旧姓表記を検出・処理: 2件" in log for log in logs)
