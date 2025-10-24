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
    """normalize_name() 関数のテスト（6ステップ正規化）"""

    def test_normalize_name_全スペース除去(self):
        """全角・半角スペースが完全に除去されることを確認"""
        assert normalize_name("山田　太郎") == "山田太郎"
        assert normalize_name("山田 太郎") == "山田太郎"
        assert normalize_name("山田  太郎") == "山田太郎"

    def test_normalize_name_半角カナをNFKC正規化(self):
        """半角カナが全角カナに変換されることを確認"""
        assert normalize_name("ｲ ｼﾞﾆ") == "イジニ"
        assert normalize_name("ﾔﾏﾀﾞ ﾀﾛｳ") == "ヤマダタロウ"

    def test_normalize_name_異体字を統一(self):
        """漢字異体字が標準字体に統一されることを確認"""
        assert normalize_name("髙橋由美") == "高橋由美"
        assert normalize_name("﨑本花子") == "崎本花子"

    def test_normalize_name_通称を除去(self):
        """通称表記が除去されることを確認"""
        assert normalize_name("姜利一通称山本昭雄") == "姜利一山本昭雄"
        assert normalize_name("(通称)水山 義常") == "()水山義常"

    def test_normalize_name_全角英数を半角に(self):
        """全角英数字が半角に変換されることを確認"""
        assert normalize_name("ＡＢＣ１２３") == "ABC123"

    def test_normalize_name_複合パターン(self):
        """通称・異体字・半角カナ・スペースの複合パターンを確認"""
        # 通称 + 異体字 + スペース
        assert normalize_name("髙橋 由美通称佐藤 由美") == "高橋由美佐藤由美"
        # 半角カナ + 異体字 + スペース
        assert normalize_name("ﾀｶﾊｼ 髙子") == "タカハシ高子"

    def test_normalize_name_空文字(self):
        """空文字を渡すと空文字が返ることを確認"""
        assert normalize_name("") == ""

    def test_normalize_name_None(self):
        """Noneを渡すと空文字が返ることを確認"""
        assert normalize_name(None) == ""


class TestRemoveTsusho:
    """remove_tsusho() 関数のテスト"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        from processors.iog_registration import remove_tsusho
        self.remove_tsusho = remove_tsusho

    def test_remove_tsusho_括弧付き通称(self):
        """括弧付き通称が正しく除去されることを確認"""
        result = self.remove_tsusho("(通称)水山 義常　KANG RYOON")
        assert result == "()水山 義常　KANG RYOON"

    def test_remove_tsusho_括弧なし通称(self):
        """括弧なし通称が正しく除去されることを確認"""
        result = self.remove_tsusho("姜利一通称山本昭雄")
        assert result == "姜利一山本昭雄"

    def test_remove_tsusho_英字と通称(self):
        """英字と通称の複合パターンが正しく処理されることを確認"""
        result = self.remove_tsusho("ISHIMOTOCINTIAHARUMI通称石元ハルミ")
        assert result == "ISHIMOTOCINTIAHARUMI石元ハルミ"

    def test_remove_tsusho_通称なし(self):
        """通称がない場合、そのまま返されることを確認"""
        result = self.remove_tsusho("山田太郎")
        assert result == "山田太郎"

    def test_remove_tsusho_空文字(self):
        """空文字を渡すと空文字が返ることを確認"""
        result = self.remove_tsusho("")
        assert result == ""

    def test_remove_tsusho_None(self):
        """Noneを渡すと空文字が返ることを確認"""
        result = self.remove_tsusho(None)
        assert result == ""

    def test_remove_tsusho_複数の通称(self):
        """複数の「通称」が全て除去されることを確認"""
        result = self.remove_tsusho("姜利一通称山本昭雄通称田中太郎")
        assert result == "姜利一山本昭雄田中太郎"


class TestNormalizeKanjiVariants:
    """normalize_kanji_variants() 関数のテスト"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        from processors.iog_registration import normalize_kanji_variants
        self.normalize_kanji_variants = normalize_kanji_variants

    def test_normalize_kanji_variants_はしごだか(self):
        """はしごだか（髙）が標準字体（高）に変換されることを確認"""
        result = self.normalize_kanji_variants("髙橋由美")
        assert result == "高橋由美"

    def test_normalize_kanji_variants_たつさき(self):
        """たつさき（﨑）が標準字体（崎）に変換されることを確認"""
        result = self.normalize_kanji_variants("﨑本花子")
        assert result == "崎本花子"

    def test_normalize_kanji_variants_濵(self):
        """濵が浜に変換されることを確認"""
        result = self.normalize_kanji_variants("濵田太郎")
        assert result == "浜田太郎"

    def test_normalize_kanji_variants_邊(self):
        """邊が辺に変換されることを確認"""
        result = self.normalize_kanji_variants("渡邊次郎")
        assert result == "渡辺次郎"

    def test_normalize_kanji_variants_複数の異体字(self):
        """複数の異体字が同時に変換されることを確認"""
        result = self.normalize_kanji_variants("髙橋﨑本濵田邊")
        assert result == "高橋崎本浜田辺"

    def test_normalize_kanji_variants_異体字なし(self):
        """異体字がない場合、そのまま返されることを確認"""
        result = self.normalize_kanji_variants("山田太郎")
        assert result == "山田太郎"

    def test_normalize_kanji_variants_空文字(self):
        """空文字を渡すと空文字が返ることを確認"""
        result = self.normalize_kanji_variants("")
        assert result == ""

    def test_normalize_kanji_variants_None(self):
        """Noneを渡すと空文字が返ることを確認"""
        result = self.normalize_kanji_variants(None)
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


class TestPartialNameMatching:
    """部分一致マッチングのテスト"""

    def setup_method(self):
        """各テストの前に実行される準備処理"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from processors.iog_registration import find_matching_transfer, merge_transfer_data

        self.find_matching_transfer = find_matching_transfer
        self.merge_transfer_data = merge_transfer_data

    def test_部分一致_繰り返し氏名パターン(self):
        """繰り返し氏名パターンで部分一致が機能することを確認"""
        transfer_df = pd.DataFrame({
            "賃借人氏名": ["谷田絵理"],
            "物件名": ["サンプル物件"]
        })

        matched = self.find_matching_transfer("佐藤絵理福岡絵理谷田絵理", transfer_df)
        assert matched is not None
        assert matched["賃借人氏名"] == "谷田絵理"

    def test_部分一致_複数回結婚パターン(self):
        """複数回結婚パターンで部分一致が機能することを確認"""
        transfer_df = pd.DataFrame({
            "賃借人氏名": ["長野美生"],
            "物件名": ["テスト物件"]
        })

        matched = self.find_matching_transfer("岩城美生長野美生", transfer_df)
        assert matched is not None
        assert matched["賃借人氏名"] == "長野美生"

    def test_部分一致_3文字未満は誤マッチ防止(self):
        """3文字未満の名前は誤マッチしないことを確認"""
        transfer_df = pd.DataFrame({
            "賃借人氏名": ["太郎"],  # 2文字
            "物件名": ["誤マッチ物件"]
        })

        # "山田太郎" に "太郎" が含まれるが、3文字未満なのでマッチしない
        matched = self.find_matching_transfer("山田太郎", transfer_df)
        assert matched is None

    def test_部分一致_完全一致優先(self):
        """完全一致がある場合は部分一致より優先されることを確認"""
        jid_df = pd.DataFrame({
            "対象者名": ["田中花子", "佐藤絵理福岡絵理谷田絵理"]
        })

        transfer_df = pd.DataFrame({
            "賃借人氏名": ["田中花子", "谷田絵理"],
            "物件名": ["物件A", "物件B"],
            "_source_info": ["完全一致", "部分一致"]
        })

        merged_df, duplicates, match_stats = self.merge_transfer_data(jid_df, transfer_df)

        # 統計確認
        assert match_stats["exact"] == 1  # 完全一致 1件
        assert match_stats["partial"] == 1  # 部分一致 1件

        # データ確認
        assert merged_df.loc[0, "物件名"] == "物件A"  # 完全一致
        assert merged_df.loc[1, "物件名"] == "物件B"  # 部分一致

    def test_部分一致_マッチなし(self):
        """部分一致でもマッチしない場合の確認"""
        transfer_df = pd.DataFrame({
            "賃借人氏名": ["山田太郎"],
            "物件名": ["物件X"]
        })

        # 全く異なる名前
        matched = self.find_matching_transfer("鈴木一郎", transfer_df)
        assert matched is None

    def test_統合_2段階マッチング(self):
        """完全一致と部分一致が両方機能する統合テスト"""
        jid_df = pd.DataFrame({
            "対象者名": [
                "田中花子",                      # 完全一致する
                "佐藤絵理福岡絵理谷田絵理",      # 部分一致する
                "鈴木一郎"                       # マッチしない
            ]
        })

        transfer_df = pd.DataFrame({
            "賃借人氏名": ["田中花子", "谷田絵理"],
            "物件名": ["物件A", "物件B"],
            "_source_info": ["完全", "部分"]
        })

        merged_df, duplicates, match_stats = self.merge_transfer_data(jid_df, transfer_df)

        # 統計確認
        assert match_stats["exact"] == 1
        assert match_stats["partial"] == 1

        # データ確認
        assert merged_df.loc[0, "物件名"] == "物件A"  # 完全一致
        assert merged_df.loc[1, "物件名"] == "物件B"  # 部分一致
        assert pd.isna(merged_df.loc[2, "物件名"])    # マッチなし
