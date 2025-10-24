#!/usr/bin/env python3
"""
JID新規登録プロセッサ
日本賃貸保証（JID）返却データを111列テンプレートに変換

入力ファイル:
- 合同会社IOG（日本賃貸保証返却データ）YYYYMMDD.xlsx

出力ファイル:
- MMDDjid_新規登録.csv (111列) - アーク新規登録と同じテンプレート準拠
"""

import pandas as pd
import io
import unicodedata
from datetime import datetime
from typing import Tuple, List, Dict
import logging
from processors.common.detailed_logger import DetailedLogger
from processors.common.address_splitter import AddressSplitter


class IOGConfig:
    """IOG新規登録設定・定数管理クラス"""

    OUTPUT_FILE_PREFIX = "iog_新規登録"

    # 【最重要】正確な111列テンプレートヘッダー（アーク新規登録と同一）
    OUTPUT_COLUMNS = [
        "引継番号", "契約者氏名", "契約者カナ", "契約者生年月日",
        "契約者TEL自宅", "契約者TEL携帯", "契約者現住所郵便番号",
        "契約者現住所1", "契約者現住所2", "契約者現住所3",
        "引継情報", "物件名", "部屋番号",
        "物件住所郵便番号", "物件住所1", "物件住所2", "物件住所3",
        "入居ステータス", "滞納ステータス", "受託状況",
        "月額賃料", "管理費", "共益費", "水道代", "駐車場代",
        "その他費用1", "その他費用2", "敷金", "礼金",
        "回収口座金融機関CD", "回収口座金融機関名", "回収口座支店CD", "回収口座支店名",
        "回収口座種類", "回収口座番号", "回収口座名義",
        "契約種類", "管理受託日", "契約確認日",
        "退去済手数料", "入居中滞納手数料", "入居中正常手数料",
        "管理前滞納額", "更新契約手数料", "退去手続き（実費）",
        "初回振替月", "保証開始日", "クライアントCD", "パートナーCD",
        "契約者勤務先名", "契約者勤務先カナ", "契約者勤務先TEL", "勤務先業種",
        "契約者勤務先郵便番号", "契約者勤務先住所1", "契約者勤務先住所2", "契約者勤務先住所3",
        "保証人１氏名", "保証人１カナ", "保証人１契約者との関係", "保証人１生年月日",
        "保証人１郵便番号", "保証人１住所1", "保証人１住所2", "保証人１住所3",
        "保証人１TEL自宅", "保証人１TEL携帯",
        "保証人２氏名", "保証人２カナ", "保証人２契約者との関係", "保証人２生年月日",
        "保証人２郵便番号", "保証人２住所1", "保証人２住所2", "保証人２住所3",
        "保証人２TEL自宅", "保証人２TEL携帯",
        "緊急連絡人１氏名", "緊急連絡人１カナ", "緊急連絡人１契約者との関係",
        "緊急連絡人１郵便番号", "緊急連絡人１現住所1", "緊急連絡人１現住所2", "緊急連絡人１現住所3",
        "緊急連絡人１TEL自宅", "緊急連絡人１TEL携帯",
        "緊急連絡人２氏名", "緊急連絡人２カナ", "緊急連絡人２契約者との関係",
        "緊急連絡人２郵便番号", "緊急連絡人２現住所1", "緊急連絡人２現住所2", "緊急連絡人２現住所3",
        "緊急連絡人２TEL自宅", "緊急連絡人２TEL携帯",
        "保証入金日", "保証入金者",
        "引落銀行CD", "引落銀行名", "引落支店CD", "引落支店名",
        "引落預金種別", "引落口座番号", "引落口座名義",
        "解約日", "管理会社", "委託先法人ID",
        "", "", "",  # 108-110番目: 空列
        "登録フラグ"  # 111番目
    ]

    # 固定値設定
    FIXED_VALUES = {
        # ステータス関連
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "契約種類": "バックレント",

        # 回収口座関連
        "回収口座金融機関CD": "310",
        "回収口座金融機関名": "GMOあおぞらネット銀行",
        "回収口座種類": "普通",
        "回収口座名義": "",

        # その他固定値
        "クライアントCD": "",
        "水道代": "0",
        "更新契約手数料": "1",
        "退去済手数料": "0",
        "入居中滞納手数料": "0",
        "入居中正常手数料": "0",
        "委託先法人ID": "3",

        # 空白項目
        "契約者生年月日": "",
        "管理費": "",
        "契約確認日": "",
        "保証開始日": "",
        "パートナーCD": "",
        "引落預金種別": "",
        "管理会社": "",

        # 勤務先情報（全て空白）
        "契約者勤務先名": "",
        "契約者勤務先カナ": "",
        "契約者勤務先TEL": "",
        "勤務先業種": "",
        "契約者勤務先郵便番号": "",
        "契約者勤務先住所1": "",
        "契約者勤務先住所2": "",
        "契約者勤務先住所3": "",

        # 保証人1情報（全て空白）
        "保証人１氏名": "", "保証人１カナ": "", "保証人１契約者との関係": "", "保証人１生年月日": "",
        "保証人１郵便番号": "", "保証人１住所1": "", "保証人１住所2": "", "保証人１住所3": "",
        "保証人１TEL自宅": "", "保証人１TEL携帯": "",

        # 保証人2情報（全て空白）
        "保証人２氏名": "", "保証人２カナ": "", "保証人２契約者との関係": "", "保証人２生年月日": "",
        "保証人２郵便番号": "", "保証人２住所1": "", "保証人２住所2": "", "保証人２住所3": "",
        "保証人２TEL自宅": "", "保証人２TEL携帯": "",

        # 緊急連絡人1情報（全て空白）
        "緊急連絡人１氏名": "", "緊急連絡人１カナ": "", "緊急連絡人１契約者との関係": "",
        "緊急連絡人１郵便番号": "", "緊急連絡人１現住所1": "", "緊急連絡人１現住所2": "", "緊急連絡人１現住所3": "",
        "緊急連絡人１TEL自宅": "", "緊急連絡人１TEL携帯": "",

        # 緊急連絡人2情報（全て空白）
        "緊急連絡人２氏名": "", "緊急連絡人２カナ": "", "緊急連絡人２契約者との関係": "",
        "緊急連絡人２郵便番号": "", "緊急連絡人２現住所1": "", "緊急連絡人２現住所2": "", "緊急連絡人２現住所3": "",
        "緊急連絡人２TEL自宅": "", "緊急連絡人２TEL携帯": "",

        # 引落・その他情報（全て空白）
        "保証入金日": "", "保証入金者": "",
        "引落銀行CD": "", "引落銀行名": "", "引落支店CD": "", "引落支店名": "",
        "引落口座番号": "", "引落口座名義": "", "解約日": "",

        # 物件情報（全て空白）
        "物件名": "", "部屋番号": "",
        "物件住所郵便番号": "", "物件住所1": "", "物件住所2": "", "物件住所3": "",

        # 金額情報（全て空白または0）
        "月額賃料": "0",
        "共益費": "0",
        "駐車場代": "0",
        "その他費用1": "0",
        "その他費用2": "0",
        "敷金": "0",
        "礼金": "0",
        "退去手続き（実費）": "70000",  # 最低値
        "初回振替月": "",

        # 回収口座番号・支店情報（全て空白）
        "回収口座支店CD": "",
        "回収口座支店名": "",
        "回収口座番号": "",

        # 引継情報（空白）
        "引継情報": ""
    }


class DataConverter:
    """データ変換クラス"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.address_splitter = AddressSplitter()

    def safe_str_convert(self, value) -> str:
        """安全な文字列変換"""
        if pd.isna(value):
            return ""
        return str(value).strip()

    def remove_all_spaces(self, text: str) -> str:
        """全てのスペースを除去"""
        if not text:
            return ""
        return text.replace(" ", "").replace("　", "")

    def hankaku_to_zenkaku(self, text: str) -> str:
        """半角カナを全角カナに変換"""
        if not text:
            return ""
        return unicodedata.normalize('NFKC', text)

    def normalize_for_client_system(self, text: str) -> str:
        """
        クライアントシステム用正規化（英数字=半角、カタカナ=全角）

        Args:
            text: 正規化する文字列

        Returns:
            str: 英数字が半角、カタカナが全角に統一された文字列
        """
        if not text:
            return ""

        # Step1: 半角カナを全角カナに変換（NFKC正規化）
        normalized = unicodedata.normalize('NFKC', text)

        # Step2: 全角英数字を半角に変換
        # 全角英数字の範囲：Ａ-Ｚ（U+FF21-FF3A）、ａ-ｚ（U+FF41-FF5A）、０-９（U+FF10-FF19）
        result = []
        for char in normalized:
            code = ord(char)
            # 全角英大文字 A-Z → 半角
            if 0xFF21 <= code <= 0xFF3A:
                result.append(chr(code - 0xFEE0))
            # 全角英小文字 a-z → 半角
            elif 0xFF41 <= code <= 0xFF5A:
                result.append(chr(code - 0xFEE0))
            # 全角数字 0-9 → 半角
            elif 0xFF10 <= code <= 0xFF19:
                result.append(chr(code - 0xFEE0))
            else:
                result.append(char)

        return ''.join(result)

    def normalize_phone_number(self, value: str) -> str:
        """電話番号の正規化"""
        if pd.isna(value) or str(value).strip() == "" or str(value).strip() == "電話無":
            return ""

        phone = str(value).strip()

        # 全角数字を半角に変換
        phone = unicodedata.normalize('NFKC', phone)

        # 記号を統一
        phone = phone.replace("－", "-").replace("ー", "-").replace("‐", "-")
        phone = phone.replace("（", "(").replace("）", ")")

        # 不要な文字を除去（数字、ハイフン、括弧のみ残す）
        import re
        phone = re.sub(r"[^\d\-\(\)]", "", phone)

        return phone


    def split_address(self, address: str) -> Dict[str, str]:
        """住所を郵便番号、都道府県、市区町村、残り住所に分割（辞書方式）"""
        if pd.isna(address) or str(address).strip() == "":
            return {"postal_code": "", "prefecture": "", "city": "", "remaining": ""}

        # AddressSplitterを使用
        return self.address_splitter.split_address(str(address))

    def clean_property_name(self, property_name: str) -> str:
        """
        物件名から先頭の数字コードを除去

        Args:
            property_name: 物件名（例: "02180 サン・ガーデン 303"）

        Returns:
            str: クリーニング済み物件名（例: "サン・ガーデン 303"）
        """
        if pd.isna(property_name) or str(property_name).strip() == "":
            return ""

        import re
        cleaned = str(property_name).strip()

        # 先頭の「数字（全角半角）+ スペース（全角半角）」パターンを除去
        cleaned = re.sub(r'^[0-9０-９]+[\s　]+', '', cleaned)

        return cleaned.strip()

    def extract_room_from_property_name(self, property_name: str) -> Tuple[str, str]:
        """物件名から部屋番号を抽出（先頭コードクリーニング済み）"""
        if pd.isna(property_name) or str(property_name).strip() == "":
            return "", ""

        # 先頭の数字コードをクリーニング
        prop_name = self.clean_property_name(property_name)

        # 部屋番号パターンを検索（例：３Ｇ５、２０４、101号室など）
        import re
        room_patterns = [
            r'[　\s]([０-９A-Zア-ンＡ-Ｚ\d]+)$',  # 全角・半角混在の末尾パターン
            r'(\d+)号室?',
            r'([A-Z]?\d+)号',
            r'([０-９Ａ-Ｚ]{2,})$',  # 全角数字・アルファベット
            r'(\d{3,4})$'
        ]

        for pattern in room_patterns:
            match = re.search(pattern, prop_name)
            if match:
                room_num = match.group(1).strip()
                clean_prop_name = re.sub(pattern, '', prop_name).strip()
                return clean_prop_name, room_num

        return prop_name, ""

    def process_phone_numbers(self, home_tel: str, mobile_tel: str) -> Dict[str, str]:
        """電話番号処理（自宅TELのみの場合は携帯TELに移動）"""
        home = self.normalize_phone_number(home_tel)
        mobile = self.normalize_phone_number(mobile_tel)

        # 自宅TELのみの場合、携帯TELに移動
        if home and not mobile:
            return {"home": "", "mobile": home}

        return {"home": home, "mobile": mobile}

    def parse_date(self, date_value: str) -> str:
        """日付のパースとフォーマット変換（YYYY/MM/DD形式に統一）"""
        if pd.isna(date_value) or str(date_value).strip() == "":
            return ""

        try:
            # pandas Timestampの場合
            if isinstance(date_value, pd.Timestamp):
                return date_value.strftime("%Y/%m/%d")

            # 文字列の場合
            date_str = str(date_value).strip()

            # 既に YYYY/MM/DD 形式の場合
            if "/" in date_str:
                parts = date_str.split()[0].split("/")  # 時刻部分を除去
                if len(parts) == 3:
                    return date_str.split()[0]

            # YYYY-MM-DD 形式の場合
            if "-" in date_str:
                date_str = date_str.split()[0]  # 時刻部分を除去
                return date_str.replace("-", "/")

            return date_str
        except Exception as e:
            self.logger.debug(f"日付パースエラー: {date_value} - {e}")
            return ""

    def convert_jid_data(self, jid_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """JIDデータを111列テンプレートに変換（譲渡一覧なし）"""
        return self.convert_jid_data_with_transfer(jid_df, has_transfer=False)

    def convert_jid_data_with_transfer(self, merged_df: pd.DataFrame, has_transfer: bool = False) -> Tuple[pd.DataFrame, List[str]]:
        """
        JIDデータ（譲渡一覧マージ済み）を111列テンプレートに変換

        Args:
            merged_df: マージ済みのデータフレーム
            has_transfer: 譲渡一覧データの有無

        Returns:
            Tuple[pd.DataFrame, List[str]]: (変換済みDF, 処理ログ)
        """
        output_data = []
        logs = []

        for _, row in merged_df.iterrows():
            converted_row = {}

            # 1. 基本情報（JIDデータから）
            converted_row["引継番号"] = self.safe_str_convert(row.get("契約番号", ""))

            # 2. 氏名の優先ロジック（譲渡一覧優先）
            iog_name = self.safe_str_convert(row.get("対象者名", ""))
            transfer_name = self.safe_str_convert(row.get("賃借人氏名", ""))

            # 譲渡一覧にマッチした場合は譲渡一覧の氏名を使用、それ以外はIOGの氏名をそのまま使用
            if has_transfer and transfer_name:
                final_name = transfer_name
            else:
                final_name = iog_name

            converted_row["契約者氏名"] = self.remove_all_spaces(final_name)
            converted_row["契約者カナ"] = self.remove_all_spaces(self.safe_str_convert(row.get("フリガナ", "")))

            # 2. 電話番号処理（JIDデータから）
            phone_result = self.process_phone_numbers(
                row.get("自宅電話", ""),
                row.get("携帯", "")
            )
            converted_row["契約者TEL自宅"] = phone_result["home"]
            converted_row["契約者TEL携帯"] = phone_result["mobile"]

            # 3. 住所分割処理（JIDデータから）
            address = self.safe_str_convert(row.get("自宅", ""))
            address_parts = self.split_address(address)

            # 契約者現住所
            converted_row["契約者現住所郵便番号"] = self.safe_str_convert(row.get("郵便番号", ""))
            converted_row["契約者現住所1"] = address_parts["prefecture"]
            converted_row["契約者現住所2"] = address_parts["city"]
            converted_row["契約者現住所3"] = address_parts["remaining"]

            # 4. 管理前滞納額（JIDデータから）
            converted_row["管理前滞納額"] = self.safe_str_convert(row.get("差引残高", "0"))

            # 5. 管理受託日（JIDデータから）
            converted_row["管理受託日"] = self.parse_date(row.get("受任日", ""))

            # 6. 物件情報（譲渡一覧から）
            if has_transfer:
                # 物件名から部屋番号を分割
                raw_property_name = self.safe_str_convert(row.get("物件名", ""))
                clean_property_name, room_number = self.extract_room_from_property_name(raw_property_name)

                converted_row["物件名"] = clean_property_name
                converted_row["部屋番号"] = room_number
                converted_row["物件住所郵便番号"] = self.safe_str_convert(row.get("物件郵便番号", ""))
                converted_row["物件住所1"] = self.safe_str_convert(row.get("物件都道府県", ""))
                converted_row["物件住所2"] = self.safe_str_convert(row.get("物件市区町村", ""))
                converted_row["物件住所3"] = self.safe_str_convert(row.get("物件町域名", ""))

                # 7. 保証人1（譲渡一覧から）
                converted_row["保証人１氏名"] = self.remove_all_spaces(self.safe_str_convert(row.get("連帯保証人氏名（滞納）", "")))
                converted_row["保証人１カナ"] = ""  # 譲渡一覧にカナなし

                # 続柄変換（値がある場合「他」に統一）
                guarantor_relationship = self.safe_str_convert(row.get("連帯保証人続柄名（滞納）", ""))
                converted_row["保証人１契約者との関係"] = "他" if guarantor_relationship else ""
                converted_row["保証人１郵便番号"] = self.safe_str_convert(row.get("連帯保証人郵便番号（滞納）", ""))
                converted_row["保証人１住所1"] = self.safe_str_convert(row.get("連帯保証人都道府県（滞納）", ""))
                converted_row["保証人１住所2"] = self.safe_str_convert(row.get("連帯保証人市区町村（滞納）", ""))

                # 住所3 = 町域名 + マンション名
                guarantor_addr3_parts = []
                town = self.safe_str_convert(row.get("連帯保証人町域名（滞納）", ""))
                mansion = self.safe_str_convert(row.get("連帯保証人マンションなど（滞納）", ""))
                if town:
                    guarantor_addr3_parts.append(town)
                if mansion:
                    guarantor_addr3_parts.append(mansion)
                converted_row["保証人１住所3"] = "　".join(guarantor_addr3_parts) if guarantor_addr3_parts else ""

                # 保証人電話
                guarantor_phone = self.process_phone_numbers(
                    row.get("連帯保証人電話番号（滞納）", ""),
                    row.get("連帯保証人携帯電話電話号（滞納）", "")
                )
                converted_row["保証人１TEL自宅"] = guarantor_phone["home"]
                converted_row["保証人１TEL携帯"] = guarantor_phone["mobile"]

                # 8. 緊急連絡先1（譲渡一覧から）
                converted_row["緊急連絡人１氏名"] = self.remove_all_spaces(self.safe_str_convert(row.get("緊急連絡先氏名（滞納）", "")))
                converted_row["緊急連絡人１カナ"] = ""  # 譲渡一覧にカナなし

                # 続柄変換（値がある場合「他」に統一）
                emergency_relationship = self.safe_str_convert(row.get("緊急連絡先続柄名（滞納）", ""))
                converted_row["緊急連絡人１契約者との関係"] = "他" if emergency_relationship else ""
                converted_row["緊急連絡人１郵便番号"] = self.safe_str_convert(row.get("緊急連絡先郵便番号（滞納）", ""))
                converted_row["緊急連絡人１現住所1"] = self.safe_str_convert(row.get("緊急連絡先都道府県（滞納）", ""))
                converted_row["緊急連絡人１現住所2"] = self.safe_str_convert(row.get("緊急連絡先市区町村（滞納）", ""))

                # 現住所3 = 町域名 + マンション名
                emergency_addr3_parts = []
                town2 = self.safe_str_convert(row.get("緊急連絡先町域名（滞納）", ""))
                mansion2 = self.safe_str_convert(row.get("緊急連絡先マンションなど（滞納）", ""))
                if town2:
                    emergency_addr3_parts.append(town2)
                if mansion2:
                    emergency_addr3_parts.append(mansion2)
                converted_row["緊急連絡人１現住所3"] = "　".join(emergency_addr3_parts) if emergency_addr3_parts else ""

                # 緊急連絡先電話
                emergency_phone = self.process_phone_numbers(
                    row.get("緊急連絡先電話番号（滞納）", ""),
                    row.get("緊急連絡先携帯電話電話号（滞納）", "")
                )
                converted_row["緊急連絡人１TEL自宅"] = emergency_phone["home"]
                converted_row["緊急連絡人１TEL携帯"] = emergency_phone["mobile"]

            # 9. 固定値設定
            for key, value in IOGConfig.FIXED_VALUES.items():
                if key not in converted_row or converted_row[key] == "":
                    converted_row[key] = value

            # 10. 登録フラグ設定（譲渡一覧からのソース情報）
            if has_transfer:
                converted_row["登録フラグ"] = self.safe_str_convert(row.get("_source_info", ""))
            else:
                converted_row["登録フラグ"] = ""

            # 11. 全項目を一括正規化（英数字→半角、カタカナ→全角）
            for key in converted_row:
                converted_row[key] = self.normalize_for_client_system(converted_row[key])

            output_data.append(converted_row)

        # 111列の正確な順序でDataFrame構築（空列対応）
        temp_columns = []
        temp_data = {}
        empty_col_counter = 1

        for i, col in enumerate(IOGConfig.OUTPUT_COLUMNS):
            if col == "":  # 空列の場合
                temp_col_name = f"__EMPTY_COL_{empty_col_counter}__"
                temp_columns.append(temp_col_name)
                temp_data[temp_col_name] = [""] * len(output_data)
                empty_col_counter += 1
            else:
                temp_columns.append(col)
                temp_data[col] = [row.get(col, "") for row in output_data]

        # DataFrameを一度に構築
        final_df = pd.DataFrame(temp_data)

        # 列順を正しく設定し、空列の仮名前を元の空文字列に戻す
        final_df = final_df.reindex(columns=temp_columns)
        final_df.columns = IOGConfig.OUTPUT_COLUMNS

        return final_df, logs


def load_transfer_files(transfer_files: List[Tuple[str, bytes]]) -> pd.DataFrame:
    """
    譲渡一覧ファイル（複数）を読み込んで結合（3シート対応）

    Args:
        transfer_files: (ファイル名, バイトデータ) のタプルリスト

    Returns:
        pd.DataFrame: 結合された譲渡一覧データ（_source_info列付き）
    """
    if not transfer_files:
        return pd.DataFrame()

    all_transfer_data = []
    sheet_names = ["譲渡許可", "譲渡－東", "譲渡－西"]

    for file_name, file_content in transfer_files:
        for sheet_name in sheet_names:
            try:
                # header=1で読み込み（1行目がヘッダー）
                df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, dtype=str, header=1)

                # ソース情報列を追加
                df["_source_info"] = f"{file_name} - {sheet_name}"

                all_transfer_data.append(df)
            except Exception as e:
                logging.getLogger(__name__).warning(f"譲渡一覧シート読み込みエラー（スキップ）: {file_name} - {sheet_name}: {e}")
                continue

    if not all_transfer_data:
        return pd.DataFrame()

    # 全ファイル・全シートを結合
    combined_df = pd.concat(all_transfer_data, ignore_index=True)

    # 重複削除（同じ人が複数月に出現する場合、最初の1件のみ）
    if "賃借人氏名" in combined_df.columns:
        combined_df = combined_df.drop_duplicates(subset=["賃借人氏名"], keep="first")

    return combined_df


def normalize_name(name: str) -> str:
    """
    氏名を正規化（スペース統一）

    Args:
        name: 氏名文字列

    Returns:
        str: 正規化された氏名
    """
    if pd.isna(name) or not name:
        return ""

    # 全角スペースを半角スペースに
    normalized = str(name).replace("　", " ")

    # 連続スペースを1つに
    import re
    normalized = re.sub(r'\s+', ' ', normalized)

    # 前後の空白削除
    normalized = normalized.strip()

    return normalized


def find_matching_transfer(iog_name: str, transfer_df: pd.DataFrame) -> pd.Series:
    """
    部分一致で譲渡一覧を検索（最小3文字制限）

    Args:
        iog_name: IOGデータの氏名
        transfer_df: 譲渡一覧データ

    Returns:
        pd.Series: マッチした譲渡一覧の行（マッチしない場合はNone）

    Examples:
        IOG: "佐藤絵理福岡絵理谷田絵理"
        譲渡: "谷田絵理"
        → マッチ（"谷田絵理" in "佐藤絵理福岡絵理谷田絵理"）
    """
    if not iog_name or transfer_df.empty:
        return None

    iog_normalized = normalize_name(iog_name)

    for idx, row in transfer_df.iterrows():
        transfer_name = normalize_name(row.get("賃借人氏名", ""))

        # 3文字以上のフルネームのみマッチング対象（誤マッチ防止）
        if len(transfer_name) >= 3 and transfer_name in iog_normalized:
            return row

    return None


def merge_transfer_data(jid_df: pd.DataFrame, transfer_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, int]]:
    """
    JIDデータと譲渡一覧を氏名でマージ（2段階マッチング）

    第1段階: 完全一致でマージ
    第2段階: マッチしなかった行について部分一致でマージ

    Args:
        jid_df: JIDデータ
        transfer_df: 譲渡一覧データ

    Returns:
        Tuple[pd.DataFrame, Dict[str, int], Dict[str, int]]:
            (マージされたデータ, 同姓同名辞書, マッチング統計)
    """
    if transfer_df.empty:
        return jid_df, {}, {"exact": 0, "partial": 0}

    # 氏名を正規化
    jid_df = jid_df.copy()
    jid_df["_normalized_name"] = jid_df["対象者名"].apply(normalize_name)

    transfer_df = transfer_df.copy()
    transfer_df["_normalized_name"] = transfer_df["賃借人氏名"].apply(normalize_name)

    # 譲渡一覧で同姓同名を検出
    name_counts = transfer_df["_normalized_name"].value_counts()
    duplicates = name_counts[name_counts > 1].to_dict()

    # 第1段階: 完全一致でマージ
    merged_df = jid_df.merge(
        transfer_df,
        on="_normalized_name",
        how="left",
        suffixes=("", "_transfer")
    )

    # 完全一致のカウント
    exact_match_count = merged_df["物件名"].notna().sum() if "物件名" in merged_df.columns else 0
    partial_match_count = 0

    # 第2段階: 部分一致（マッチしなかった行のみ）
    unmatched_mask = merged_df["物件名"].isna() if "物件名" in merged_df.columns else pd.Series([True] * len(merged_df))

    for idx in merged_df[unmatched_mask].index:
        iog_name = merged_df.loc[idx, "対象者名"]
        matched_row = find_matching_transfer(iog_name, transfer_df)

        if matched_row is not None:
            # マッチした譲渡一覧のデータをマージ
            for col in transfer_df.columns:
                if col != "_normalized_name":
                    merged_df.loc[idx, col] = matched_row[col]
            partial_match_count += 1

    # 正規化列を削除
    merged_df = merged_df.drop("_normalized_name", axis=1)

    match_stats = {
        "exact": exact_match_count,
        "partial": partial_match_count
    }

    return merged_df, duplicates, match_stats


def process_jid_data(excel_content: bytes, transfer_files: List[Tuple[str, bytes]] = None) -> Tuple[pd.DataFrame, List[str], str]:
    """
    IOG新規登録データ処理メイン関数（譲渡一覧結合対応）

    Args:
        excel_content: IOG Excelファイルの内容
        transfer_files: (ファイル名, バイトデータ) のタプルリスト（任意）

    Returns:
        tuple: (変換済みDF, 処理ログ, 出力ファイル名)
    """
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        logs = []
        start_time = datetime.now()

        logs.append("=== IOG新規登録処理開始 ===")
        logger.info("IOG新規登録処理開始")

        # 1. IOG Excelファイル読み込み
        iog_df = pd.read_excel(io.BytesIO(excel_content), dtype=str)

        logs.append(f"IOGファイル読み込み完了: {len(iog_df)}件")
        logs.append(DetailedLogger.log_initial_load(len(iog_df)))

        # 2. 譲渡一覧ファイル読み込み＆結合
        transfer_df = pd.DataFrame()
        if transfer_files:
            transfer_df = load_transfer_files(transfer_files)
            if not transfer_df.empty:
                logs.append(f"譲渡一覧読み込み完了: {len(transfer_df)}件（{len(transfer_files)}ファイル結合後）")
            else:
                logs.append("譲渡一覧データなし（IOGデータのみで処理）")
        else:
            logs.append("譲渡一覧なし（IOGデータのみで処理）")

        # 3. データマージ（氏名マッチング：2段階）
        merged_df, duplicates, match_stats = merge_transfer_data(iog_df, transfer_df)

        if not transfer_df.empty:
            total_match = match_stats["exact"] + match_stats["partial"]
            logs.append(f"氏名マッチング結果: {total_match}件マッチ（{len(iog_df) - total_match}件マッチなし）")
            if match_stats["exact"] > 0:
                logs.append(f"  └─ 完全一致: {match_stats['exact']}件")
            if match_stats["partial"] > 0:
                logs.append(f"  └─ 部分一致: {match_stats['partial']}件（繰り返し氏名対応）")

            # 同姓同名の警告
            if duplicates:
                logs.append("")
                logs.append(f"⚠️ 同姓同名検出: {len(duplicates)}件")
                for name, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True):
                    logs.append(f"  • {name}（{count}件マッチ → 最初の1件を使用）")

        # 4. データ変換（111列テンプレート準拠）
        data_converter = DataConverter()
        output_df, conversion_logs = data_converter.convert_jid_data_with_transfer(merged_df, not transfer_df.empty)

        # 変換ログを追加
        logs.extend(conversion_logs)

        logs.append(f"データ変換完了: {len(output_df)}件 → 111列テンプレート形式")
        logs.append(DetailedLogger.log_final_result(len(output_df)))

        # 5. マッピング可視化情報
        logs.append("")
        logs.append("=== データマッピング状況 ===")
        logs.append("【IOGデータから（9項目）】")
        logs.append("  ✓ 引継番号 ← 契約番号")
        logs.append("  ✓ 契約者氏名 ← 対象者名")
        logs.append("  ✓ 契約者カナ ← フリガナ")
        logs.append("  ✓ 契約者TEL自宅 ← 自宅電話")
        logs.append("  ✓ 契約者TEL携帯 ← 携帯")
        logs.append("  ✓ 契約者現住所郵便番号 ← 郵便番号")
        logs.append("  ✓ 契約者現住所1-3 ← 自宅（住所分割）")
        logs.append("  ✓ 管理前滞納額 ← 差引残高")
        logs.append("  ✓ 管理受託日 ← 受任日")

        if not transfer_df.empty:
            logs.append("")
            logs.append("【譲渡一覧から（追加項目）】")
            logs.append("  ✓ 物件名・部屋番号（自動分割）")
            logs.append("  ✓ 物件住所（郵便番号、都道府県、市区町村、町域名）")
            logs.append("  ✓ 保証人1（氏名、続柄、住所、電話、携帯）")
            logs.append("  ✓ 緊急連絡先1（氏名、続柄、住所、電話、携帯）")

        logs.append("")
        logs.append("【空の列】")
        logs.append("  - 金額情報（賃料、管理費等）→ 0または空白")
        logs.append("  - 回収口座情報 → 固定値")
        logs.append("  - 保証人2、緊急連絡先2 → 空白")
        logs.append("  - その他 → 固定値または空白")

        # 6. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{IOGConfig.OUTPUT_FILE_PREFIX}.csv"

        # 7. 処理完了
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        logs.append("")
        logs.append(f"=== 処理完了: {output_filename} ({processing_time:.2f}秒) ===")
        logger.info(f"IOG処理完了: {len(output_df)}件出力")

        return output_df, logs, output_filename

    except Exception as e:
        error_msg = f"IOG処理エラー: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame(), [error_msg], "error.csv"
