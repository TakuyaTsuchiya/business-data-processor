#!/usr/bin/env python3
"""
ガレージバンク新規登録プロセッサ

ガレージバンクから送られてくるExcelデータを、自社システムのContractListと突合し、
新規データのみを111列フォーマットで出力する。

入力:
    - iraidata_xxxx.xlsx: ガレージバンクからのデータ（zokuseiシートのみ使用）
    - ContractList_xxxx.csv: 自社システムの既存データ（重複チェック用）

出力:
    - MMDDガレージバンク新規登録.csv: 111列フォーマット
"""

import pandas as pd
import io
import re
from datetime import datetime
from typing import Tuple, List, Optional
from .common.address_splitter import AddressSplitter


class GBConfig:
    """ガレージバンク新規登録の設定"""

    # Excel読み込み設定
    EXCEL_SKIPROWS = 1  # zokuseiシートの1行目はヘッダー
    EXCEL_SHEET_NAME = "zokusei"

    # 出力列（111列）
    OUTPUT_COLUMNS = [
        "引継番号", "契約者氏名", "契約者カナ", "契約者生年月日",
        "契約者TEL自宅", "契約者TEL携帯", "契約者現住所郵便番号",
        "契約者現住所1", "契約者現住所2", "契約者現住所3",
        "引継情報", "物件名", "部屋番号", "物件住所郵便番号",
        "物件住所1", "物件住所2", "物件住所3",
        "入居ステータス", "滞納ステータス", "受託状況",
        "月額賃料", "管理費", "共益費", "水道代", "駐車場代",
        "その他費用1", "その他費用2", "敷金", "礼金",
        "回収口座金融機関CD", "回収口座金融機関名", "回収口座支店CD",
        "回収口座支店名", "回収口座種類", "回収口座番号", "回収口座名義",
        "契約種類", "管理受託日", "契約確認日",
        "退去済手数料", "入居中滞納手数料", "入居中正常手数料",
        "管理前滞納額", "更新契約手数料", "退去手続き（実費）",
        "初回振替月", "保証開始日", "クライアントCD", "パートナーCD",
        "契約者勤務先名", "契約者勤務先カナ", "契約者勤務先TEL",
        "勤務先業種", "契約者勤務先郵便番号", "契約者勤務先住所1",
        "契約者勤務先住所2", "契約者勤務先住所3",
        "保証人１氏名", "保証人１カナ", "保証人１契約者との関係",
        "保証人１生年月日", "保証人１郵便番号", "保証人１住所1",
        "保証人１住所2", "保証人１住所3", "保証人１TEL自宅", "保証人１TEL携帯",
        "保証人２氏名", "保証人２カナ", "保証人２契約者との関係",
        "保証人２生年月日", "保証人２郵便番号", "保証人２住所1",
        "保証人２住所2", "保証人２住所3", "保証人２TEL自宅", "保証人２TEL携帯",
        "緊急連絡人１氏名", "緊急連絡人１カナ", "緊急連絡人１契約者との関係",
        "緊急連絡人１郵便番号", "緊急連絡人１現住所1", "緊急連絡人１現住所2",
        "緊急連絡人１現住所3", "緊急連絡人１TEL自宅", "緊急連絡人１TEL携帯",
        "緊急連絡人２氏名", "緊急連絡人２カナ", "緊急連絡人２契約者との関係",
        "緊急連絡人２郵便番号", "緊急連絡人２現住所1", "緊急連絡人２現住所2",
        "緊急連絡人２現住所3", "緊急連絡人２TEL自宅", "緊急連絡人２TEL携帯",
        "保証入金日", "保証入金者", "引落銀行CD", "引落銀行名",
        "引落支店CD", "引落支店名", "引落預金種別", "引落口座番号",
        "引落口座名義", "解約日", "管理会社", "委託先法人ID",
        "", "", "", "登録フラグ"
    ]

    # 固定値
    FIXED_VALUES = {
        # クライアント・法人ID
        "クライアントCD": "9288",
        "委託先法人ID": "7",

        # 回収口座（金融機関固定）
        "回収口座金融機関CD": "310",
        "回収口座金融機関名": "GMOあおぞらネット銀行",

        # 物件情報（固定）
        "物件名": "カシャリ",
        "物件住所郵便番号": "000-0000",
        "物件住所1": "東京都",
        "物件住所2": "リース",
        "物件住所3": "債権",

        # ステータス固定値
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "契約種類": "バックレント",

        # 手数料固定値
        "退去済手数料": "20",
        "入居中滞納手数料": "20",
        "入居中正常手数料": "20",

        # その他固定値
        "月額賃料": "0",
    }


# =============================================================================
# フォーマット関数
# =============================================================================

def format_zipcode(zipcode: str) -> str:
    """
    郵便番号をフォーマットする
    7桁の場合: 1000001 → 100-0001
    """
    if pd.isna(zipcode) or str(zipcode).strip() == "":
        return ""

    zipcode = str(zipcode).strip()

    # 既にハイフンがある場合はそのまま返す
    if "-" in zipcode:
        return zipcode

    # 数字のみ抽出
    digits = ''.join(filter(str.isdigit, zipcode))

    # 7桁の場合のみフォーマット
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"

    return zipcode


def format_phone(phone: str) -> str:
    """
    電話番号をフォーマットする
    - 11桁携帯: 09012345678 → 090-1234-5678
    - 先頭0欠損対応: 9012345678 → 090-1234-5678
    """
    if pd.isna(phone) or str(phone).strip() == "":
        return ""

    phone = str(phone).strip()

    # 既にハイフンがある場合はそのまま返す
    if "-" in phone:
        return phone

    # 数字のみ抽出
    digits = ''.join(filter(str.isdigit, phone))

    # 先頭0欠損対応（10桁で7/8/9始まり → 携帯番号の先頭0補完）
    if len(digits) == 10 and digits[0] in {'7', '8', '9'}:
        digits = '0' + digits

    # 11桁（携帯電話）
    if len(digits) == 11:
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"

    # 10桁（固定電話）
    if len(digits) == 10:
        # フリーダイヤル・ナビダイヤル
        if digits[:4] in {'0120', '0570', '0800'}:
            return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
        # 東京/大阪（2桁市外局番）
        if digits[:2] in {'03', '06'}:
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
        # その他（3桁市外局番）
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

    return phone


def format_date(date_str: str) -> str:
    """
    日付をフォーマットする
    YYYY-MM-DD → YYYY/M/D
    """
    if pd.isna(date_str) or str(date_str).strip() == "":
        return ""

    date_str = str(date_str).strip()

    # 既に / 区切りの場合はそのまま返す
    if "/" in date_str:
        return date_str

    # YYYY-MM-DD 形式の場合
    if "-" in date_str:
        try:
            parts = date_str.split("-")
            if len(parts) == 3:
                year = parts[0]
                month = str(int(parts[1]))  # 先頭ゼロ除去
                day = str(int(parts[2].split()[0]))  # 時刻部分があれば除去
                return f"{year}/{month}/{day}"
        except (ValueError, IndexError):
            pass

    return date_str


def remove_spaces(text: str) -> str:
    """
    スペースを除去する（全角・半角両方）
    """
    if pd.isna(text) or str(text).strip() == "":
        return ""

    text = str(text)
    # 半角スペースと全角スペースを除去
    return text.replace(" ", "").replace("\u3000", "")


def format_account_type(account_type: str) -> str:
    """
    口座種別をフォーマットする
    普通預金 → 普通
    当座預金 → 当座
    """
    if pd.isna(account_type) or str(account_type).strip() == "":
        return ""

    account_type = str(account_type).strip()

    if "普通" in account_type:
        return "普通"
    if "当座" in account_type:
        return "当座"

    return account_type


# =============================================================================
# 住所分割関数（AddressSplitter使用）
# =============================================================================

# AddressSplitterインスタンス（辞書ベースの市区町村判定）
_address_splitter = AddressSplitter()


def split_address(address: str, prefecture: str = "") -> Tuple[str, str]:
    """
    住所を市区町村と残りに分割する（辞書ベース）

    Args:
        address: 住所文字列（市区町村以降）
        prefecture: 都道府県（指定がない場合は全都道府県で検索）

    Returns:
        Tuple[str, str]: (市区町村, 残りの住所)
    """
    if pd.isna(address) or str(address).strip() == "":
        return "", ""

    address = str(address).strip()

    # 都道府県が指定されている場合
    if prefecture:
        city, rest = _address_splitter.extract_municipality(prefecture, address)
        if city:
            return city, rest

    # 都道府県が指定されていない場合、全都道府県で検索
    for pref in _address_splitter.prefectures:
        city, rest = _address_splitter.extract_municipality(pref, address)
        if city:
            return city, rest

    # 分割できない場合は全体を市区町村として扱う
    return address, ""


def combine_address(address_rest: str, building: str) -> str:
    """
    住所の残り部分と建物名を結合する
    結合には全角スペースを使用
    """
    if pd.isna(address_rest):
        address_rest = ""
    else:
        address_rest = str(address_rest).strip()

    if pd.isna(building) or str(building).strip() == "":
        return address_rest

    building = str(building).strip()
    return f"{address_rest}\u3000{building}"


# =============================================================================
# ファイル読み込みクラス
# =============================================================================

class FileReader:
    """ファイル読み込みクラス"""

    def read_excel_file(self, content: bytes, skiprows: int = 1,
                        sheet_name: str = "zokusei") -> pd.DataFrame:
        """
        Excelファイルを読み込む

        Args:
            content: ファイルのバイナリデータ
            skiprows: スキップする行数
            sheet_name: シート名

        Returns:
            pd.DataFrame: 読み込んだデータ
        """
        try:
            df = pd.read_excel(
                io.BytesIO(content),
                sheet_name=sheet_name,
                skiprows=skiprows,
                dtype=str,
                engine='openpyxl'
            )
            return df
        except Exception as e:
            raise ValueError(f"Excelファイルの読み込みに失敗しました: {e}")

    def read_csv_file(self, content: bytes) -> pd.DataFrame:
        """
        CSVファイルを読み込む（エンコーディング自動判定）

        Args:
            content: ファイルのバイナリデータ

        Returns:
            pd.DataFrame: 読み込んだデータ
        """
        encodings = ['cp932', 'shift_jis', 'utf-8', 'utf-8-sig']

        for enc in encodings:
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, dtype=str)
                return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue

        raise ValueError("CSVファイルの読み込みに失敗しました: エンコーディングを判定できません")


# =============================================================================
# 重複チェッククラス
# =============================================================================

class DuplicateChecker:
    """重複チェッククラス"""

    def check_duplicates(
        self,
        excel_df: pd.DataFrame,
        contract_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        重複チェックを行う

        Args:
            excel_df: Excelから読み込んだデータ
            contract_df: ContractListのデータ

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, dict]:
                - 新規データ
                - 既存データ
                - 統計情報
        """
        # 必須列の確認
        if "ユーザーID（社内管理用）" not in excel_df.columns:
            raise ValueError("Excel入力データに「ユーザーID（社内管理用）」列が存在しません")
        if "引継番号" not in contract_df.columns:
            raise ValueError("ContractListに「引継番号」列が存在しません")

        # ユーザーIDを文字列に変換（float対応）
        excel_ids = excel_df["ユーザーID（社内管理用）"].astype(str).str.replace('.0', '', regex=False)
        contract_ids = set(contract_df["引継番号"].astype(str).tolist())

        # 新規・既存の判定
        new_mask = ~excel_ids.isin(contract_ids)

        new_data = excel_df[new_mask].copy()
        existing_data = excel_df[~new_mask].copy()

        # 統計情報
        total = len(excel_df)
        new_count = len(new_data)
        existing_count = len(existing_data)

        stats = {
            "total_records": total,
            "new_records": new_count,
            "existing_records": existing_count,
            "new_percentage": round(new_count / total * 100, 1) if total > 0 else 0
        }

        return new_data, existing_data, stats


# =============================================================================
# データマッピングクラス
# =============================================================================

class DataMapper:
    """データマッピングクラス"""

    def __init__(self, config: GBConfig):
        self.config = config

    def create_output_dataframe(self, excel_df: pd.DataFrame) -> pd.DataFrame:
        """
        111列の出力DataFrameを作成する

        Args:
            excel_df: Excelから読み込んだデータ

        Returns:
            pd.DataFrame: 111列の空のDataFrame
        """
        # 空列対応（仮名前方式）
        temp_columns = []
        empty_counter = 0
        for col in GBConfig.OUTPUT_COLUMNS:
            if col == "":
                temp_columns.append(f"__EMPTY_COL_{empty_counter}__")
                empty_counter += 1
            else:
                temp_columns.append(col)

        # 空のDataFrameを作成
        output_df = pd.DataFrame(
            "",
            index=range(len(excel_df)),
            columns=temp_columns
        )

        # 列名を元に戻す
        output_df.columns = GBConfig.OUTPUT_COLUMNS

        return output_df

    def map_contractor_info(self, output_df: pd.DataFrame, excel_df: pd.DataFrame) -> None:
        """契約者情報をマッピングする"""
        # 引継番号
        if "ユーザーID（社内管理用）" in excel_df.columns:
            output_df["引継番号"] = excel_df["ユーザーID（社内管理用）"].astype(str).str.replace('.0', '', regex=False)

        # 契約者氏名（スペース除去）
        if "ユーザー名" in excel_df.columns:
            output_df["契約者氏名"] = excel_df["ユーザー名"].apply(remove_spaces)

        # 契約者カナ（スペース除去）
        if "カナ" in excel_df.columns:
            output_df["契約者カナ"] = excel_df["カナ"].apply(remove_spaces)

        # 契約者生年月日（日付フォーマット）
        if "生年月日" in excel_df.columns:
            output_df["契約者生年月日"] = excel_df["生年月日"].apply(format_date)

        # 契約者TEL携帯（電話フォーマット）
        if "電話番号" in excel_df.columns:
            output_df["契約者TEL携帯"] = excel_df["電話番号"].apply(format_phone)

        # 契約者現住所郵便番号（郵便番号フォーマット）
        if "郵便番号" in excel_df.columns:
            output_df["契約者現住所郵便番号"] = excel_df["郵便番号"].apply(format_zipcode)

        # 契約者現住所1（都道府県）
        if "住所_都道府県" in excel_df.columns:
            output_df["契約者現住所1"] = excel_df["住所_都道府県"]

        # 契約者現住所2, 3（住所分割）
        if "住所_1" in excel_df.columns:
            # 住所_1を市区町村と残りに分割
            split_results = excel_df["住所_1"].apply(split_address)
            output_df["契約者現住所2"] = split_results.apply(lambda x: x[0])

            # 残りの住所と住所_2（建物名）を結合
            address_rest = split_results.apply(lambda x: x[1])

            if "住所_2" in excel_df.columns:
                output_df["契約者現住所3"] = [
                    combine_address(rest, building)
                    for rest, building in zip(address_rest, excel_df["住所_2"])
                ]
            else:
                output_df["契約者現住所3"] = address_rest

    def map_hikitsugi_info(self, output_df: pd.DataFrame, excel_df: pd.DataFrame) -> None:
        """引継情報をマッピングする"""
        today = datetime.now().strftime("%Y/%m/%d")

        if "メールアドレス" in excel_df.columns:
            output_df["引継情報"] = excel_df["メールアドレス"].apply(
                lambda email: f"{today}\u3000ガレージバンク一括登録\u3000●メールアドレス：{email if pd.notna(email) else ''}"
            )
        else:
            output_df["引継情報"] = f"{today}\u3000ガレージバンク一括登録"

    def map_financial_info(self, output_df: pd.DataFrame, excel_df: pd.DataFrame) -> None:
        """金融情報をマッピングする"""
        # 管理前滞納額
        if "請求金額" in excel_df.columns:
            output_df["管理前滞納額"] = excel_df["請求金額"]

        # 回収口座支店名
        if "振込先支店名" in excel_df.columns:
            output_df["回収口座支店名"] = excel_df["振込先支店名"]

        # 回収口座種類（「普通預金」→「普通」）
        if "振込先口座種別" in excel_df.columns:
            output_df["回収口座種類"] = excel_df["振込先口座種別"].apply(format_account_type)

        # 回収口座番号
        if "振込先口座番号" in excel_df.columns:
            output_df["回収口座番号"] = excel_df["振込先口座番号"]

        # 回収口座名義
        if "振込先口座名義人" in excel_df.columns:
            output_df["回収口座名義"] = excel_df["振込先口座名義人"]

    def apply_fixed_values(self, output_df: pd.DataFrame) -> None:
        """固定値を適用する"""
        # 管理受託日（処理実行日）
        today = datetime.now().strftime("%Y/%m/%d")
        output_df["管理受託日"] = today

        # 固定値を適用
        for col, value in GBConfig.FIXED_VALUES.items():
            if col in output_df.columns:
                output_df[col] = value


# =============================================================================
# メイン処理関数
# =============================================================================

def process_gb_data(
    excel_content: bytes,
    contract_content: bytes
) -> Tuple[pd.DataFrame, List[str], str]:
    """
    ガレージバンク新規登録データを処理する

    Args:
        excel_content: Excelファイルのバイナリデータ
        contract_content: ContractListのバイナリデータ

    Returns:
        Tuple[pd.DataFrame, List[str], str]:
            - 処理結果のDataFrame
            - ログメッセージのリスト
            - 出力ファイル名
    """
    logs = []
    config = GBConfig()
    file_reader = FileReader()
    duplicate_checker = DuplicateChecker()
    data_mapper = DataMapper(config)

    # 1. ファイル読み込み
    logs.append("ファイル読み込み開始...")

    excel_df = file_reader.read_excel_file(
        excel_content,
        skiprows=config.EXCEL_SKIPROWS,
        sheet_name=config.EXCEL_SHEET_NAME
    )
    logs.append(f"Excel読み込み完了: {len(excel_df)}件")

    contract_df = file_reader.read_csv_file(contract_content)
    logs.append(f"ContractList読み込み完了: {len(contract_df)}件")

    # 2. 重複チェック
    logs.append("重複チェック開始...")

    new_data, existing_data, stats = duplicate_checker.check_duplicates(
        excel_df, contract_df
    )

    logs.append(f"重複チェック完了:")
    logs.append(f"  - 入力データ: {stats['total_records']}件")
    logs.append(f"  - 新規: {stats['new_records']}件 ({stats['new_percentage']}%)")
    logs.append(f"  - 既存: {stats['existing_records']}件")

    # 3. 新規データがない場合は空のDataFrameを返す
    if len(new_data) == 0:
        logs.append("新規データはありません")
        empty_df = pd.DataFrame(columns=config.OUTPUT_COLUMNS)
        timestamp = datetime.now().strftime("%m%d")
        filename = f"{timestamp}ガレージバンク新規登録.csv"
        return empty_df, logs, filename

    # 4. データマッピング
    logs.append("データマッピング開始...")

    output_df = data_mapper.create_output_dataframe(new_data)
    data_mapper.map_contractor_info(output_df, new_data)
    data_mapper.map_hikitsugi_info(output_df, new_data)
    data_mapper.map_financial_info(output_df, new_data)
    data_mapper.apply_fixed_values(output_df)

    logs.append(f"データマッピング完了: {len(output_df)}件")

    # 5. 出力ファイル名生成
    timestamp = datetime.now().strftime("%m%d")
    filename = f"{timestamp}ガレージバンク新規登録.csv"

    logs.append(f"処理完了: {filename}")

    return output_df, logs, filename
