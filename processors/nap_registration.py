#!/usr/bin/env python3
"""
ナップ新規登録プロセッサ

入力ファイル:
- ミライル様Excelファイル (skiprows=13で51列)
- ContractList_*.csv (122列)

出力ファイル:
- MMDDナップ新規登録.csv (111列) - 絶対変更禁止テンプレート準拠

重複チェック:
- Excel「承認番号」⟷ ContractList「引継番号」
- ContractListは委託先法人ID=5のみ対象

主要マッピングルール:
【基本情報】
- 引継番号: 承認番号の下6桁を抽出
- 引継情報: "●入居日" + 日割家賃発生日 (例: ●入居日2024/3/15)
- 契約者生年月日: 契約者生年月日列から直接マッピング
- 契約者現住所郵便番号: 契約者郵便番号
- 契約者現住所3: 契約者１住所３ + 契約者住所アパート等（結合）
- 物件住所郵便番号: 物件郵便番号
- 物件住所3: 物件住所３ + 物件名 + 部屋番号（結合）
- 契約者カナ/保証人１カナ/緊急連絡人１カナ: ひらがな→カタカナ自動変換
- 契約者TEL携帯: 契約者携帯1を優先、空白時は契約者電話を使用
- 契約者TEL自宅: 契約者携帯1がある場合のみ契約者電話をマッピング（携帯1が空白で電話を携帯に使用した場合は空白）
- 保証人１契約者との関係: 保証人１氏名に値がある場合は「他」に設定
- 緊急連絡人１契約者との関係: 緊急連絡人氏名に値がある場合は「他」に設定

【賃料・費用】
- 月額賃料: 賃料
- 退去手続き（実費）: 賃料
- 共益費: 0 (固定)
- その他費用2: 0 (固定)

【口座情報】
- 回収口座金融機関CD: 1 (固定)
- 回収口座金融機関名: みずほ銀行 (固定)
- 回収口座支店CD: 797 (固定)
- 回収口座支店名: 第五集中支店 (固定)
- 回収口座種類: 普通 (固定)
- 回収口座番号: バーチャル口座: 名称
- 回収口座名義: ナップ賃貸保証株式会社 (固定)

【その他固定値】
- クライアントCD: 9268
- 委託先法人ID: 5
- 契約種類: レントワン
- 管理前滞納額: 1
"""

import pandas as pd
import io
import chardet
from datetime import datetime
from typing import Tuple, List, Dict, Union
import logging
import time
import requests
import re


def format_zipcode(zipcode: str) -> str:
    """
    郵便番号にハイフンを挿入（7桁の場合のみ）

    Args:
        zipcode: 郵便番号文字列

    Returns:
        フォーマット済み郵便番号（7桁の場合はXXX-XXXX形式）

    Examples:
        >>> format_zipcode("1000001")
        "100-0001"
        >>> format_zipcode("100-0001")
        "100-0001"
        >>> format_zipcode("")
        ""
    """
    # pd.isna()を先にチェック（pd.NAのambiguousエラー回避）
    if pd.isna(zipcode) or not zipcode:
        return ""

    # 数字のみを抽出
    digits = ''.join(filter(str.isdigit, str(zipcode)))

    # 7桁の場合のみフォーマット
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"

    # それ以外はそのまま返す
    return str(zipcode)


def format_phone(phone: str) -> str:
    """
    電話番号にハイフンを挿入

    Args:
        phone: 電話番号文字列

    Returns:
        フォーマット済み電話番号

    Examples:
        >>> format_phone("09012345678")
        "090-1234-5678"
        >>> format_phone("0312345678")
        "03-1234-5678"
        >>> format_phone("0120123456")
        "0120-123-456"
        >>> format_phone("0421112222")
        "042-111-2222"
        >>> format_phone("9037978313")  # 先頭0欠損（Excel数値化）
        "090-3797-8313"
    """
    # pd.isna()を先にチェック（pd.NAのambiguousエラー回避）
    if pd.isna(phone) or not phone:
        return ""

    # 数字のみを抽出
    digits = ''.join(filter(str.isdigit, str(phone)))

    # 先頭の0が欠けている携帯番号を補完（Excelの数値型変換による欠損対策）
    # 10桁で7/8/9始まり → 携帯番号（070/080/090）の可能性が高い
    if len(digits) == 10 and digits[0] in ['7', '8', '9']:
        digits = '0' + digits

    if len(digits) == 11:
        # 携帯電話・IP電話: 3-4-4形式
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    elif len(digits) == 10:
        # 2桁市外局番（東京23区・大阪のみ）
        if digits[:2] in ['03', '06']:
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
        # 0120, 0800などの特番
        elif digits[:4] in ['0120', '0800']:
            return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
        # それ以外は3桁市外局番: 3-3-4形式
        else:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

    # それ以外はそのまま返す
    return str(phone)


def lookup_zipcode_from_address(address1: str, address2: str, address3: str = "") -> str:
    """
    住所から郵便番号を検索（zipcloud API使用）

    Args:
        address1: 住所1（都道府県・市区町村）
        address2: 住所2（町名・番地）
        address3: 住所3（建物名など、オプション）

    Returns:
        郵便番号（7桁、ハイフンなし）。取得失敗時は空文字列

    Examples:
        >>> lookup_zipcode_from_address("東京都", "千代田区丸の内1-1")
        "1000001"
        >>> lookup_zipcode_from_address("", "", "")
        ""

    Note:
        - zipcloud API (https://zipcloud.ibsnet.co.jp/api/search) を使用
        - API失敗時は空文字列を返す（エラーにしない）
        - タイムアウト: 5秒
    """
    logger = logging.getLogger(__name__)

    # pd.NA/NaN/Noneを空文字列に変換するヘルパー
    def safe_str(val):
        if pd.isna(val):
            return ""
        return str(val).strip()

    # 住所をクリーニング
    addr1_clean = safe_str(address1)
    addr2_clean = safe_str(address2)

    # 住所2から番地以降を削除して町名まで抽出（検索精度向上）
    # 数字+ハイフンで始まる部分以降を削除（番地・建物名除去）
    addr2_clean = re.sub(r'[0-9０-９]+[-－].*', '', addr2_clean).strip()
    # 半角カタカナを削除
    addr2_clean = re.sub(r'[ｦ-ﾟ]+', '', addr2_clean).strip()

    # 住所を結合（addr3は除外済み）
    full_address = addr1_clean + addr2_clean
    if not full_address:
        return ""

    try:
        response = requests.get(
            "https://zipcloud.ibsnet.co.jp/api/search",
            params={"address": full_address},
            timeout=5
        )

        if response.ok:
            data = response.json()
            if data.get("results") and len(data["results"]) > 0:
                zipcode = data["results"][0].get("zipcode", "")
                logger.info(f"郵便番号検索成功: {full_address[:20]}... → {zipcode}")
                return zipcode
            else:
                logger.warning(f"郵便番号検索結果なし: {full_address[:30]}...")
        else:
            logger.warning(f"郵便番号API HTTPエラー {response.status_code}: {full_address[:30]}...")

    except requests.Timeout:
        logger.warning(f"郵便番号API タイムアウト: {full_address[:30]}...")
    except Exception as e:
        logger.warning(f"郵便番号検索エラー: {full_address[:30]}... - {type(e).__name__}: {str(e)}")

    return ""


class NapConfig:
    """ナップ新規登録設定・定数管理クラス"""

    OUTPUT_FILE_PREFIX = "ナップ新規登録"

    # 【最重要】正確な111列テンプレートヘッダー（絶対変更禁止）
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
        "契約種類": "レントワン",

        # 回収口座関連
        "回収口座金融機関CD": "1",
        "回収口座金融機関名": "みずほ銀行",
        "回収口座種類": "普通",
        "回収口座名義": "ナップ賃貸保証株式会社",

        # その他固定値
        "クライアントCD": "9268",
        "委託先法人ID": "5",
        "退去済手数料": "0",
        "入居中滞納手数料": "0",
        "入居中正常手数料": "0",
        "管理前滞納額": "1",

        # 空白項目
        "契約確認日": "",
        "保証開始日": "",
        "パートナーCD": "",
        "初回振替月": "",
        "引落預金種別": "",
        "解約日": "",
        "保証入金日": "",
        "保証入金者": "",
        "登録フラグ": "",

        # 勤務先業種・郵便番号・住所（空白）
        "勤務先業種": "",
        "契約者勤務先郵便番号": "",
        "契約者勤務先住所1": "",
        "契約者勤務先住所2": "",
        "契約者勤務先住所3": "",
        "契約者勤務先カナ": "",

        # 費用関連（空白 or 0）
        "共益費": "0",
        "その他費用2": "0",
        "敷金": "",
        "礼金": "",
        "更新契約手数料": "",

        # 支店関連
        "回収口座支店CD": "797",
        "回収口座支店名": "第五集中支店",
        "引落銀行CD": "",
        "引落銀行名": "",
        "引落支店CD": "",
        "引落支店名": "",
        "引落口座番号": "",
        "引落口座名義": "",

        # 保証人２（空白）
        "保証人２氏名": "",
        "保証人２カナ": "",
        "保証人２契約者との関係": "",
        "保証人２生年月日": "",
        "保証人２郵便番号": "",
        "保証人２住所1": "",
        "保証人２住所2": "",
        "保証人２住所3": "",
        "保証人２TEL自宅": "",
        "保証人２TEL携帯": "",

        # 緊急連絡人２（空白）
        "緊急連絡人２氏名": "",
        "緊急連絡人２カナ": "",
        "緊急連絡人２契約者との関係": "",
        "緊急連絡人２郵便番号": "",
        "緊急連絡人２現住所1": "",
        "緊急連絡人２現住所2": "",
        "緊急連絡人２現住所3": "",
        "緊急連絡人２TEL自宅": "",
        "緊急連絡人２TEL携帯": "",
    }

    # 委託先法人IDフィルタ値
    TARGET_CORPORATION_ID = "5"

    # Excelファイルのskiprows設定
    EXCEL_SKIPROWS = 13


class FileReader:
    """ファイル読み込みクラス"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def read_excel_file(
        self,
        file_content: Union[bytes, io.BytesIO],
        skiprows: int = 13
    ) -> pd.DataFrame:
        """
        Excelファイルを読み込む

        Args:
            file_content: ファイルバイナリ
            skiprows: スキップする行数

        Returns:
            DataFrame

        Raises:
            ValueError: ファイル読み込み失敗
        """
        try:
            if hasattr(file_content, 'read'):
                content = file_content.read()
            else:
                content = file_content

            df = pd.read_excel(
                io.BytesIO(content),
                skiprows=skiprows,
                dtype=str,  # 全列を文字列として扱い数値の精度問題を回避
                engine='openpyxl',  # .xlsxファイル用のエンジンを明示的に指定
                engine_kwargs={'read_only': True, 'data_only': True}  # 高速化: ストリーミング読み込み
            )

            self.logger.info(f"Excel file loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            return df

        except Exception as e:
            raise ValueError(f"Excelファイルの読み込みに失敗しました: {str(e)}")

    def read_csv_file(
        self,
        file_content: Union[bytes, io.BytesIO]
    ) -> pd.DataFrame:
        """
        CSVファイルを読み込む（エンコーディング自動判定）

        Args:
            file_content: ファイルバイナリ

        Returns:
            DataFrame

        Raises:
            ValueError: ファイル読み込み失敗
        """
        if hasattr(file_content, 'read'):
            content = file_content.read()
        else:
            content = file_content

        # エンコーディング検出（最初の10KBのみで高速化）
        sample_size = min(10240, len(content))
        detected = chardet.detect(content[:sample_size])
        encoding = detected['encoding']

        # エンコーディング順リスト
        encodings = ['cp932', 'shift_jis', 'utf-8', 'utf-8-sig']

        if encoding and encoding.lower() not in [e.lower() for e in encodings]:
            encodings.insert(0, encoding)

        for enc in encodings:
            try:
                df = pd.read_csv(
                    io.BytesIO(content),
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False
                )

                self.logger.info(f"CSV file loaded with {enc}: {df.shape[0]} rows, {df.shape[1]} columns")
                return df

            except Exception:
                continue

        raise ValueError("CSVファイルの読み込みに失敗しました")


class DuplicateChecker:
    """重複チェッククラス"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def filter_contract_list(
        self,
        contract_df: pd.DataFrame,
        target_id: str = "5"
    ) -> pd.DataFrame:
        """
        ContractListを委託先法人IDでフィルタ

        Args:
            contract_df: ContractList DataFrame
            target_id: 対象の委託先法人ID

        Returns:
            フィルタ済みDataFrame
        """
        if "委託先法人ID" not in contract_df.columns:
            raise ValueError("ContractListに「委託先法人ID」列が存在しません")

        filtered_df = contract_df[contract_df["委託先法人ID"] == target_id].copy()
        self.logger.info(f"ContractList filtered: {len(filtered_df)} records (委託先法人ID={target_id})")

        return filtered_df

    def check_duplicates(
        self,
        excel_df: pd.DataFrame,
        contract_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict, List[str]]:
        """
        重複チェック実行

        Args:
            excel_df: Excel入力データ
            contract_df: ContractList（フィルタ済み）

        Returns:
            - new_data: 新規データ
            - existing_data: 既存データ
            - stats: 統計情報
            - logs: 処理ログ
        """
        logs = []

        # 承認番号列の確認
        approval_col = "承認番号"
        if approval_col not in excel_df.columns:
            raise ValueError(f"Excel入力データに「{approval_col}」列が存在しません")

        # 引継番号列の確認
        if "引継番号" not in contract_df.columns:
            raise ValueError("ContractListに「引継番号」列が存在しません")

        # 承認番号を文字列に変換（float対策）
        excel_df[approval_col] = excel_df[approval_col].astype(str).str.replace('.0', '', regex=False)
        contract_df["引継番号"] = contract_df["引継番号"].astype(str)

        # 既存の引継番号セット
        existing_ids = set(contract_df["引継番号"])

        # 新規・既存に分離
        new_mask = ~excel_df[approval_col].isin(existing_ids)
        new_data = excel_df[new_mask].copy()
        existing_data = excel_df[~new_mask].copy()

        # 統計情報
        stats = {
            "total": len(excel_df),
            "new_records": len(new_data),
            "existing_records": len(existing_data),
            "new_percentage": (len(new_data) / len(excel_df) * 100) if len(excel_df) > 0 else 0
        }

        logs.append(f"Excel総数: {stats['total']}件")
        logs.append(f"新規契約: {stats['new_records']}件 ({stats['new_percentage']:.1f}%)")
        logs.append(f"既存契約（重複）: {stats['existing_records']}件")

        self.logger.info(f"Duplicate check: {stats['new_records']} new, {stats['existing_records']} existing")

        return new_data, existing_data, stats, logs


class DataMapper:
    """データマッピングクラス"""

    def __init__(self, config: NapConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def create_output_dataframe(
        self,
        excel_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        111列の出力DataFrameを作成

        Args:
            excel_df: 入力DataFrame

        Returns:
            111列のDataFrame
        """
        # 111列の空DataFrameを作成
        output_df = pd.DataFrame("", index=range(len(excel_df)), columns=self.config.OUTPUT_COLUMNS)
        return output_df

    def map_contractor_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """
        契約者情報をマッピング（in-place）

        Args:
            output_df: 出力DataFrame（in-place更新）
            excel_df: 入力Excel DataFrame
        """
        # 引継番号（承認番号の下6桁）
        if "承認番号" in excel_df.columns:
            # 承認番号を文字列化し、.0を除去後、下6桁を抽出
            approval_numbers = excel_df["承認番号"].astype(str).str.replace('.0', '', regex=False)
            output_df["引継番号"] = approval_numbers.str[-6:]

        # 契約者氏名・カナ
        if "契約者氏名" in excel_df.columns:
            output_df["契約者氏名"] = excel_df["契約者氏名"]
        if "契約者氏名かな" in excel_df.columns:
            # ひらがなをカタカナに変換
            kana = excel_df["契約者氏名かな"].fillna("")
            output_df["契約者カナ"] = kana.str.translate(
                str.maketrans("ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゕゖ",
                             "ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶ")
            )

        # 契約者生年月日
        if "契約者生年月日" in excel_df.columns:
            output_df["契約者生年月日"] = excel_df["契約者生年月日"]

        # 電話番号
        # ルール: 携帯1がある → TEL携帯=携帯1、TEL自宅=電話
        #        携帯1が空白で電話がある → TEL携帯=電話、TEL自宅=空白（同じ番号を2箇所に入れない）
        if "契約者携帯1" in excel_df.columns and "契約者電話" in excel_df.columns:
            mobile = excel_df["契約者携帯1"].fillna("").astype(str).apply(format_phone)
            phone = excel_df["契約者電話"].fillna("").astype(str).apply(format_phone)
            # 携帯1を優先、空白なら電話を使用
            output_df["契約者TEL携帯"] = mobile.where(mobile != "", phone)
            # 携帯1がある行のみ、自宅に電話番号を入れる（携帯1が空白の行は自宅も空白）
            output_df["契約者TEL自宅"] = phone.where(mobile != "", "")
        elif "契約者携帯1" in excel_df.columns:
            output_df["契約者TEL携帯"] = excel_df["契約者携帯1"].apply(format_phone)
            if "契約者電話" in excel_df.columns:
                output_df["契約者TEL自宅"] = excel_df["契約者電話"].apply(format_phone)
        elif "契約者電話" in excel_df.columns:
            # 携帯1列がない場合は電話を携帯に使用、自宅は空白
            output_df["契約者TEL携帯"] = excel_df["契約者電話"].apply(format_phone)

        # 現住所
        if "契約者郵便番号" in excel_df.columns:
            logger = logging.getLogger(__name__)

            # 郵便番号が空白の場合、住所からルックアップを試みる
            def get_contractor_zipcode(row):
                row_idx = row.name
                zipcode = row.get("契約者郵便番号", "")

                if pd.isna(zipcode) or not str(zipcode).strip():
                    addr1 = row.get("契約者１住所１", "")
                    addr2 = row.get("契約者１住所２", "")
                    addr3 = row.get("契約者１住所３", "")

                    # pd.NAを安全に処理
                    def safe_val(val):
                        return "" if pd.isna(val) else str(val).strip()

                    addr1_safe = safe_val(addr1)
                    addr2_safe = safe_val(addr2)
                    addr3_safe = safe_val(addr3)

                    # 住所が全て空白なら何もしない
                    if not (addr1_safe or addr2_safe or addr3_safe):
                        return ""

                    # 住所があればAPI検索（建物名は除外して精度向上）
                    logger.info(f"行{row_idx} - 契約者現住所郵便番号を検索中: {addr1_safe}{addr2_safe}")
                    zipcode = lookup_zipcode_from_address(addr1, addr2, "")

                    if zipcode:
                        logger.info(f"行{row_idx} - 契約者現住所郵便番号を補完: {zipcode}")
                    else:
                        logger.warning(f"行{row_idx} - 契約者現住所郵便番号: 検索結果なし ({addr1_safe}{addr2_safe})")

                return format_zipcode(zipcode)

            output_df["契約者現住所郵便番号"] = excel_df.apply(get_contractor_zipcode, axis=1)
        if "契約者１住所１" in excel_df.columns:
            output_df["契約者現住所1"] = excel_df["契約者１住所１"]
        if "契約者１住所２" in excel_df.columns:
            output_df["契約者現住所2"] = excel_df["契約者１住所２"]

        # 契約者現住所3 = 契約者１住所３ + 契約者住所アパート等
        addr3 = excel_df["契約者１住所３"].fillna("") if "契約者１住所３" in excel_df.columns else ""
        apt = excel_df["契約者住所アパート等"].fillna("") if "契約者住所アパート等" in excel_df.columns else ""
        output_df["契約者現住所3"] = addr3 + apt

        # 勤務先情報
        if "契約者勤務先名" in excel_df.columns:
            output_df["契約者勤務先名"] = excel_df["契約者勤務先名"]
        if "契約者勤務先電話" in excel_df.columns:
            output_df["契約者勤務先TEL"] = excel_df["契約者勤務先電話"].apply(format_phone)

    def map_property_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """
        物件情報をマッピング（in-place）

        Args:
            output_df: 出力DataFrame（in-place更新）
            excel_df: 入力Excel DataFrame
        """
        # 物件名・部屋番号
        if "物件名" in excel_df.columns:
            output_df["物件名"] = excel_df["物件名"]
        if "部屋番号" in excel_df.columns:
            output_df["部屋番号"] = excel_df["部屋番号"]

        # 物件住所
        if "物件郵便番号" in excel_df.columns:
            logger = logging.getLogger(__name__)

            # 郵便番号が空白の場合、住所からルックアップを試みる
            def get_property_zipcode(row):
                row_idx = row.name
                zipcode = row.get("物件郵便番号", "")

                if pd.isna(zipcode) or not str(zipcode).strip():
                    addr1 = row.get("物件住所１", "")
                    addr2 = row.get("物件住所２", "")
                    addr3 = row.get("物件住所３", "")

                    # pd.NAを安全に処理
                    def safe_val(val):
                        return "" if pd.isna(val) else str(val).strip()

                    addr1_safe = safe_val(addr1)
                    addr2_safe = safe_val(addr2)
                    addr3_safe = safe_val(addr3)

                    # 住所が全て空白なら何もしない
                    if not (addr1_safe or addr2_safe or addr3_safe):
                        return ""

                    # 住所があればAPI検索（建物名は除外して精度向上）
                    logger.info(f"行{row_idx} - 物件住所郵便番号を検索中: {addr1_safe}{addr2_safe}")
                    zipcode = lookup_zipcode_from_address(addr1, addr2, "")

                    if zipcode:
                        logger.info(f"行{row_idx} - 物件住所郵便番号を補完: {zipcode}")
                    else:
                        logger.warning(f"行{row_idx} - 物件住所郵便番号: 検索結果なし ({addr1_safe}{addr2_safe})")

                return format_zipcode(zipcode)

            output_df["物件住所郵便番号"] = excel_df.apply(get_property_zipcode, axis=1)
        if "物件住所１" in excel_df.columns:
            output_df["物件住所1"] = excel_df["物件住所１"]
        if "物件住所２" in excel_df.columns:
            output_df["物件住所2"] = excel_df["物件住所２"]

        # 物件住所3 = 物件住所３ + 物件名 + 部屋番号
        addr3 = excel_df["物件住所３"].fillna("") if "物件住所３" in excel_df.columns else ""
        prop_name = excel_df["物件名"].fillna("") if "物件名" in excel_df.columns else ""
        room = excel_df["部屋番号"].fillna("") if "部屋番号" in excel_df.columns else ""
        output_df["物件住所3"] = addr3 + prop_name + room

        # 賃料関連
        if "賃料" in excel_df.columns:
            output_df["月額賃料"] = excel_df["賃料"]
            # 退去手続き（実費）にも「賃料」列を使用
            output_df["退去手続き（実費）"] = excel_df["賃料"]
        if "管理費公益費" in excel_df.columns:
            output_df["管理費"] = excel_df["管理費公益費"]
        if "水道代" in excel_df.columns:
            output_df["水道代"] = excel_df["水道代"]
        if "駐車場" in excel_df.columns:
            output_df["駐車場代"] = excel_df["駐車場"]
        if "その他費用" in excel_df.columns:
            output_df["その他費用1"] = excel_df["その他費用"]

        # 管理会社
        if "加盟店: 加盟店名" in excel_df.columns:
            output_df["管理会社"] = excel_df["加盟店: 加盟店名"]

        # 回収口座番号
        if "バーチャル口座: 名称" in excel_df.columns:
            output_df["回収口座番号"] = excel_df["バーチャル口座: 名称"]

        # 引継情報（●入居日 + 日割家賃発生日）
        if "日割家賃発生日" in excel_df.columns:
            # 日付フォーマットを変換: 2024/03/15 → 2024/3/15
            dates = pd.to_datetime(excel_df["日割家賃発生日"], errors='coerce')
            formatted_dates = dates.dt.strftime('%Y/%-m/%-d')  # ゼロ埋めなし
            output_df["引継情報"] = "●入居日" + formatted_dates.fillna("")

    def map_guarantor_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """
        保証人情報をマッピング（in-place）

        Args:
            output_df: 出力DataFrame（in-place更新）
            excel_df: 入力Excel DataFrame

        Note:
            入力の「連帯保証人」を出力の「保証人１」にマッピング
        """
        # 連帯保証人 → 保証人１
        if "連保人1氏名" in excel_df.columns:
            output_df["保証人１氏名"] = excel_df["連保人1氏名"]
            # 氏名に値がある場合は関係を「他」に設定
            name = excel_df["連保人1氏名"].fillna("")
            output_df["保証人１契約者との関係"] = name.apply(lambda x: "他" if x != "" else "")
        if "連保人1氏名かな" in excel_df.columns:
            # ひらがなをカタカナに変換
            kana = excel_df["連保人1氏名かな"].fillna("")
            output_df["保証人１カナ"] = kana.str.translate(
                str.maketrans("ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゕゖ",
                             "ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶ")
            )
        if "連保人1生年月日" in excel_df.columns:
            output_df["保証人１生年月日"] = excel_df["連保人1生年月日"]
        if "連保人1郵便番号" in excel_df.columns:
            logger = logging.getLogger(__name__)

            # 郵便番号が空白の場合、住所からルックアップを試みる
            def get_guarantor_zipcode(row):
                row_idx = row.name
                zipcode = row.get("連保人1郵便番号", "")

                if pd.isna(zipcode) or not str(zipcode).strip():
                    addr1 = row.get("連保人1住所１", "")
                    addr2 = row.get("連保人1住所２", "")
                    addr3 = row.get("連保人1住所３", "")

                    # pd.NAを安全に処理
                    def safe_val(val):
                        return "" if pd.isna(val) else str(val).strip()

                    addr1_safe = safe_val(addr1)
                    addr2_safe = safe_val(addr2)
                    addr3_safe = safe_val(addr3)

                    # 住所が全て空白なら何もしない
                    if not (addr1_safe or addr2_safe or addr3_safe):
                        return ""

                    # 住所があればAPI検索（建物名は除外して精度向上）
                    logger.info(f"行{row_idx} - 保証人１郵便番号を検索中: {addr1_safe}{addr2_safe}")
                    zipcode = lookup_zipcode_from_address(addr1, addr2, "")

                    if zipcode:
                        logger.info(f"行{row_idx} - 保証人１郵便番号を補完: {zipcode}")
                    else:
                        logger.warning(f"行{row_idx} - 保証人１郵便番号: 検索結果なし ({addr1_safe}{addr2_safe})")

                return format_zipcode(zipcode)

            output_df["保証人１郵便番号"] = excel_df.apply(get_guarantor_zipcode, axis=1)
        if "連保人1住所１" in excel_df.columns:
            output_df["保証人１住所1"] = excel_df["連保人1住所１"]
        if "連保人1住所２" in excel_df.columns:
            output_df["保証人１住所2"] = excel_df["連保人1住所２"]

        # 保証人住所3 = 連保人1住所３ + 連保人住所アパート等（該当列がある場合）
        addr3 = excel_df["連保人1住所３"].fillna("") if "連保人1住所３" in excel_df.columns else ""
        apt = excel_df["連保人住所アパート等"].fillna("") if "連保人住所アパート等" in excel_df.columns else ""
        output_df["保証人１住所3"] = addr3 + apt
        if "連保人1電話" in excel_df.columns:
            output_df["保証人１TEL自宅"] = excel_df["連保人1電話"].apply(format_phone)
        if "連保人1携帯番号" in excel_df.columns:
            output_df["保証人１TEL携帯"] = excel_df["連保人1携帯番号"].apply(format_phone)

    def map_emergency_contact_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """
        緊急連絡人情報をマッピング（in-place）

        Args:
            output_df: 出力DataFrame（in-place更新）
            excel_df: 入力Excel DataFrame
        """
        # 緊急連絡人１
        if "緊急連絡人氏名" in excel_df.columns:
            output_df["緊急連絡人１氏名"] = excel_df["緊急連絡人氏名"]
            # 氏名に値がある場合は関係を「他」に設定
            name = excel_df["緊急連絡人氏名"].fillna("")
            output_df["緊急連絡人１契約者との関係"] = name.apply(lambda x: "他" if x != "" else "")
        if "緊急連絡人氏名かな" in excel_df.columns:
            # ひらがなをカタカナに変換
            kana = excel_df["緊急連絡人氏名かな"].fillna("")
            output_df["緊急連絡人１カナ"] = kana.str.translate(
                str.maketrans("ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゕゖ",
                             "ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶ")
            )
        if "緊急連絡人郵便番号" in excel_df.columns:
            logger = logging.getLogger(__name__)

            # 郵便番号が空白の場合、住所からルックアップを試みる
            def get_emergency_contact_zipcode(row):
                row_idx = row.name
                zipcode = row.get("緊急連絡人郵便番号", "")

                if pd.isna(zipcode) or not str(zipcode).strip():
                    addr1 = row.get("緊急連絡人住所１", "")
                    addr2 = row.get("緊急連絡人住所２", "")
                    addr3 = row.get("緊急連絡人住所３", "")

                    # pd.NAを安全に処理
                    def safe_val(val):
                        return "" if pd.isna(val) else str(val).strip()

                    addr1_safe = safe_val(addr1)
                    addr2_safe = safe_val(addr2)
                    addr3_safe = safe_val(addr3)

                    # 住所が全て空白なら何もしない
                    if not (addr1_safe or addr2_safe or addr3_safe):
                        return ""

                    # 住所があればAPI検索（建物名は除外して精度向上）
                    logger.info(f"行{row_idx} - 緊急連絡人１郵便番号を検索中: {addr1_safe}{addr2_safe}")
                    zipcode = lookup_zipcode_from_address(addr1, addr2, "")

                    if zipcode:
                        logger.info(f"行{row_idx} - 緊急連絡人１郵便番号を補完: {zipcode}")
                    else:
                        logger.warning(f"行{row_idx} - 緊急連絡人１郵便番号: 検索結果なし ({addr1_safe}{addr2_safe})")

                return format_zipcode(zipcode)

            output_df["緊急連絡人１郵便番号"] = excel_df.apply(get_emergency_contact_zipcode, axis=1)
        if "緊急連絡人住所１" in excel_df.columns:
            output_df["緊急連絡人１現住所1"] = excel_df["緊急連絡人住所１"]
        if "緊急連絡人住所２" in excel_df.columns:
            output_df["緊急連絡人１現住所2"] = excel_df["緊急連絡人住所２"]
        if "緊急連絡人住所３" in excel_df.columns:
            output_df["緊急連絡人１現住所3"] = excel_df["緊急連絡人住所３"]
        if "緊急連絡人電話" in excel_df.columns:
            output_df["緊急連絡人１TEL自宅"] = excel_df["緊急連絡人電話"].apply(format_phone)
        if "緊急連絡人携帯１" in excel_df.columns:
            output_df["緊急連絡人１TEL携帯"] = excel_df["緊急連絡人携帯１"].apply(format_phone)

    def apply_fixed_values(
        self,
        output_df: pd.DataFrame
    ) -> None:
        """
        固定値を適用（in-place）

        Args:
            output_df: 出力DataFrame（in-place更新）
        """
        # 管理受託日（処理実行日）
        today = datetime.now().strftime("%Y/%m/%d")
        output_df["管理受託日"] = today

        # その他固定値を一括適用
        for col, value in self.config.FIXED_VALUES.items():
            if col in output_df.columns:
                output_df[col] = value


def process_nap_data(
    excel_file: Union[bytes, io.BytesIO],
    contract_file: Union[bytes, io.BytesIO]
) -> Tuple[pd.DataFrame, List[str], str]:
    """
    ナップ新規登録のメイン処理関数

    Args:
        excel_file: ミライル様Excelファイル
        contract_file: ContractList CSVファイル

    Returns:
        - output_df: 出力DataFrame（111列）
        - logs: 処理ログ
        - filename: 出力ファイル名
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logs = []
    config = NapConfig()

    try:
        # 1. ファイル読み込み
        phase_start = time.time()
        logger.info("=== Phase 1: ファイル読み込み ===")
        file_reader = FileReader()

        excel_df = file_reader.read_excel_file(excel_file, skiprows=config.EXCEL_SKIPROWS)
        logs.append(f"✓ Excelファイル読み込み: {len(excel_df)}件")

        contract_df = file_reader.read_csv_file(contract_file)
        logs.append(f"✓ ContractList読み込み: {len(contract_df)}件")

        phase_time = time.time() - phase_start
        logger.info(f"Phase 1 completed in {phase_time:.2f}s")

        # 2. ContractListフィルタリング
        phase_start = time.time()
        logger.info("=== Phase 2: ContractListフィルタリング ===")
        checker = DuplicateChecker()

        filtered_contract_df = checker.filter_contract_list(
            contract_df,
            target_id=config.TARGET_CORPORATION_ID
        )
        logs.append(f"✓ ContractListフィルタ（委託先法人ID={config.TARGET_CORPORATION_ID}）: {len(filtered_contract_df)}件")

        phase_time = time.time() - phase_start
        logger.info(f"Phase 2 completed in {phase_time:.2f}s")

        # 3. 重複チェック
        phase_start = time.time()
        logger.info("=== Phase 3: 重複チェック ===")
        new_data, existing_data, stats, check_logs = checker.check_duplicates(
            excel_df,
            filtered_contract_df
        )
        logs.extend(check_logs)

        phase_time = time.time() - phase_start
        logger.info(f"Phase 3 completed in {phase_time:.2f}s")

        # 新規データがない場合
        if len(new_data) == 0:
            logs.append("⚠️ 新規登録対象データがありません（全て既存契約）")
            empty_df = pd.DataFrame(columns=config.OUTPUT_COLUMNS)
            return empty_df, logs, ""

        # 4. データマッピング
        logger.info("=== Phase 4: データマッピング ===")
        mapper = DataMapper(config)

        # 4-1. DataFrame作成
        phase_start = time.time()
        output_df = mapper.create_output_dataframe(new_data)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-1 (create_output_dataframe) completed in {phase_time:.2f}s")

        # 4-2. 契約者情報マッピング
        phase_start = time.time()
        mapper.map_contractor_info(output_df, new_data)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-2 (map_contractor_info) completed in {phase_time:.2f}s")

        # 4-3. 物件情報マッピング
        phase_start = time.time()
        mapper.map_property_info(output_df, new_data)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-3 (map_property_info) completed in {phase_time:.2f}s")

        # 4-4. 保証人情報マッピング
        phase_start = time.time()
        mapper.map_guarantor_info(output_df, new_data)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-4 (map_guarantor_info) completed in {phase_time:.2f}s")

        # 4-5. 緊急連絡人情報マッピング
        phase_start = time.time()
        mapper.map_emergency_contact_info(output_df, new_data)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-5 (map_emergency_contact_info) completed in {phase_time:.2f}s")

        # 4-6. 固定値適用
        phase_start = time.time()
        mapper.apply_fixed_values(output_df)
        phase_time = time.time() - phase_start
        logger.info(f"Phase 4-6 (apply_fixed_values) completed in {phase_time:.2f}s")

        logs.append(f"✓ データマッピング完了: {len(output_df)}件")

        # 5. ファイル名生成
        timestamp = datetime.now().strftime("%m%d")
        filename = f"{timestamp}{config.OUTPUT_FILE_PREFIX}.csv"

        logs.append(f"✓ 処理完了: {filename}")

        return output_df, logs, filename

    except Exception as e:
        logger.error(f"処理エラー: {str(e)}")
        logs.append(f"❌ エラー: {str(e)}")
        raise
