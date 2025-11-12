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
"""

import pandas as pd
import io
import chardet
from datetime import datetime
from typing import Tuple, List, Dict, Union
import logging


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
        "回収口座番号": "4389306",
        "回収口座名義": "ナップ賃貸保証株式会社",

        # その他固定値
        "クライアントCD": "9268",
        "委託先法人ID": "5",
        "退去済手数料": "0",
        "入居中滞納手数料": "0",
        "入居中正常手数料": "0",
        "管理前滞納額": "1",

        # 空白項目
        "契約者生年月日": "",
        "契約者現住所郵便番号": "",
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
        "共益費": "",
        "敷金": "",
        "礼金": "",
        "その他費用2": "",
        "更新契約手数料": "",
        "退去手続き（実費）": "",

        # 引継情報（空白）
        "引継情報": "",

        # 支店関連（空白）
        "回収口座支店CD": "",
        "回収口座支店名": "",
        "引落銀行CD": "",
        "引落銀行名": "",
        "引落支店CD": "",
        "引落支店名": "",
        "引落口座番号": "",
        "引落口座名義": "",

        # 物件住所郵便番号（空白）
        "物件住所郵便番号": "",

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
                dtype=str
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

        # エンコーディング検出
        detected = chardet.detect(content)
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
        """契約者情報をマッピング（in-place）"""
        # 引継番号（承認番号）
        if "承認番号" in excel_df.columns:
            output_df["引継番号"] = excel_df["承認番号"].astype(str).str.replace('.0', '', regex=False)

        # 契約者氏名・カナ
        if "契約者氏名" in excel_df.columns:
            output_df["契約者氏名"] = excel_df["契約者氏名"]
        if "契約者氏名かな" in excel_df.columns:
            output_df["契約者カナ"] = excel_df["契約者氏名かな"]

        # 電話番号
        if "契約者電話" in excel_df.columns:
            output_df["契約者TEL自宅"] = excel_df["契約者電話"]
        if "契約者携帯1" in excel_df.columns:
            output_df["契約者TEL携帯"] = excel_df["契約者携帯1"]

        # 現住所
        if "契約者１住所１" in excel_df.columns:
            output_df["契約者現住所1"] = excel_df["契約者１住所１"]
        if "契約者１住所２" in excel_df.columns:
            output_df["契約者現住所2"] = excel_df["契約者１住所２"]
        if "契約者１住所３" in excel_df.columns:
            output_df["契約者現住所3"] = excel_df["契約者１住所３"]

        # 勤務先情報
        if "契約者勤務先名" in excel_df.columns:
            output_df["契約者勤務先名"] = excel_df["契約者勤務先名"]
        if "契約者勤務先電話" in excel_df.columns:
            output_df["契約者勤務先TEL"] = excel_df["契約者勤務先電話"]

    def map_property_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """物件情報をマッピング（in-place）"""
        # 物件名・部屋番号
        if "物件名" in excel_df.columns:
            output_df["物件名"] = excel_df["物件名"]
        if "部屋番号" in excel_df.columns:
            output_df["部屋番号"] = excel_df["部屋番号"]

        # 物件住所
        if "物件住所１" in excel_df.columns:
            output_df["物件住所1"] = excel_df["物件住所１"]
        if "物件住所２" in excel_df.columns:
            output_df["物件住所2"] = excel_df["物件住所２"]
        if "物件住所３" in excel_df.columns:
            output_df["物件住所3"] = excel_df["物件住所３"]

        # 賃料関連
        if "賃料合計額" in excel_df.columns:
            output_df["月額賃料"] = excel_df["賃料合計額"]
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

    def map_guarantor_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """保証人情報をマッピング（in-place）

        注: 入力の「連帯保証人」を出力の「保証人１」にマッピング
        """
        # 連帯保証人 → 保証人１
        if "連保人1氏名" in excel_df.columns:
            output_df["保証人１氏名"] = excel_df["連保人1氏名"]
        if "連保人1氏名かな" in excel_df.columns:
            output_df["保証人１カナ"] = excel_df["連保人1氏名かな"]
        if "連帯保証人関係" in excel_df.columns:
            output_df["保証人１契約者との関係"] = excel_df["連帯保証人関係"]
        if "連保人1生年月日" in excel_df.columns:
            output_df["保証人１生年月日"] = excel_df["連保人1生年月日"]
        if "連保人1郵便番号" in excel_df.columns:
            output_df["保証人１郵便番号"] = excel_df["連保人1郵便番号"]
        if "連保人1住所１" in excel_df.columns:
            output_df["保証人１住所1"] = excel_df["連保人1住所１"]
        if "連保人1住所２" in excel_df.columns:
            output_df["保証人１住所2"] = excel_df["連保人1住所２"]
        if "連保人1住所３" in excel_df.columns:
            output_df["保証人１住所3"] = excel_df["連保人1住所３"]
        if "連保人1電話" in excel_df.columns:
            output_df["保証人１TEL自宅"] = excel_df["連保人1電話"]
        if "連保人1携帯番号" in excel_df.columns:
            output_df["保証人１TEL携帯"] = excel_df["連保人1携帯番号"]

    def map_emergency_contact_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """緊急連絡人情報をマッピング（in-place）"""
        # 緊急連絡人１
        if "緊急連絡人氏名" in excel_df.columns:
            output_df["緊急連絡人１氏名"] = excel_df["緊急連絡人氏名"]
        if "緊急連絡人氏名かな" in excel_df.columns:
            output_df["緊急連絡人１カナ"] = excel_df["緊急連絡人氏名かな"]
        if "緊急連絡人関係" in excel_df.columns:
            output_df["緊急連絡人１契約者との関係"] = excel_df["緊急連絡人関係"]
        if "緊急連絡人郵便番号" in excel_df.columns:
            output_df["緊急連絡人１郵便番号"] = excel_df["緊急連絡人郵便番号"]
        if "緊急連絡人住所１" in excel_df.columns:
            output_df["緊急連絡人１現住所1"] = excel_df["緊急連絡人住所１"]
        if "緊急連絡人住所２" in excel_df.columns:
            output_df["緊急連絡人１現住所2"] = excel_df["緊急連絡人住所２"]
        if "緊急連絡人住所３" in excel_df.columns:
            output_df["緊急連絡人１現住所3"] = excel_df["緊急連絡人住所３"]
        if "緊急連絡人電話" in excel_df.columns:
            output_df["緊急連絡人１TEL自宅"] = excel_df["緊急連絡人電話"]
        if "緊急連絡人携帯１" in excel_df.columns:
            output_df["緊急連絡人１TEL携帯"] = excel_df["緊急連絡人携帯１"]

    def apply_fixed_values(
        self,
        output_df: pd.DataFrame
    ) -> None:
        """固定値を適用（in-place）"""
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
        logger.info("=== Phase 1: ファイル読み込み ===")
        file_reader = FileReader()

        excel_df = file_reader.read_excel_file(excel_file, skiprows=config.EXCEL_SKIPROWS)
        logs.append(f"✓ Excelファイル読み込み: {len(excel_df)}件")

        contract_df = file_reader.read_csv_file(contract_file)
        logs.append(f"✓ ContractList読み込み: {len(contract_df)}件")

        # 2. ContractListフィルタリング
        logger.info("=== Phase 2: ContractListフィルタリング ===")
        checker = DuplicateChecker()

        filtered_contract_df = checker.filter_contract_list(
            contract_df,
            target_id=config.TARGET_CORPORATION_ID
        )
        logs.append(f"✓ ContractListフィルタ（委託先法人ID={config.TARGET_CORPORATION_ID}）: {len(filtered_contract_df)}件")

        # 3. 重複チェック
        logger.info("=== Phase 3: 重複チェック ===")
        new_data, existing_data, stats, check_logs = checker.check_duplicates(
            excel_df,
            filtered_contract_df
        )
        logs.extend(check_logs)

        # 新規データがない場合
        if len(new_data) == 0:
            logs.append("⚠️ 新規登録対象データがありません（全て既存契約）")
            empty_df = pd.DataFrame(columns=config.OUTPUT_COLUMNS)
            return empty_df, logs, ""

        # 4. データマッピング
        logger.info("=== Phase 4: データマッピング ===")
        mapper = DataMapper(config)

        output_df = mapper.create_output_dataframe(new_data)
        mapper.map_contractor_info(output_df, new_data)
        mapper.map_property_info(output_df, new_data)
        mapper.map_guarantor_info(output_df, new_data)
        mapper.map_emergency_contact_info(output_df, new_data)
        mapper.apply_fixed_values(output_df)

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
