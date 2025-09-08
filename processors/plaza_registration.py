#!/usr/bin/env python3
"""
プラザ新規登録プロセッサ

入力ファイル:
- 2025年09月コールセンター回収委託_ミライル.csv (44列)
- ContractList_*.csv (122列)

出力ファイル:
- MMDDプラザ新規登録.csv (111列)

重複チェック:
- プラザCSV「会員番号」(D列) ⟷ ContractList「引継番号」
"""

import pandas as pd
import io
import re
import chardet
import unicodedata
from datetime import datetime
from typing import Tuple, List, Dict, Union
import logging


class PlazaConfig:
    """プラザ新規登録設定・定数管理クラス"""
    
    OUTPUT_FILE_PREFIX = "プラザ新規登録"
    
    # 正確な111列テンプレートヘッダー
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
        "保証人1氏名", "保証人1カナ", "保証人1契約者との関係", "保証人1生年月日",
        "保証人1郵便番号", "保証人1住所1", "保証人1住所2", "保証人1住所3",
        "保証人1TEL自宅", "保証人1TEL携帯",
        "保証人2氏名", "保証人2カナ", "保証人2契約者との関係", "保証人2生年月日",
        "保証人2郵便番号", "保証人2住所1", "保証人2住所2", "保証人2住所3",
        "保証人2TEL自宅", "保証人2TEL携帯",
        "保証人3氏名", "保証人3カナ", "保証人3契約者との関係", "保証人3生年月日",
        "保証人3郵便番号", "保証人3住所1", "保証人3住所2", "保証人3住所3",
        "保証人3TEL自宅", "保証人3TEL携帯",
        "緊急連絡人1氏名", "緊急連絡人1カナ", "緊急連絡人1契約者との関係", "緊急連絡人1生年月日",
        "緊急連絡人1郵便番号", "緊急連絡人1住所1", "緊急連絡人1住所2", "緊急連絡人1住所3",
        "緊急連絡人1TEL自宅", "緊急連絡人1TEL携帯",
        "緊急連絡人2氏名", "緊急連絡人2カナ", "緊急連絡人2契約者との関係", "緊急連絡人2生年月日",
        "緊急連絡人2郵便番号", "緊急連絡人2住所1", "緊急連絡人2住所2", "緊急連絡人2住所3",
        "緊急連絡人2TEL自宅", "緊急連絡人2TEL携帯",
        "委託先法人ID"
    ]
    
    # プラザCSVヘッダー
    PLAZA_CSV_HEADERS = [
        "コールセンター送信日", "文章タイプ", "部屋受付番号", "会員番号", "号室", "AP番号",
        "氏名（漢字）", "フリガナ", "生年月日", "郵便番号", "住所", "物件名",
        "電話番号", "メール", "業態区分名", "国籍", "契約日", "入居日",
        "利用料合計", "未納利用料合計", "支払年保金額", "未納年保金額",
        "支払更新料", "未納更新料", "未納事務手数料", "延滞合計", "事務手数料",
        "バーチャル口座支店番号", "バーチャル口座支店名", "バーチャル口座番号",
        "マイペイメントurl", "支払期日",
        "連帯保証人　名（漢字）", "連帯保証人　フリガナ", "連帯保証人　続柄",
        "連帯保証人　電話番号",
        "緊急連絡人　氏名（漢字）", "緊急連絡人　フリガナ", "緊急連絡人　続柄",
        "緊急連絡人　電話番号",
        "勤務先名", "勤務先住所", "勤務先TEL", "滞納スパン"
    ]
    
    # 都道府県リスト
    PREFECTURES = [
        '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
        '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
        '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
        '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
        '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
        '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
        '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
    ]


class FileReader:
    """ファイル読み込みクラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def read_file(self, file_content: Union[bytes, io.BytesIO], 
                  expected_headers: List[str] = None) -> pd.DataFrame:
        """ファイル読み込み"""
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
                
                # ヘッダー検証
                if expected_headers:
                    if list(df.columns) != expected_headers:
                        continue
                
                self.logger.info(f"Successfully read file with encoding: {enc}")
                return df
                
            except Exception:
                continue
        
        raise ValueError("ファイルの読み込みに失敗しました")


class DuplicateChecker:
    """重複チェッククラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_duplicates(self, plaza_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict, List]:
        """重複チェック処理"""
        logs = []
        
        # 会員番号列の取得
        plaza_member_col = plaza_df.columns[3]  # D列（0ベース）
        
        # 引継番号でチェック
        existing_contracts = plaza_df[plaza_df[plaza_member_col].isin(contract_df["引継番号"])].copy()
        new_contracts = plaza_df[~plaza_df[plaza_member_col].isin(contract_df["引継番号"])].copy()
        
        # 統計情報
        stats = {
            'total_plaza': len(plaza_df),
            'new_contracts': len(new_contracts),
            'existing_contracts': len(existing_contracts),
            'new_percentage': (len(new_contracts) / len(plaza_df) * 100) if len(plaza_df) > 0 else 0
        }
        
        return new_contracts, existing_contracts, stats, logs


class DataConverter:
    """データ変換クラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prefectures = PlazaConfig.PREFECTURES
    
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
    
    def normalize_phone_number(self, value: str) -> str:
        """電話番号の正規化（先頭0補完とハイフン挿入）"""
        if pd.isna(value) or str(value).strip() == "":
            return ""
        
        phone = str(value).strip()
        
        # 全角数字を半角に変換
        phone = unicodedata.normalize('NFKC', phone)
        
        # 数字のみ抽出
        phone_digits = re.sub(r'[^\d]', '', phone)
        
        if not phone_digits:
            return ""
        
        # 先頭0補完
        if len(phone_digits) == 10 and phone_digits[0] != '0':
            phone_digits = '0' + phone_digits
        elif len(phone_digits) == 9 and phone_digits[0] in ['3', '4', '5', '6', '7', '8', '9']:
            phone_digits = '0' + phone_digits
        
        # ハイフン挿入
        if len(phone_digits) == 11:  # 携帯番号
            return f"{phone_digits[:3]}-{phone_digits[3:7]}-{phone_digits[7:]}"
        elif len(phone_digits) == 10:  # 固定電話
            if phone_digits[:2] == '03' or phone_digits[:2] == '06':
                return f"{phone_digits[:2]}-{phone_digits[2:6]}-{phone_digits[6:]}"
            else:
                return f"{phone_digits[:3]}-{phone_digits[3:6]}-{phone_digits[6:]}"
        
        return phone_digits
    
    def hankaku_to_zenkaku(self, text: str) -> str:
        """半角カナを全角カナに変換"""
        if not text:
            return ""
        return unicodedata.normalize('NFKC', text)
    
    def convert_date_format(self, date_str: str) -> str:
        """日付フォーマット変換 YYYYMMDD → YYYY/M/D"""
        if pd.isna(date_str) or str(date_str).strip() == "":
            return ""
        
        date_str = str(date_str).strip()
        
        # 数字のみ抽出
        date_digits = re.sub(r'[^\d]', '', date_str)
        
        if len(date_digits) == 8:
            year = date_digits[:4]
            month = str(int(date_digits[4:6]))  # 先頭0を除去
            day = str(int(date_digits[6:8]))    # 先頭0を除去
            return f"{year}/{month}/{day}"
        
        return date_str
    
    def split_address(self, address: str) -> Dict[str, str]:
        """住所を都道府県、市区町村、残り住所に分割"""
        if pd.isna(address) or str(address).strip() == "":
            return {"prefecture": "", "city": "", "remaining": ""}
        
        addr = str(address).strip()
        
        # 都道府県を検索
        prefecture = ""
        for pref in self.prefectures:
            if addr.startswith(pref):
                prefecture = pref
                addr = addr[len(pref):]
                break
        
        # 市区町村を抽出
        city = ""
        
        # 市川市・市原市・町田市の特別処理
        if addr.startswith("市川市"):
            city = "市川市"
            addr = addr[3:]
        elif addr.startswith("市原市"):
            city = "市原市"
            addr = addr[3:]
        elif addr.startswith("町田市"):
            city = "町田市"
            addr = addr[3:]
        
        # 東京23区の特別処理
        if prefecture == "東京都" and not city:
            tokyo_wards = [
                "千代田区", "中央区", "港区", "新宿区", "文京区", "台東区", "墨田区", "江東区",
                "品川区", "目黒区", "大田区", "世田谷区", "渋谷区", "中野区", "杉並区", "豊島区",
                "北区", "荒川区", "板橋区", "練馬区", "足立区", "葛飾区", "江戸川区"
            ]
            for ward in tokyo_wards:
                if addr.startswith(ward):
                    city = ward
                    addr = addr[len(ward):]
                    break
        
        # 一般的な市区町村パターン
        if not city:
            # 政令指定都市パターン（○○市○○区）を優先処理
            designated_city_pattern = r'^([^市区町村]+?市[^市区町村]+?区)'
            match = re.match(designated_city_pattern, addr)
            if match:
                city = match.group(1)
                addr = addr[len(city):]
            else:
                # 一般市町村パターン
                city_patterns = [
                    r'^([^市区町村]+?[市])',
                    r'^([^市区町村]+?[町])',  
                    r'^([^市区町村]+?[村])'
                ]
                for pattern in city_patterns:
                    match = re.match(pattern, addr)
                    if match:
                        city = match.group(1)
                        addr = addr[len(city):]
                        break
        
        return {
            "prefecture": prefecture,
            "city": city,
            "remaining": addr
        }


class PlazaProcessor:
    """プラザ新規登録プロセッサー"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.file_reader = FileReader()
        self.duplicate_checker = DuplicateChecker()
        self.converter = DataConverter()
    
    def convert_to_output_format(self, plaza_df: pd.DataFrame) -> pd.DataFrame:
        """プラザデータを111列フォーマットに変換"""
        self.logger.info(f"変換開始: {len(plaza_df)}行")
        output_df = pd.DataFrame(columns=PlazaConfig.OUTPUT_COLUMNS)
        
        for idx, row in plaza_df.iterrows():
            try:
                self.logger.debug(f"行 {idx} 処理開始")
                output_row = {}
                
                # 列インデックスで取得（0ベース）
                cols = plaza_df.columns
                
                # A列：引継番号 ← D列「会員番号」
                output_row["引継番号"] = self.converter.safe_str_convert(row[cols[3]])
                
                # B列：契約者氏名 ← G列「氏名（漢字）」（スペース削除）
                output_row["契約者氏名"] = self.converter.remove_all_spaces(
                    self.converter.safe_str_convert(row[cols[6]])
                )
                
                # C列：契約者カナ ← H列「フリガナ」（スペース削除、半角→全角カナ変換）
                kana = self.converter.safe_str_convert(row[cols[7]])
                kana = self.converter.remove_all_spaces(kana)
                output_row["契約者カナ"] = self.converter.hankaku_to_zenkaku(kana)
            
                # D列：契約者生年月日 ← I列「生年月日」（YYYYMMDD→YYYY/M/D）
                output_row["契約者生年月日"] = self.converter.convert_date_format(
                    self.converter.safe_str_convert(row[cols[8]])
                )
            
                # E列：契約者TEL自宅（空欄）
                output_row["契約者TEL自宅"] = ""
            
                # F列：契約者TEL携帯 ← M列「電話番号」（先頭0補完、ハイフン挿入）
                output_row["契約者TEL携帯"] = self.converter.normalize_phone_number(
                    self.converter.safe_str_convert(row[cols[12]])
                )
            
                # G列：契約者現住所郵便番号 ← J列「郵便番号」
                output_row["契約者現住所郵便番号"] = self.converter.safe_str_convert(row[cols[9]])
            
                # 住所分割（K列「住所」）
                address = self.converter.safe_str_convert(row[cols[10]])
                addr_parts = self.converter.split_address(address)
            
                # H列：契約者現住所1（都道府県）
                output_row["契約者現住所1"] = addr_parts["prefecture"]
            
                # I列：契約者現住所2（市区町村）
                output_row["契約者現住所2"] = addr_parts["city"]
            
                # J列：契約者現住所3（住所の残り + 物件名 + 号室）
                remaining = addr_parts["remaining"]
                building = self.converter.safe_str_convert(row[cols[11]])  # L列「物件名」
                room = self.converter.safe_str_convert(row[cols[4]])      # E列「号室」
                output_row["契約者現住所3"] = f"{remaining}{building}{room}"
            
                # K列：引継情報（処理日付 + "プラザ一括登録"）
                today = datetime.now().strftime("%Y/%-m/%-d")
                output_row["引継情報"] = f"{today}プラザ一括登録"
            
                # L列：物件名 ← L列「物件名」
                output_row["物件名"] = building
            
                # M列：部屋番号 ← E列「号室」
                output_row["部屋番号"] = room
            
                # N列：物件住所郵便番号 ← J列「郵便番号」
                output_row["物件住所郵便番号"] = self.converter.safe_str_convert(row[cols[9]])
            
                # O列：物件住所1（都道府県）
                output_row["物件住所1"] = addr_parts["prefecture"]
            
                # P列：物件住所2（市区町村）
                output_row["物件住所2"] = addr_parts["city"]
            
                # Q列：物件住所3（住所の残り部分のみ、物件名・号室は含まない）
                output_row["物件住所3"] = addr_parts["remaining"]
            
                # R列：入居ステータス（固定値）
                output_row["入居ステータス"] = "入居中"
            
                # S列：滞納ステータス（固定値）
                output_row["滞納ステータス"] = "未精算"
            
                # T列：受託状況（固定値）
                output_row["受託状況"] = "契約中"
            
                # U列：月額賃料 ← S列「利用料合計」
                output_row["月額賃料"] = self.converter.safe_str_convert(row[cols[18]])
            
                # V〜AC列：空欄
                for col in ["管理費", "共益費", "水道代", "駐車場代", "その他費用1", "その他費用2", "敷金", "礼金"]:
                    output_row[col] = ""
            
                # AD列：回収口座金融機関CD（固定値）
                output_row["回収口座金融機関CD"] = "310"
            
                # AE列：回収口座金融機関名（固定値）
                output_row["回収口座金融機関名"] = "GMOあおぞらネット銀行"
            
                # AF列：回収口座支店CD ← AB列「バーチャル口座支店番号」
                output_row["回収口座支店CD"] = self.converter.safe_str_convert(row[cols[27]])
            
                # AG列：回収口座支店名 ← AC列「バーチャル口座支店名」
                output_row["回収口座支店名"] = self.converter.safe_str_convert(row[cols[28]])
            
                # AH列：回収口座種類（固定値）
                output_row["回収口座種類"] = "普通"
            
                # AI列：回収口座番号 ← AD列「バーチャル口座番号」
                output_row["回収口座番号"] = self.converter.safe_str_convert(row[cols[29]])
            
                # AJ列：回収口座名義（固定値）
                output_row["回収口座名義"] = "プラザ賃貸管理保証株式会社"
            
                # AK列：契約種類（固定値）
                output_row["契約種類"] = "バックレント"
            
                # AL列：管理受託日（処理実行日）
                output_row["管理受託日"] = datetime.now().strftime("%Y/%m/%d")
            
                # AM列：契約確認日（空欄）
                output_row["契約確認日"] = ""
            
                # AN列：退去済手数料（固定値）
                output_row["退去済手数料"] = "20"
            
                # AO列：入居中滞納手数料（固定値）
                output_row["入居中滞納手数料"] = "10"
            
                # AP列：入居中正常手数料（固定値）
                output_row["入居中正常手数料"] = "0"
            
                # AQ列：管理前滞納額（Z列「延滞合計」+ AA列「事務手数料」）
                late_fee_str = self.converter.safe_str_convert(row[cols[25]])  # Z列
                admin_fee_str = self.converter.safe_str_convert(row[cols[26]])  # AA列
                
                try:
                    late_fee = float(late_fee_str) if late_fee_str else 0.0
                    admin_fee = float(admin_fee_str) if admin_fee_str else 0.0
                    output_row["管理前滞納額"] = str(int(late_fee + admin_fee))
                except ValueError as e:
                    error_msg = f"行 {idx+1}: 管理前滞納額の計算エラー - 延滞合計: '{late_fee_str}', 事務手数料: '{admin_fee_str}'"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg) from e
            
                # AR〜AU列：空欄
                for col in ["更新契約手数料", "退去手続き（実費）", "初回振替月", "保証開始日"]:
                    output_row[col] = ""
            
                # AV列：クライアントCD（固定値）
                output_row["クライアントCD"] = "7"
            
                # AW列：パートナーCD（空欄）
                output_row["パートナーCD"] = ""
            
                # 勤務先情報（「退職済」の場合は空欄）
                work_name = self.converter.safe_str_convert(row[cols[40]])  # AO列「勤務先名」
                work_tel = self.converter.safe_str_convert(row[cols[42]])   # AQ列「勤務先TEL」
            
                if "退職済" in work_name or "退職済" in work_tel:
                    output_row["契約者勤務先名"] = ""
                    output_row["契約者勤務先TEL"] = ""
                else:
                    output_row["契約者勤務先名"] = work_name
                    output_row["契約者勤務先TEL"] = work_tel
            
                # AY列：契約者勤務先カナ（空欄）
                output_row["契約者勤務先カナ"] = ""
            
                # BA〜BE列：勤務先住所関連（空欄）
                for col in ["勤務先業種", "契約者勤務先郵便番号", "契約者勤務先住所1", "契約者勤務先住所2", "契約者勤務先住所3"]:
                    output_row[col] = ""
            
                # 保証人１情報
                # BF列：保証人１氏名 ← AG列「連帯保証人　名（漢字）」（スペース削除）
                output_row["保証人1氏名"] = self.converter.remove_all_spaces(
                    self.converter.safe_str_convert(row[cols[32]])
                )
            
                # BG列：保証人１カナ ← AH列「連帯保証人　フリガナ」（スペース削除、半角→全角カナ変換）
                guarantor_kana = self.converter.safe_str_convert(row[cols[33]])
                guarantor_kana = self.converter.remove_all_spaces(guarantor_kana)
                output_row["保証人1カナ"] = self.converter.hankaku_to_zenkaku(guarantor_kana)
            
                # BH列：保証人１契約者との関係（固定値）
                output_row["保証人1契約者との関係"] = "他"
            
                # BI〜BN列：保証人１その他情報（空欄）
                for col in ["保証人1生年月日", "保証人1郵便番号", "保証人1住所1", "保証人1住所2", "保証人1住所3", "保証人1TEL自宅"]:
                    output_row[col] = ""
            
                # BO列：保証人１TEL携帯 ← AJ列「連帯保証人　電話番号」（先頭0補完、ハイフン挿入）
                output_row["保証人1TEL携帯"] = self.converter.normalize_phone_number(
                    self.converter.safe_str_convert(row[cols[35]])
                )
            
                # BP〜BY列：保証人２・３情報（空欄）
                for i in ["2", "3"]:
                    for col in [f"保証人{i}氏名", f"保証人{i}カナ", f"保証人{i}契約者との関係", 
                               f"保証人{i}生年月日", f"保証人{i}郵便番号", f"保証人{i}住所1", 
                               f"保証人{i}住所2", f"保証人{i}住所3", f"保証人{i}TEL自宅", f"保証人{i}TEL携帯"]:
                        output_row[col] = ""
            
                # 緊急連絡人１情報
                # BZ列：緊急連絡人１氏名 ← AK列「緊急連絡人　氏名（漢字）」（スペース削除）
                output_row["緊急連絡人1氏名"] = self.converter.remove_all_spaces(
                    self.converter.safe_str_convert(row[cols[36]])
                )
            
                # CA列：緊急連絡人１カナ ← AL列「緊急連絡人　フリガナ」（スペース削除、半角→全角カナ変換）
                emergency_kana = self.converter.safe_str_convert(row[cols[37]])
                emergency_kana = self.converter.remove_all_spaces(emergency_kana)
                output_row["緊急連絡人1カナ"] = self.converter.hankaku_to_zenkaku(emergency_kana)
            
                # CB列：緊急連絡人１契約者との関係（固定値）
                output_row["緊急連絡人1契約者との関係"] = "他"
            
                # CC〜CG列：緊急連絡人１その他情報（空欄）
                for col in ["緊急連絡人1生年月日", "緊急連絡人1郵便番号", "緊急連絡人1住所1", "緊急連絡人1住所2", "緊急連絡人1住所3", "緊急連絡人1TEL自宅"]:
                    output_row[col] = ""
            
                # CH列：緊急連絡人１TEL携帯 ← AN列「緊急連絡人　電話番号」（先頭0補完、ハイフン挿入）
                output_row["緊急連絡人1TEL携帯"] = self.converter.normalize_phone_number(
                    self.converter.safe_str_convert(row[cols[39]])
                )
            
                # CI〜DB列：緊急連絡人２情報（空欄）
                for col in ["緊急連絡人2氏名", "緊急連絡人2カナ", "緊急連絡人2契約者との関係",
                       "緊急連絡人2生年月日", "緊急連絡人2郵便番号", "緊急連絡人2住所1",
                       "緊急連絡人2住所2", "緊急連絡人2住所3", "緊急連絡人2TEL自宅", "緊急連絡人2TEL携帯"]:
                    output_row[col] = ""
            
                # DC列：委託先法人ID（固定値）
                output_row["委託先法人ID"] = "6"
            
                self.logger.debug(f"行 {idx} DataFrame連結開始")
                output_df = pd.concat([output_df, pd.DataFrame([output_row])], ignore_index=True)
                self.logger.debug(f"行 {idx} 処理完了")
            
            except Exception as e:
                self.logger.error(f"行 {idx} でエラー発生: {str(e)}", exc_info=True)
                raise
        
        self.logger.info(f"変換完了: 出力{len(output_df)}行")
        return output_df
    
    def process(self, plaza_file, contract_file) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict, List]:
        """メイン処理"""
        try:
            logs = []
            self.logger.info("プラザ新規登録処理開始")
            
            # ファイル読み込み
            self.logger.info("ファイル読み込み開始")
            plaza_df = self.file_reader.read_file(plaza_file)
            self.logger.info(f"プラザファイル読み込み完了: {len(plaza_df)}行")
            contract_df = self.file_reader.read_file(contract_file)
            self.logger.info(f"ContractList読み込み完了: {len(contract_df)}行")
            
            # 重複チェック
            self.logger.info("重複チェック開始")
            new_contracts, existing_contracts, stats, check_logs = self.duplicate_checker.check_duplicates(
                plaza_df, contract_df
            )
            logs.extend(check_logs)
            self.logger.info(f"重複チェック完了: 新規{len(new_contracts)}件, 既存{len(existing_contracts)}件")
            
            # 新規契約をフォーマット変換
            self.logger.info("フォーマット変換開始")
            if len(new_contracts) > 0:
                output_df = self.convert_to_output_format(new_contracts)
                self.logger.info(f"フォーマット変換完了: {len(output_df)}行")
            else:
                output_df = pd.DataFrame(columns=PlazaConfig.OUTPUT_COLUMNS)
                self.logger.info("新規契約なし")
            
            self.logger.info(f"プラザ新規登録処理完了: {stats}")
            
            return output_df, new_contracts, existing_contracts, stats, logs
            
        except Exception as e:
            self.logger.error(f"処理エラー: {str(e)}", exc_info=True)
            raise


def process_plaza_data(plaza_file, contract_file) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict, List]:
    """プラザ新規登録処理のエントリポイント"""
    processor = PlazaProcessor()
    return processor.process(plaza_file, contract_file)