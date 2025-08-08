#!/usr/bin/env python3
"""
カプコ新規登録プロセッサ（0から完全再構築版）
正確な111列テンプレートヘッダー完全準拠

入力ファイル:
- カプコ元データ.csv (46列)
- ContractList_*.csv (122列)

出力ファイル:
- ContractInfoSample.csv (111列) - 絶対変更禁止テンプレート準拠

マージキー:
- カプコ元データ「契約No」⟷ ContractList「引継番号」
"""

import pandas as pd
import io
import re
import chardet
from datetime import datetime
from typing import Tuple, List, Dict, Union
import logging


class CapcoConfig:
    """カプコ新規登録設定・定数管理クラス"""
    
    OUTPUT_FILE_PREFIX = "カプコ新規登録"
    
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
        "契約種類": "バックレント",
        "回収口座種類": "普通",
        
        # 手数料関連
        "退去済手数料": 25,
        "入居中滞納手数料": 0,
        "入居中正常手数料": 0,
        "回収口座金融機関CD": 9,
        
        # 空白項目（費用関連）
        "契約者生年月日": "",
        "月額賃料": "", "管理費": "", "共益費": "", "水道代": "", "駐車場代": "",
        "その他費用1": "", "その他費用2": "", "敷金": "", "礼金": "",
        
        # 空白項目（その他）
        "契約確認日": "", "更新契約手数料": "", "保証開始日": "",
        "パートナーCD": "", "引落預金種別": "", "登録フラグ": "",
        
        # 勤務先情報（全て空白）
        "契約者勤務先名": "", "契約者勤務先カナ": "", "契約者勤務先TEL": "", "勤務先業種": "",
        "契約者勤務先郵便番号": "", "契約者勤務先住所1": "", "契約者勤務先住所2": "", "契約者勤務先住所3": "",
        
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
        
        # 委託先法人ID
        "委託先法人ID": "5"
    }
    
    # 支店コードマッピング
    BRANCH_CODE_MAPPING = {
        "中央支店": "763",
        "東海支店": "730"
    }
    
    # クライアントCDマッピング
    CLIENT_CD_MAPPING = {
        "1004": "1",
        "1005": "4"
    }
    
    # 47都道府県リスト
    PREFECTURES = [
        "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
        "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
        "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
        "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
        "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
        "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
        "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
    ]


class DataLoader:
    """CSVファイル読み込みクラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_encoding(self, file_content: bytes) -> str:
        """エンコーディング自動検出（文字化け解消）"""
        # chardetで検出
        result = chardet.detect(file_content[:10000])
        detected_encoding = result['encoding']
        confidence = result.get('confidence', 0)
        
        # エンコーディング候補リスト（優先順位）
        encoding_candidates = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8', 'iso-2022-jp', 'euc-jp']
        
        # 信頼度が低い場合は代替エンコーディングを試す
        if confidence < 0.7:
            for encoding in encoding_candidates:
                try:
                    file_content.decode(encoding)
                    return encoding
                except UnicodeDecodeError:
                    continue
        
        # よくあるエンコーディングの修正
        if detected_encoding in ['CP932', 'SHIFT_JIS', 'SHIFT-JIS']:
            return 'cp932'
        elif detected_encoding in ['UTF-8-SIG']:
            return 'utf-8-sig'
        elif detected_encoding:
            return detected_encoding
        else:
            return 'cp932'  # フォールバック
    
    def load_csv_from_bytes(self, file_content: bytes) -> pd.DataFrame:
        """バイト形式CSVファイル読み込み（エンコーディング自動対応）"""
        encoding = self.detect_encoding(file_content)
        
        # 複数のエンコーディングで試行
        encodings_to_try = [encoding, 'cp932', 'shift_jis', 'utf-8-sig', 'utf-8', 'iso-2022-jp', 'euc-jp']
        
        for try_encoding in encodings_to_try:
            try:
                df = pd.read_csv(io.BytesIO(file_content), encoding=try_encoding, dtype=str)
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if try_encoding == encodings_to_try[-1]:
                    raise ValueError(f"CSVファイルの読み込みに失敗: {str(e)}")
                continue
        
        raise ValueError("対応するエンコーディングが見つかりませんでした")
    
    def load_capco_data(self, file_content: bytes) -> pd.DataFrame:
        """カプコ元データ読み込み"""
        df = self.load_csv_from_bytes(file_content)
        
        # 必須カラム確認
        required_columns = ["契約No", "契約者名", "契約者ふりがな"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"カプコ元データに必須カラムが不足: {missing_columns}")
        
        return df
    
    def load_contract_list(self, file_content: bytes) -> pd.DataFrame:
        """ContractList読み込み"""
        df = self.load_csv_from_bytes(file_content)
        
        if "引継番号" not in df.columns:
            raise ValueError("ContractListに'引継番号'カラムが見つかりません")
        
        # 重複除去
        df = df.drop_duplicates(subset=["引継番号"], keep='first')
        return df


class DuplicateChecker:
    """重複チェッククラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def find_new_contracts(self, capco_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
        """新規契約の特定（マージキー照合）"""
        # キー項目を文字列に変換・正規化
        capco_df = capco_df.copy()
        capco_df["契約No"] = capco_df["契約No"].astype(str).str.strip()
        
        contract_df = contract_df.copy()
        contract_df["引継番号"] = contract_df["引継番号"].astype(str).str.strip()
        
        # 既存契約番号セット作成
        existing_contract_numbers = set(contract_df["引継番号"].tolist())
        
        # 新規・既存を分離
        new_contracts = capco_df[~capco_df["契約No"].isin(existing_contract_numbers)].copy()
        existing_contracts = capco_df[capco_df["契約No"].isin(existing_contract_numbers)].copy()
        
        # 統計情報
        stats = {
            'total_capco': len(capco_df),
            'new_contracts': len(new_contracts),
            'existing_contracts': len(existing_contracts),
            'new_percentage': (len(new_contracts) / len(capco_df) * 100) if len(capco_df) > 0 else 0
        }
        
        return new_contracts, existing_contracts, stats


class DataConverter:
    """データ変換クラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prefectures = CapcoConfig.PREFECTURES
    
    def convert_name(self, name: str) -> str:
        """氏名変換：スペース削除"""
        if pd.isna(name) or str(name).strip() == "":
            return ""
        return str(name).strip().replace(" ", "").replace("　", "")
    
    def convert_kana(self, kana: str) -> str:
        """カナ変換：ひらがな→カタカナ + スペース削除"""
        if pd.isna(kana) or str(kana).strip() == "":
            return ""
        
        kana_str = str(kana).strip()
        
        # ひらがな→カタカナ変換
        hiragana = 'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをんがぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽぁぃぅぇぉゃゅょっ'
        katakana = 'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲンガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポァィゥェォャュョッ'
        
        kana_map = str.maketrans(hiragana, katakana)
        katakana_result = kana_str.translate(kana_map)
        
        # スペース削除
        return katakana_result.replace(" ", "").replace("　", "")
    
    def extract_clean_phone_number(self, phone_input: str) -> str:
        """
        混入文字を除去して電話番号のみを抽出
        
        Examples:
            '未080-5787-5364' -> '080-5787-5364'
            '×042-361-5460' -> '042-361-5460'  
            '娘080-6868-0817' -> '080-6868-0817'
        """
        if pd.isna(phone_input) or str(phone_input).strip() == "":
            return ""
        
        phone_str = str(phone_input).strip()
        
        # 日本の電話番号パターン（優先順位順）
        phone_patterns = [
            r'(\d{3}-\d{4}-\d{4})',         # 携帯: 080-1234-5678
            r'(\d{2}-\d{4}-\d{4})',         # 固定: 03-1234-5678
            r'(\d{3}-\d{3}-\d{4})',         # 固定: 042-123-4567
            r'(\d{4}-\d{2}-\d{4})',         # 固定: 0123-45-6789
            r'(\d{5}-\d{1}-\d{4})',         # 固定: 01234-5-6789
            r'(\d{11})',                    # ハイフンなし11桁: 08012345678
            r'(\d{10})',                    # ハイフンなし10桁: 0312345678
        ]
        
        # 各パターンで電話番号を検索
        for pattern in phone_patterns:
            match = re.search(pattern, phone_str)
            if match:
                extracted_phone = match.group(1)
                
                # 11桁または10桁の数字のみの場合、適切な形式に変換
                if extracted_phone.isdigit():
                    if len(extracted_phone) == 11:
                        # 11桁の場合（携帯）: 08012345678 -> 080-1234-5678
                        return f"{extracted_phone[:3]}-{extracted_phone[3:7]}-{extracted_phone[7:]}"
                    elif len(extracted_phone) == 10:
                        # 10桁の場合（固定）: 0312345678 -> 03-1234-5678
                        if extracted_phone.startswith('0'):
                            if extracted_phone[1] in ['3', '4', '6']:  # 東京・大阪・福岡など2桁市外局番
                                return f"{extracted_phone[:2]}-{extracted_phone[2:6]}-{extracted_phone[6:]}"
                            else:  # 3桁市外局番
                                return f"{extracted_phone[:3]}-{extracted_phone[3:6]}-{extracted_phone[6:]}"
                
                return extracted_phone
        
        return ""
    
    def process_phone_numbers(self, home_tel: str, mobile_tel: str) -> Tuple[str, str]:
        """電話番号処理：携帯優先ロジック + 混入文字除去"""
        # 混入文字を除去してクリーンな電話番号を抽出
        home_clean = self.extract_clean_phone_number(home_tel)
        mobile_clean = self.extract_clean_phone_number(mobile_tel)
        
        # 携帯番号が空で自宅番号がある場合、自宅を携帯に移動
        if not mobile_clean and home_clean:
            return "", home_clean
        
        return home_clean, mobile_clean
    
    def split_address(self, full_address: str, building_name: str, room_name: str) -> Dict[str, str]:
        """住所分割処理"""
        if pd.isna(full_address) or str(full_address).strip() == "":
            return {"prefecture": "", "city": "", "address3": ""}
        
        addr = str(full_address).strip()
        building = str(building_name).strip() if pd.notna(building_name) else ""
        room = str(room_name).strip() if pd.notna(room_name) else ""
        
        # 1. 都道府県抽出
        prefecture = ""
        for pref in self.prefectures:
            if addr.startswith(pref):
                prefecture = pref
                addr = addr[len(pref):]
                break
        
        # 2. 市区町村抽出（政令指定都市対応）
        city = ""
        city_patterns = [
            r'([^市区町村]*市[^区]*区)',  # 政令指定都市の区（例：横浜市港北区）
            r'([^市区町村]*[市])',       # 市
            r'([^市区町村]*[区])',       # 特別区（東京23区など）
            r'([^市区町村]*[町村])'      # 町村
        ]
        
        for pattern in city_patterns:
            match = re.search(pattern, addr)
            if match:
                city = match.group(1)
                addr = addr[len(city):]
                break
        
        # 3. 現住所3組み立て（市区町村以降 + 建物名 + 部屋名）
        address3_parts = []
        if addr.strip():
            address3_parts.append(addr.strip())
        if building:
            address3_parts.append(building)
        if room:
            address3_parts.append(room)
        
        address3 = "　".join(address3_parts)  # 全角スペース結合
        
        # 物件住所3用（建物名・部屋名を除いた部分）
        property_address3 = addr.strip()
        
        return {
            "prefecture": prefecture,
            "city": city,
            "address3": address3,
            "property_address3": property_address3
        }
    
    def get_branch_code(self, branch_name: str) -> str:
        """支店コード取得"""
        if pd.isna(branch_name):
            return ""
        return CapcoConfig.BRANCH_CODE_MAPPING.get(str(branch_name).strip(), "")
    
    def get_client_cd(self, yakutei_date: str) -> str:
        """クライアントCD取得（約定日判定）"""
        if pd.isna(yakutei_date) or str(yakutei_date).strip() == "":
            return ""
        
        date_str = str(yakutei_date).strip()
        
        # 約定日による判定
        if "1004" in date_str:
            return "1"
        elif "1005" in date_str:
            return "4"
        
        return ""
    
    def convert_new_contracts(self, new_contracts_df: pd.DataFrame) -> pd.DataFrame:
        """新規契約データを111列テンプレートに変換"""
        output_data = []
        
        for _, row in new_contracts_df.iterrows():
            converted_row = {}
            
            # 1. 基本情報
            converted_row["引継番号"] = str(row.get("契約No", "")).strip()
            converted_row["契約者氏名"] = self.convert_name(row.get("契約者名", ""))
            converted_row["契約者カナ"] = self.convert_kana(row.get("契約者ふりがな", ""))
            
            # 2. 電話番号処理
            home_tel, mobile_tel = self.process_phone_numbers(
                row.get("契約者：電話番号", ""),
                row.get("契約者：携帯番号", "")
            )
            converted_row["契約者TEL自宅"] = home_tel
            converted_row["契約者TEL携帯"] = mobile_tel
            
            # 3. 住所分割処理
            address_parts = self.split_address(
                row.get("建物：住所", ""),
                row.get("建物名", ""),
                row.get("部屋名", "")
            )
            
            # 契約者現住所
            converted_row["契約者現住所郵便番号"] = str(row.get("建物：郵便番号", "")).strip()
            converted_row["契約者現住所1"] = address_parts["prefecture"]
            converted_row["契約者現住所2"] = address_parts["city"]
            converted_row["契約者現住所3"] = address_parts["address3"]
            
            # 4. 物件情報
            converted_row["物件名"] = str(row.get("建物名", "")).strip()
            converted_row["部屋番号"] = str(row.get("部屋名", "")).strip()
            
            # 物件住所（契約者住所をコピー + 物件住所3は建物名・部屋名除外）
            converted_row["物件住所郵便番号"] = converted_row["契約者現住所郵便番号"]
            converted_row["物件住所1"] = converted_row["契約者現住所1"]
            converted_row["物件住所2"] = converted_row["契約者現住所2"]
            converted_row["物件住所3"] = address_parts["property_address3"]
            
            # 5. 引継情報生成
            contract_start = str(row.get("契約開始", "")).strip()
            converted_row["引継情報"] = f"カプコ一括登録　●保証開始日：{contract_start}"
            
            # 6. 口座情報
            converted_row["回収口座金融機関名"] = str(row.get("V口座銀行名", "")).strip()
            converted_row["回収口座支店CD"] = self.get_branch_code(row.get("V口振支店名", ""))
            converted_row["回収口座支店名"] = str(row.get("V口振支店名", "")).strip()
            converted_row["回収口座番号"] = str(row.get("V口振番号", "")).strip()
            converted_row["回収口座名義"] = str(row.get("V口座振込先", "")).strip()
            
            # 7. 特殊項目
            client_cd = self.get_client_cd(row.get("約定日", ""))
            converted_row["クライアントCD"] = client_cd
            converted_row["管理前滞納額"] = str(row.get("滞納額合計", "")).strip()
            
            # 管理会社処理：nan回避 + クライアントCD条件処理
            management_company = row.get("管理会社", "")
            if pd.isna(management_company) or str(management_company).strip().lower() == "nan":
                management_company = ""
            else:
                management_company = str(management_company).strip()
            
            # クライアントCDが"1"の場合は株式会社前田を設定
            if client_cd == "1":
                converted_row["管理会社"] = "株式会社前田"
            else:
                converted_row["管理会社"] = management_company
                
            converted_row["管理受託日"] = datetime.now().strftime("%Y/%m/%d")
            
            # 8. 固定値設定（すべて適用）
            for key, value in CapcoConfig.FIXED_VALUES.items():
                converted_row[key] = value
            
            output_data.append(converted_row)
        
        # 111列の正確な順序でDataFrame構築（空列対応）
        # 空列用の一意な仮名前を使用してから最後にリネーム
        temp_columns = []
        temp_data = {}
        empty_col_counter = 1
        
        for i, col in enumerate(CapcoConfig.OUTPUT_COLUMNS):
            if col == "":  # 空列の場合
                temp_col_name = f"__EMPTY_COL_{empty_col_counter}__"
                temp_columns.append(temp_col_name)
                temp_data[temp_col_name] = [""] * len(output_data)
                empty_col_counter += 1
            else:
                temp_columns.append(col)
                temp_data[col] = [row.get(col, "") for row in output_data]
        
        # DataFrameを一度に構築（パフォーマンス向上）
        final_df = pd.DataFrame(temp_data, columns=temp_columns)
        
        # 空列の仮名前を元に戻す
        final_df.columns = CapcoConfig.OUTPUT_COLUMNS
        
        return final_df


def process_capco_data(capco_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    カプコ新規登録データ処理メイン関数
    
    Args:
        capco_content: カプコ元データ.csvの内容
        contract_content: ContractList_*.csvの内容
        
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
        
        logs.append("=== カプコ新規登録処理開始 ===")
        logger.info("カプコ新規登録処理開始")
        
        # 1. データ読み込み
        data_loader = DataLoader()
        capco_df = data_loader.load_capco_data(capco_content)
        contract_df = data_loader.load_contract_list(contract_content)
        
        logs.append(f"ファイル読み込み完了: カプコ{len(capco_df)}件, ContractList{len(contract_df)}件")
        
        # 2. 重複チェック（新規案件抽出）
        duplicate_checker = DuplicateChecker()
        new_contracts, existing_contracts, match_stats = duplicate_checker.find_new_contracts(capco_df, contract_df)
        
        logs.append(f"重複チェック結果: 新規{match_stats['new_contracts']}件, 既存{match_stats['existing_contracts']}件")
        
        if len(new_contracts) == 0:
            logs.append("⚠️ 新規案件が見つかりませんでした")
            return pd.DataFrame(), logs, "no_new_contracts.csv"
        
        # 3. データ変換（111列テンプレート準拠）
        data_converter = DataConverter()
        output_df = data_converter.convert_new_contracts(new_contracts)
        
        logs.append(f"データ変換完了: {len(output_df)}件 → 111列テンプレート形式")
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{CapcoConfig.OUTPUT_FILE_PREFIX}.csv"
        
        # 5. 処理完了
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logs.append(f"=== 処理完了: {output_filename} ({processing_time:.2f}秒) ===")
        logger.info(f"カプコ処理完了: {len(output_df)}件出力")
        
        return output_df, logs, output_filename
        
    except FileNotFoundError as e:
        error_msg = f"ファイルが見つかりません: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(), [error_msg], "error.csv"
    except ValueError as e:
        error_msg = f"データ形式エラー: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(), [error_msg], "error.csv"
    except Exception as e:
        error_msg = f"カプコ処理エラー: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(), [error_msg], "error.csv"


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレート（111列完全準拠）"""
    return pd.DataFrame(columns=CapcoConfig.OUTPUT_COLUMNS)