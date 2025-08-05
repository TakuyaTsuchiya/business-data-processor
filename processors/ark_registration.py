#!/usr/bin/env python3
"""
アーク新規登録プロセッサ（0から完全再構築版）
正確な111列テンプレートヘッダー完全準拠

入力ファイル:
- 案件取込用レポート.csv
- ContractList_*.csv (122列)

出力ファイル:
- MMDDアーク新規登録.csv (111列) - 絶対変更禁止テンプレート準拠

重複チェック:
- 案件取込用レポート「契約番号」⟷ ContractList「引継番号」（0付与後）
"""

import pandas as pd
import io
import re
import chardet
import unicodedata
from datetime import datetime
from typing import Tuple, List, Dict, Union
import logging


class ArkConfig:
    """アーク新規登録設定・定数管理クラス"""
    
    OUTPUT_FILE_PREFIX = "アーク新規登録"
    
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
        
        # 回収口座関連
        "回収口座金融機関CD": "310",
        "回収口座金融機関名": "GMOあおぞらネット銀行",
        "回収口座種類": "普通",
        "回収口座名義": "アーク株式会社",
        
        # その他固定値
        "クライアントCD": "10",
        "水道代": "0",
        "更新契約手数料": "1",
        "退去済手数料": "0",
        "入居中滞納手数料": "0",
        "入居中正常手数料": "0",
        
        # 空白項目
        "契約者生年月日": "",
        "管理費": "",  # 仕様書通り空白
        "契約確認日": "", 
        "保証開始日": "",
        "パートナーCD": "", 
        "引落預金種別": "", 
        "登録フラグ": "",
        "管理会社": "",
        "委託先法人ID": "",
        
        # 勤務先情報（基本空白、一部入力データから取得）
        "契約者勤務先カナ": "", 
        "勤務先業種": "",
        "契約者勤務先郵便番号": "", 
        "契約者勤務先住所1": "", 
        "契約者勤務先住所2": "", 
        "契約者勤務先住所3": "",
        
        # 保証人2情報（全て空白）
        "保証人２氏名": "", "保証人２カナ": "", "保証人２契約者との関係": "", "保証人２生年月日": "",
        "保証人２郵便番号": "", "保証人２住所1": "", "保証人２住所2": "", "保証人２住所3": "",
        "保証人２TEL自宅": "", "保証人２TEL携帯": "",
        
        # 緊急連絡人2情報（全て空白）
        "緊急連絡人２氏名": "", "緊急連絡人２カナ": "", "緊急連絡人２契約者との関係": "",
        "緊急連絡人２郵便番号": "", "緊急連絡人２現住所1": "", "緊急連絡人２現住所2": "", "緊急連絡人２現住所3": "",
        "緊急連絡人２TEL自宅": "", "緊急連絡人２TEL携帯": "",
        
        # 引落・その他情報（全て空白）
        "保証入金日": "", "保証入金者": "",
        "引落銀行CD": "", "引落銀行名": "", "引落支店CD": "", "引落支店名": "",
        "引落口座番号": "", "引落口座名義": "", "解約日": ""
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
                # デバッグ情報をファイルに保存
                with open('debug_csv_loading.log', 'w', encoding='utf-8') as f:
                    f.write(f"CSV読み込み成功 (エンコーディング: {try_encoding})\n")
                    f.write(f"読み込み列数: {len(df.columns)}\n")
                    f.write(f"読み込み行数: {len(df)}\n")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if try_encoding == encodings_to_try[-1]:
                    raise ValueError(f"CSVファイルの読み込みに失敗: {str(e)}")
                continue
        
        raise ValueError("対応するエンコーディングが見つかりませんでした")
    
    def load_ark_report_data(self, file_content: bytes) -> pd.DataFrame:
        """案件取込用レポート読み込み"""
        df = self.load_csv_from_bytes(file_content)
        
        # DEBUG: 列名詳細確認（ログに出力）
        debug_logs = []
        debug_logs.append("DEBUG: 案件取込用レポート列名一覧:")
        for i, col in enumerate(df.columns, 1):
            debug_logs.append(f"  {i:2d}: '{col}' (len: {len(str(col))})")
        
        # 重要列の存在確認
        target_columns = ['名前2', '名前2（カナ）', '種別／続柄２', '生年月日2', '自宅住所2', '自宅TEL2', '携帯TEL2']
        debug_logs.append("DEBUG: 重要列の存在確認:")
        for col in target_columns:
            exists = col in df.columns
            debug_logs.append(f"  '{col}': {exists}")
        
        # 1行目データ確認（名前2関連のみ）
        if len(df) > 0:
            first_row = df.iloc[0]
            debug_logs.append("DEBUG: 1行目の名前2関連データ:")
            for col in ['名前2', '名前2（カナ）', '種別／続柄２']:
                value = first_row.get(col, 'NOT_FOUND')
                debug_logs.append(f"  '{col}': '{value}'")
        
        # デバッグログをファイルに保存（外部確認用）
        with open('debug_ark_columns.log', 'w', encoding='utf-8') as f:
            f.write('\n'.join(debug_logs))
        
        # 一部をprintで出力（開発時確認用）
        for log in debug_logs[:10]:  # 最初の10行のみ
            print(log)
        
        # 必須カラム確認
        required_columns = ["契約番号", "契約元帳: 主契約者"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"案件取込用レポートに必須カラムが不足: {missing_columns}")
        
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
    
    def find_new_contracts(self, report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
        """新規契約の特定（重複チェック）"""
        # キー項目を文字列に変換・正規化
        report_df = report_df.copy()
        report_df["契約番号"] = report_df["契約番号"].astype(str).str.strip()
        
        contract_df = contract_df.copy()
        contract_df["引継番号"] = contract_df["引継番号"].astype(str).str.strip()
        
        # 案件取込用レポートの契約番号に0を付与して引継番号生成
        report_df["引継番号_temp"] = "0" + report_df["契約番号"]
        
        # 既存契約番号セット作成
        existing_contract_numbers = set(contract_df["引継番号"].tolist())
        
        # 新規・既存を分離
        new_contracts = report_df[~report_df["引継番号_temp"].isin(existing_contract_numbers)].copy()
        existing_contracts = report_df[report_df["引継番号_temp"].isin(existing_contract_numbers)].copy()
        
        # 一時的な列を削除
        new_contracts = new_contracts.drop("引継番号_temp", axis=1, errors='ignore')
        existing_contracts = existing_contracts.drop("引継番号_temp", axis=1, errors='ignore')
        
        # 統計情報
        stats = {
            'total_reports': len(report_df),
            'new_contracts': len(new_contracts),
            'existing_contracts': len(existing_contracts),
            'new_percentage': (len(new_contracts) / len(report_df) * 100) if len(report_df) > 0 else 0
        }
        
        return new_contracts, existing_contracts, stats


class DataConverter:
    """データ変換クラス"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prefectures = ArkConfig.PREFECTURES
    
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
    
    def add_leading_zero(self, value: str) -> str:
        """引継番号生成（契約番号の先頭に0を付与）"""
        if pd.isna(value) or str(value).strip() == "":
            return ""
        return f"0{str(value).strip()}"
    
    def normalize_phone_number(self, value: str) -> str:
        """電話番号の正規化"""
        if pd.isna(value) or str(value).strip() == "":
            return ""
        
        phone = str(value).strip()
        
        # 全角数字を半角に変換
        phone = unicodedata.normalize('NFKC', phone)
        
        # 記号を統一
        phone = phone.replace("－", "-").replace("ー", "-").replace("‐", "-")
        phone = phone.replace("（", "(").replace("）", ")")
        
        # 不要な文字を除去
        phone = re.sub(r"[^\d\-\(\)]", "", phone)
        
        return phone
    
    def hankaku_to_zenkaku(self, text: str) -> str:
        """半角カナを全角カナに変換"""
        if not text:
            return ""
        return unicodedata.normalize('NFKC', text)
    
    def extract_postal_code(self, address: str) -> Tuple[str, str]:
        """住所から郵便番号を抽出"""
        if pd.isna(address) or str(address).strip() == "":
            return "", address
        
        addr = str(address).strip()
        
        # 郵便番号パターン（〒マーク有無、ハイフン有無に対応）
        postal_patterns = [
            r'〒?\s*(\d{3})-?(\d{4})',
            r'〒?\s*(\d{7})'
        ]
        
        for pattern in postal_patterns:
            match = re.search(pattern, addr)
            if match:
                if len(match.groups()) == 2:
                    postal = f"{match.group(1)}-{match.group(2)}"
                else:
                    postal = match.group(1)
                    if len(postal) == 7:
                        postal = f"{postal[:3]}-{postal[3:]}"
                
                # 郵便番号部分を削除
                addr = re.sub(pattern, '', addr).strip()
                return postal, addr
        
        return "", addr
    
    def split_address(self, address: str) -> Dict[str, str]:
        """住所を郵便番号、都道府県、市区町村、残り住所に分割（政令指定都市対応版）"""
        if pd.isna(address) or str(address).strip() == "":
            return {"postal_code": "", "prefecture": "", "city": "", "remaining": ""}
        
        # 郵便番号を抽出
        postal_code, addr = self.extract_postal_code(address)
        
        # 都道府県を検索
        prefecture = ""
        for pref in self.prefectures:
            if addr.startswith(pref):
                prefecture = pref
                addr = addr[len(pref):]
                break
        
        # 市区町村を抽出（改善版）
        city = ""
        
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
        
        # 一般的な市区町村パターン（政令指定都市対応版）
        if not city:
            # 政令指定都市パターン（○○市○○区）を優先処理
            designated_city_pattern = r'^([^市区町村]+?市[^市区町村]+?区)'
            match = re.match(designated_city_pattern, addr)
            if match:
                city = match.group(1)  # 「千葉市美浜区」全体を市区町村とする
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
            "postal_code": postal_code,
            "prefecture": prefecture,
            "city": city,
            "remaining": addr
        }
    
    def extract_room_from_property_name(self, property_name: str) -> Tuple[str, str]:
        """物件名から部屋番号を抽出"""
        if pd.isna(property_name) or str(property_name).strip() == "":
            return "", ""
        
        prop_name = str(property_name).strip()
        
        # 部屋番号パターンを検索（例：101号室、201号）
        room_patterns = [
            r'(\d+)号室?',
            r'([A-Z]?\d+)号',
            r'(\d{3,4})$'
        ]
        
        for pattern in room_patterns:
            match = re.search(pattern, prop_name)
            if match:
                room_num = match.group(1)
                clean_prop_name = re.sub(pattern, '', prop_name).strip()
                return clean_prop_name, room_num
        
        return prop_name, ""
    
    def normalize_room_number(self, value: str) -> str:
        """部屋番号の正規化（小数点除去）"""
        if pd.isna(value) or str(value).strip() == "":
            return ""
        
        room = str(value).strip()
        try:
            # 小数点がある場合は整数部分のみ取得
            if '.' in room:
                room = str(int(float(room)))
        except:
            pass
        
        return room
    
    def process_phone_numbers(self, home_tel: str, mobile_tel: str) -> Dict[str, str]:
        """電話番号処理（自宅TELのみの場合は携帯TELに移動）"""
        home = self.normalize_phone_number(home_tel)
        mobile = self.normalize_phone_number(mobile_tel)
        
        # 自宅TELのみの場合、携帯TELに移動
        if home and not mobile:
            return {"home": "", "mobile": home}
        
        return {"home": home, "mobile": mobile}
    
    def calculate_exit_procedure_fee(self, rent: str, management_fee: str, parking_fee: str) -> int:
        """退去手続き費用計算（最低70,000円）"""
        try:
            total = 0
            for fee in [rent, management_fee, parking_fee]:
                if pd.notna(fee) and str(fee).strip():
                    # カンマを除去して数値変換
                    clean_fee = str(fee).replace(',', '').replace('￥', '').strip()
                    if clean_fee.isdigit():
                        total += int(clean_fee)
            
            return max(total, 70000)
        except:
            return 70000
    
    def generate_takeover_info(self, move_in_date: str) -> str:
        """引継情報を生成"""
        if pd.isna(move_in_date) or str(move_in_date).strip() == "":
            formatted_date = ""
        else:
            formatted_date = str(move_in_date).strip()
        
        return f"●20日～25日頃に督促手数料2,750円or2,970円が加算されることあり。案内注意！！　●入居日：{formatted_date}"
    
    def process_guarantor_emergency(self, row: pd.Series) -> Dict[str, Dict[str, str]]:
        """保証人・緊急連絡人判定（シンプル版：名前2のみ処理）"""
        result = {
            "guarantor1": {},
            "emergency1": {}
        }
        
        # 名前2の処理（種別／続柄２で判定）
        relationship_type2 = self.safe_str_convert(row.get("種別／続柄２", ""))
        print(f"DEBUG: process_guarantor_emergency開始")
        print(f"DEBUG: 種別／続柄２の値: '{relationship_type2}'")
        
        if relationship_type2:
            name2 = self.remove_all_spaces(self.safe_str_convert(row.get("名前2", "")))
            print(f"DEBUG: 名前2の値: '{name2}'")
            
            # 名前2がある場合のみ処理
            if name2:
                name2_kana = self.remove_all_spaces(self.hankaku_to_zenkaku(self.safe_str_convert(row.get("名前2（カナ）", ""))))
                
                phone_result2 = self.process_phone_numbers(
                    row.get("自宅TEL2", ""),
                    row.get("携帯TEL2", "")
                )
                
                address2 = self.safe_str_convert(row.get("自宅住所2", ""))
                addr_parts2 = self.split_address(address2)
                
                common_data2 = {
                    "氏名": name2,
                    "カナ": name2_kana,
                    "生年月日": self.safe_str_convert(row.get("生年月日2", "")),
                    "続柄": "他",  # 固定値
                    "自宅TEL": phone_result2["home"],
                    "携帯TEL": phone_result2["mobile"],
                    "郵便番号": addr_parts2.get("postal_code", ""),
                    "住所1": addr_parts2.get("prefecture", ""),
                    "住所2": addr_parts2.get("city", ""),
                    "住所3": addr_parts2.get("remaining", "")
                }
                
                # 判定処理
                if "保証人" in relationship_type2:
                    result["guarantor1"] = common_data2
                elif "緊急連絡先" in relationship_type2:
                    result["emergency1"] = common_data2
        
        return result
    
    def convert_new_contracts(self, new_contracts_df: pd.DataFrame) -> pd.DataFrame:
        """新規契約データを111列テンプレートに変換"""
        output_data = []
        
        for _, row in new_contracts_df.iterrows():
            converted_row = {}
            
            # 1. 基本情報
            converted_row["引継番号"] = self.add_leading_zero(row.get("契約番号", ""))
            converted_row["契約者氏名"] = self.remove_all_spaces(self.safe_str_convert(row.get("契約元帳: 主契約者", "")))
            converted_row["契約者カナ"] = self.remove_all_spaces(self.hankaku_to_zenkaku(self.safe_str_convert(row.get("主契約者（カナ）", ""))))
            converted_row["契約者生年月日"] = self.safe_str_convert(row.get("生年月日1", ""))
            
            # 2. 電話番号処理
            phone_result = self.process_phone_numbers(
                row.get("自宅TEL1", ""),
                row.get("携帯TEL1", "")
            )
            converted_row["契約者TEL自宅"] = phone_result["home"]
            converted_row["契約者TEL携帯"] = phone_result["mobile"]
            
            # 3. 住所分割処理（契約者現住所は物件住所から取得）
            address_parts = self.split_address(self.safe_str_convert(row.get("物件住所", "")))
            
            # 契約者現住所
            converted_row["契約者現住所郵便番号"] = address_parts.get("postal_code", "")
            converted_row["契約者現住所1"] = address_parts["prefecture"]
            converted_row["契約者現住所2"] = address_parts["city"]
            
            # 契約者現住所3: 町村以下　全角スペース　物件名　全角スペース　部屋番号
            property_name = self.safe_str_convert(row.get("物件名", ""))
            room_number = self.safe_str_convert(row.get("部屋番号", ""))
            address3_parts = []
            if address_parts["remaining"]:
                address3_parts.append(address_parts["remaining"])
            if property_name:
                address3_parts.append(property_name)
            if room_number:
                address3_parts.append(room_number)
            converted_row["契約者現住所3"] = "　".join(address3_parts)  # 全角スペース結合
            
            # 4. 物件情報
            clean_prop_name, room_num = self.extract_room_from_property_name(property_name)
            converted_row["物件名"] = clean_prop_name
            
            # 部屋番号（物件名から抽出、または既存の部屋番号列）
            if room_num:
                converted_row["部屋番号"] = self.normalize_room_number(room_num)
            else:
                converted_row["部屋番号"] = self.normalize_room_number(room_number)
            
            # 物件住所（物件住所から取得）
            property_address = self.safe_str_convert(row.get("物件住所", ""))
            prop_addr_parts = self.split_address(property_address)
            converted_row["物件住所郵便番号"] = prop_addr_parts.get("postal_code", "")
            converted_row["物件住所1"] = prop_addr_parts["prefecture"]
            converted_row["物件住所2"] = prop_addr_parts["city"]
            converted_row["物件住所3"] = prop_addr_parts["remaining"]
            
            # 5. 引継情報生成
            move_in_date = self.safe_str_convert(row.get("入居日", ""))
            converted_row["引継情報"] = self.generate_takeover_info(move_in_date)
            
            # 6. 金額情報
            rent = self.safe_str_convert(row.get("賃料", "0")).replace(',', '')
            management_fee = self.safe_str_convert(row.get("管理共益費", "0")).replace(',', '')
            parking_fee = self.safe_str_convert(row.get("駐車場料金", "0")).replace(',', '')
            
            converted_row["月額賃料"] = rent
            converted_row["共益費"] = management_fee  # 管理共益費を共益費欄に設定
            converted_row["駐車場代"] = parking_fee
            converted_row["その他費用1"] = self.safe_str_convert(row.get("その他料金", "0"))
            converted_row["その他費用2"] = self.safe_str_convert(row.get("決済サービス料", "0"))
            converted_row["敷金"] = self.safe_str_convert(row.get("敷金", "0"))
            converted_row["礼金"] = self.safe_str_convert(row.get("礼金", "0"))
            
            # 7. 退去手続き費用計算
            exit_fee = self.calculate_exit_procedure_fee(rent, management_fee, parking_fee)
            converted_row["退去手続き（実費）"] = str(exit_fee)
            
            # 8. その他情報
            converted_row["管理前滞納額"] = self.safe_str_convert(row.get("未収金額合計", "0"))
            converted_row["契約者勤務先名"] = self.safe_str_convert(row.get("勤務先1", ""))
            converted_row["契約者勤務先TEL"] = self.safe_str_convert(row.get("勤務先TEL1", ""))
            
            # 9. 回収口座情報（バーチャル口座情報を含む）
            converted_row["回収口座支店名"] = self.safe_str_convert(row.get("バーチャル口座(支店)", ""))
            converted_row["回収口座番号"] = self.safe_str_convert(row.get("バーチャル口座(口座番号)", ""))
            converted_row["回収口座支店CD"] = ""  # 空
            
            # 10. 日付情報
            today = datetime.now().strftime("%Y/%m/%d")
            converted_row["管理受託日"] = today
            
            # 11. 保証人・緊急連絡人判定
            contact_info = self.process_guarantor_emergency(row)
            
            # 保証人１の設定
            if contact_info["guarantor1"]:
                g1 = contact_info["guarantor1"]
                converted_row["保証人１氏名"] = g1.get("氏名", "")
                converted_row["保証人１カナ"] = g1.get("カナ", "")
                converted_row["保証人１契約者との関係"] = g1.get("続柄", "")
                converted_row["保証人１生年月日"] = g1.get("生年月日", "")
                converted_row["保証人１TEL自宅"] = g1.get("自宅TEL", "")
                converted_row["保証人１TEL携帯"] = g1.get("携帯TEL", "")
                converted_row["保証人１郵便番号"] = g1.get("郵便番号", "")
                converted_row["保証人１住所1"] = g1.get("住所1", "")
                converted_row["保証人１住所2"] = g1.get("住所2", "")
                converted_row["保証人１住所3"] = g1.get("住所3", "")
            
            # 緊急連絡人１の設定
            if contact_info["emergency1"]:
                e1 = contact_info["emergency1"]
                converted_row["緊急連絡人１氏名"] = e1.get("氏名", "")
                converted_row["緊急連絡人１カナ"] = e1.get("カナ", "")
                converted_row["緊急連絡人１契約者との関係"] = e1.get("続柄", "")
                converted_row["緊急連絡人１郵便番号"] = e1.get("郵便番号", "")
                converted_row["緊急連絡人１現住所1"] = e1.get("住所1", "")
                converted_row["緊急連絡人１現住所2"] = e1.get("住所2", "")
                converted_row["緊急連絡人１現住所3"] = e1.get("住所3", "")
                converted_row["緊急連絡人１TEL自宅"] = e1.get("自宅TEL", "")
                converted_row["緊急連絡人１TEL携帯"] = e1.get("携帯TEL", "")
            
            # 12. 固定値設定（保証人・緊急連絡人以外のみ適用）
            for key, value in ArkConfig.FIXED_VALUES.items():
                # 既に設定済みの保証人１・緊急連絡人１データは上書きしない
                if key not in converted_row or converted_row[key] == "":
                    converted_row[key] = value
            
            output_data.append(converted_row)
        
        # 111列の正確な順序でDataFrame構築（空列対応）
        # 空列用の一意な仮名前を使用してから最後にリネーム
        temp_columns = []
        temp_data = {}
        empty_col_counter = 1
        
        for i, col in enumerate(ArkConfig.OUTPUT_COLUMNS):
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
        
        # 空列の仮名前を元の空文字列に戻す
        final_column_names = []
        for col in ArkConfig.OUTPUT_COLUMNS:
            final_column_names.append(col)  # 空文字列("")も含めてそのまま
        
        final_df.columns = final_column_names
        
        return final_df


def process_ark_data(report_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    アーク新規登録データ処理メイン関数
    
    Args:
        report_content: 案件取込用レポート.csvの内容
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
        
        logs.append("=== アーク新規登録処理開始 ===")
        logger.info("アーク新規登録処理開始")
        
        # 1. データ読み込み
        data_loader = DataLoader()
        report_df = data_loader.load_ark_report_data(report_content)
        contract_df = data_loader.load_contract_list(contract_content)
        
        logs.append(f"ファイル読み込み完了: 案件取込用レポート{len(report_df)}件, ContractList{len(contract_df)}件")
        
        # 2. 重複チェック（新規案件抽出）
        duplicate_checker = DuplicateChecker()
        new_contracts, existing_contracts, match_stats = duplicate_checker.find_new_contracts(report_df, contract_df)
        
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
        output_filename = f"{today_str}{ArkConfig.OUTPUT_FILE_PREFIX}.csv"
        
        # 5. 処理完了
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logs.append(f"=== 処理完了: {output_filename} ({processing_time:.2f}秒) ===")
        logger.info(f"アーク処理完了: {len(output_df)}件出力")
        
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
        error_msg = f"アーク処理エラー: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(), [error_msg], "error.csv"


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレート（111列完全準拠）"""
    return pd.DataFrame(columns=ArkConfig.OUTPUT_COLUMNS)