"""
アーク新規登録データ処理プロセッサー（完全版）
GitHubの動作実績コード（111列フル仕様）を完全踏襲
"""

import pandas as pd
import io
import re
import unicodedata
from datetime import datetime
from typing import Tuple, Optional, Dict, List


class ArkConfig:
    """アーク処理の設定（GitHubフル仕様）"""
    
    OUTPUT_FILE_PREFIX = "アーク新規登録"
    
    # 111列の完全出力列定義（GitHubコードから完全複製）
    OUTPUT_COLUMNS = [
        "引継番号",
        "契約者氏名", 
        "契約者カナ",
        "契約者生年月日",
        "契約者TEL自宅",
        "契約者TEL携帯",
        "契約者現住所郵便番号",
        "契約者現住所1",
        "契約者現住所2", 
        "契約者現住所3",
        "引継情報",
        "物件名",
        "部屋番号",
        "物件住所郵便番号",
        "物件住所1",
        "物件住所2",
        "物件住所3",
        "入居ステータス",
        "滞納ステータス",
        "受託状況",
        "月額賃料",
        "管理費",
        "共益費", 
        "水道代",
        "駐車場代",
        "その他費用1",
        "その他費用2",
        "敷金",
        "礼金",
        "回収口座金融機関CD",
        "回収口座金融機関名",
        "回収口座支店CD",
        "回収口座支店名",
        "回収口座種類",
        "回収口座番号",
        "回収口座名義",
        "契約種類",
        "管理受託日",
        "申請者確認日",
        "更新契約手数料",
        "賃料積立金積立金",
        "入居積立金積立金",
        "管理前滞納額",
        "退去済手数料",
        "入居中滞納手数料",
        "入居中正常手数料",
        "退去手続き（実費）",
        "クライアントCD",
        "保証人1氏名",
        "保証人1カナ",
        "保証人1生年月日",
        "保証人1続柄",
        "保証人1TEL自宅",
        "保証人1TEL携帯",
        "保証人1現住所郵便番号",
        "保証人1現住所1",
        "保証人1現住所2",
        "保証人1現住所3",
        "保証人2氏名",
        "保証人2カナ",
        "保証人2生年月日",
        "保証人2続柄",
        "保証人2TEL自宅",
        "保証人2TEL携帯",
        "保証人2現住所郵便番号",
        "保証人2現住所1",
        "保証人2現住所2",
        "保証人2現住所3",
        "緊急連絡人1氏名",
        "緊急連絡人1カナ",
        "緊急連絡人1生年月日",
        "緊急連絡人1続柄",
        "緊急連絡人1TEL自宅",
        "緊急連絡人1TEL携帯",
        "緊急連絡人1現住所郵便番号",
        "緊急連絡人1現住所1",
        "緊急連絡人1現住所2",
        "緊急連絡人1現住所3",
        "緊急連絡人2氏名",
        "緊急連絡人2カナ",
        "緊急連絡人2生年月日",
        "緊急連絡人2続柄",
        "緊急連絡人2TEL自宅",
        "緊急連絡人2TEL携帯",
        "緊急連絡人2現住所郵便番号",
        "緊急連絡人2現住所1",
        "緊急連絡人2現住所2",
        "緊急連絡人2現住所3",
        "契約管理費積立金",
        "登録フラグ",
        "Unnamed: 81",
        "Unnamed: 82",
        "Unnamed: 83",
        "Unnamed: 84",
        "Unnamed: 85",
        "Unnamed: 86",
        "Unnamed: 87",
        "Unnamed: 88",
        "Unnamed: 89",
        "Unnamed: 90",
        "Unnamed: 91",
        "Unnamed: 92",
        "Unnamed: 93",
        "Unnamed: 94",
        "Unnamed: 95",
        "Unnamed: 96",
        "Unnamed: 97",
        "Unnamed: 98",
        "Unnamed: 99",
        "Unnamed: 100",
        "Unnamed: 101",
        "Unnamed: 102",
        "Unnamed: 103",
        "Unnamed: 104",
        "Unnamed: 105",
        "Unnamed: 106",
        "Unnamed: 107",
        "Unnamed: 108",
        "Unnamed: 109",
        "Unnamed: 110"
    ]
    
    # 固定値設定（GitHubコードから完全複製）
    FIXED_VALUES = {
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "回収口座金融機関CD": "310",
        "回収口座金融機関名": "GMOあおぞらネット銀行",
        "回収口座種類": "普通",
        "回収口座名義": "アーク株式会社",
        "契約種類": "バックレント",
        "クライアントCD": "10",
        "水道代": "0",
        "共益費": "0",
        "更新契約手数料": "1",
        "契約管理費積立金": "0",
        "賃料積立金積立金": "0",
        "入居積立金積立金": "0",
        "退去済手数料": "0",
        "入居中滞納手数料": "0",
        "入居中正常手数料": "0",
        "登録フラグ": ""
    }
    
    # 都道府県リスト
    PREFECTURES = [
        "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
        "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
        "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
        "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
        "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
        "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
        "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
    ]


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def safe_str_convert(value) -> str:
    """安全な文字列変換"""
    if pd.isna(value):
        return ""
    return str(value).strip()


def remove_all_spaces(text: str) -> str:
    """全てのスペースを除去"""
    if not text:
        return ""
    return text.replace(" ", "").replace("　", "")


def add_leading_zero(value: str) -> str:
    """引継番号生成（契約番号の先頭に0を付与）"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    return f"0{str(value).strip()}"


def normalize_phone_number(value: str) -> str:
    """電話番号の正規化（GitHubコード踏襲）"""
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


def hankaku_to_zenkaku(text: str) -> str:
    """半角カナを全角カナに変換"""
    if not text:
        return ""
    return unicodedata.normalize('NFKC', text)


def normalize_room_number(value: str) -> str:
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


def extract_postal_code(address: str) -> Tuple[str, str]:
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


def split_address(address: str) -> Dict[str, str]:
    """住所を郵便番号、都道府県、市区町村、残り住所に分割（政令指定都市対応版）"""
    if pd.isna(address) or str(address).strip() == "":
        return {"postal_code": "", "prefecture": "", "city": "", "remaining": ""}
    
    # 郵便番号を抽出
    postal_code, addr = extract_postal_code(address)
    
    # 都道府県を検索
    prefecture = ""
    for pref in ArkConfig.PREFECTURES:
        if addr.startswith(pref):
            prefecture = pref
            addr = addr[len(pref):]
            break
    
    # 市区町村を抽出（改善版）
    city = ""
    
    # 東京23区の特別処理（政令指定都市処理後に実行）
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


def extract_room_from_property_name(property_name: str) -> Tuple[str, str]:
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


def generate_takeover_info(move_in_date: str) -> str:
    """引継情報を生成（GitHubコード踏襲）"""
    if pd.isna(move_in_date) or str(move_in_date).strip() == "":
        formatted_date = ""
    else:
        try:
            date_str = str(move_in_date).strip()
            formatted_date = date_str
        except:
            formatted_date = str(move_in_date).strip()
    
    return f"●20日～25日頃に督促手数料2,750円or2,970円が加算されることあり。案内注意！！　●入居日：{formatted_date}"


def process_phone_numbers(home_tel: str, mobile_tel: str) -> Dict[str, str]:
    """電話番号処理（自宅TELのみの場合は携帯TELに移動）"""
    home = normalize_phone_number(home_tel)
    mobile = normalize_phone_number(mobile_tel)
    
    # 自宅TELのみの場合、携帯TELに移動
    if home and not mobile:
        return {"home": "", "mobile": home}
    
    return {"home": home, "mobile": mobile}


def calculate_exit_procedure_fee(rent: str, management_fee: str, parking_fee: str, other_fee: str) -> int:
    """退去手続き費用計算（最低70,000円）"""
    try:
        total = 0
        for fee in [rent, management_fee, parking_fee, other_fee]:
            if pd.notna(fee) and str(fee).strip():
                # カンマを除去して数値変換
                clean_fee = str(fee).replace(',', '').replace('￥', '').strip()
                if clean_fee.isdigit():
                    total += int(clean_fee)
        
        return max(total, 70000)
    except:
        return 70000


def process_guarantor_emergency_full(row: pd.Series) -> Dict[str, Dict[str, str]]:
    """
    保証人・緊急連絡人判定（111列フル仕様）
    
    注意: ContractListの実際の列名に合わせて修正が必要
    現在は仮の列名で実装されている
    """
    result = {
        "guarantor1": {},
        "guarantor2": {},
        "emergency1": {},
        "emergency2": {}
    }
    
    # 種別／続柄２で判定
    relationship_type = safe_str_convert(row.get("種別／続柄２", ""))
    
    # 名前、電話番号、住所などの共通処理
    name2 = remove_all_spaces(safe_str_convert(row.get("名前2", "")))
    name2_kana = remove_all_spaces(hankaku_to_zenkaku(safe_str_convert(row.get("名前2（カナ）", ""))))
    
    # 電話番号処理
    phone_result = process_phone_numbers(
        row.get("自宅TEL2", ""),
        row.get("携帯TEL2", "")
    )
    
    # 住所処理
    address2 = safe_str_convert(row.get("住所2", ""))
    addr_parts = split_address(address2)
    
    # 共通データ
    common_data = {
        "氏名": name2,
        "カナ": name2_kana,
        "生年月日": safe_str_convert(row.get("生年月日2", "")),
        "続柄": "他",  # デフォルト
        "自宅TEL": phone_result["home"],
        "携帯TEL": phone_result["mobile"],
        "郵便番号": addr_parts.get("postal_code", ""),
        "住所1": addr_parts.get("prefecture", ""),
        "住所2": addr_parts.get("city", ""),
        "住所3": addr_parts.get("remaining", "")
    }
    
    # 保証人判定
    if "保証人" in relationship_type:
        result["guarantor1"] = common_data
    # 緊急連絡人判定（"緊急連絡"または"(法)代表者１/"を含む場合）
    elif "緊急連絡" in relationship_type or "(法)代表者１/" in relationship_type:
        result["emergency1"] = common_data
    
    return result


def check_for_duplicates(report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """重複チェック（ContractListの引継番号との照合）"""
    logs = []
    
    if "引継番号" not in contract_df.columns:
        logs.append("ContractListに引継番号列がありません。重複チェックをスキップします。")
        return report_df, logs
    
    # 案件取込用レポートの引継番号を生成
    report_df["引継番号_temp"] = report_df["契約番号"].apply(add_leading_zero)
    
    # 重複している引継番号を特定
    existing_ids = set(contract_df["引継番号"].dropna().astype(str))
    duplicates = report_df[report_df["引継番号_temp"].isin(existing_ids)]
    
    if len(duplicates) > 0:
        logs.append(f"重複データ {len(duplicates)}件を除外しました")
        report_df = report_df[~report_df["引継番号_temp"].isin(existing_ids)]
    
    # 一時的な列を削除
    report_df = report_df.drop("引継番号_temp", axis=1)
    
    logs.append(f"重複チェック後: {len(report_df)}件")
    return report_df, logs


def get_template_headers() -> List[str]:
    """テンプレートヘッダーを取得（GitHubコード踏襲）"""
    # GitHubのようにテンプレートファイルから読み込むのが理想だが、
    # 現在はOUTPUT_COLUMNSを使用
    headers = ArkConfig.OUTPUT_COLUMNS.copy()
    
    # "Unnamed:"列を空文字に変換
    cleaned_headers = []
    for header in headers:
        if 'Unnamed:' in str(header):
            cleaned_headers.append("")
        else:
            cleaned_headers.append(header)
    
    return cleaned_headers


def transform_data_to_ark_format_full(report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """データをアーク新規登録形式に変換（111列フル仕様）"""
    logs = []
    
    # 重複チェック
    report_df, dup_logs = check_for_duplicates(report_df, contract_df)
    logs.extend(dup_logs)
    
    if len(report_df) == 0:
        logs.append("処理対象データがありません")
        return pd.DataFrame(), logs
    
    # 111列の完全出力DataFrame作成
    output_columns = get_template_headers()
    output_df = pd.DataFrame(index=range(len(report_df)), columns=output_columns)
    
    # 全列を空文字で初期化
    for col in output_columns:
        output_df[col] = ""
    
    # 基本データの変換
    guarantor_count = 0
    emergency_count = 0
    
    for idx, row in report_df.iterrows():
        output_idx = idx
        
        # 基本情報（列名を実際のCSVヘッダーに修正）
        output_df.loc[output_idx, "引継番号"] = add_leading_zero(row.get("契約番号", ""))
        output_df.loc[output_idx, "契約者氏名"] = remove_all_spaces(safe_str_convert(row.get("契約元帳: 主契約者", "")))
        output_df.loc[output_idx, "契約者カナ"] = remove_all_spaces(hankaku_to_zenkaku(safe_str_convert(row.get("主契約者（カナ）", ""))))
        output_df.loc[output_idx, "契約者生年月日"] = safe_str_convert(row.get("生年月日", ""))
        
        # 電話番号処理（実際の列名に修正）
        phone_result = process_phone_numbers(
            row.get("自宅TEL1", ""),
            row.get("携帯TEL1", "")
        )
        output_df.loc[output_idx, "契約者TEL自宅"] = phone_result["home"]
        output_df.loc[output_idx, "契約者TEL携帯"] = phone_result["mobile"]
        
        # 住所分割（契約者現住所は物件住所から取得）
        address1 = safe_str_convert(row.get("物件住所", ""))
        addr_parts = split_address(address1)
        output_df.loc[output_idx, "契約者現住所郵便番号"] = addr_parts.get("postal_code", "")
        output_df.loc[output_idx, "契約者現住所1"] = addr_parts["prefecture"]
        output_df.loc[output_idx, "契約者現住所2"] = addr_parts["city"]
        output_df.loc[output_idx, "契約者現住所3"] = addr_parts["remaining"]
        
        # 引継情報生成（GitHubコード踏襲）
        move_in_date = safe_str_convert(row.get("入居日", ""))
        takeover_info = generate_takeover_info(move_in_date)
        output_df.loc[output_idx, "引継情報"] = takeover_info
        
        # 物件情報
        property_name = safe_str_convert(row.get("物件名", ""))
        clean_prop_name, room_num = extract_room_from_property_name(property_name)
        output_df.loc[output_idx, "物件名"] = clean_prop_name
        
        # 部屋番号（物件名から抽出、または既存の部屋番号列）
        if room_num:
            output_df.loc[output_idx, "部屋番号"] = normalize_room_number(room_num)
        else:
            output_df.loc[output_idx, "部屋番号"] = normalize_room_number(row.get("部屋番号", ""))
        
        # 物件郵便番号（実際の列名に修正）
        output_df.loc[output_idx, "物件住所郵便番号"] = safe_str_convert(row.get("物件郵便番号", ""))
        
        # 物件住所（物件住所1から取得）
        property_address = safe_str_convert(row.get("物件住所1", ""))
        prop_addr_parts = split_address(property_address)
        output_df.loc[output_idx, "物件住所郵便番号"] = prop_addr_parts.get("postal_code", "")
        output_df.loc[output_idx, "物件住所1"] = prop_addr_parts["prefecture"]
        output_df.loc[output_idx, "物件住所2"] = prop_addr_parts["city"]
        output_df.loc[output_idx, "物件住所3"] = prop_addr_parts["remaining"]
        
        # 金額（実際の列名に修正）
        rent = safe_str_convert(row.get("賃料", "0")).replace(',', '')
        management_fee = safe_str_convert(row.get("管理共益費", "0")).replace(',', '')
        parking_fee = safe_str_convert(row.get("駐車場料金", "0")).replace(',', '')
        
        output_df.loc[output_idx, "月額賃料"] = rent
        output_df.loc[output_idx, "管理費"] = ""  # 仕様書の通り空白
        output_df.loc[output_idx, "共益費"] = management_fee  # 管理共益費を共益費欄に設定
        output_df.loc[output_idx, "駐車場代"] = parking_fee
        
        # その他費用（実際の列名に修正）
        output_df.loc[output_idx, "その他費用1"] = safe_str_convert(row.get("その他料金", "0"))
        output_df.loc[output_idx, "その他費用2"] = safe_str_convert(row.get("決済サービス料", "0"))
        output_df.loc[output_idx, "敷金"] = safe_str_convert(row.get("敷金", "0"))
        output_df.loc[output_idx, "礼金"] = safe_str_convert(row.get("礼金", "0"))
        
        # 退去手続き費用計算
        exit_fee = calculate_exit_procedure_fee(rent, management_fee, parking_fee, "0")
        output_df.loc[output_idx, "退去手続き（実費）"] = str(exit_fee)
        
        # 管理前滞納額（未収金額合計から取得）
        output_df.loc[output_idx, "管理前滞納額"] = safe_str_convert(row.get("未収金額合計", "0"))
        
        # 勤務先情報（実際の列名に対応）
        output_df.loc[output_idx, "契約者勤務先名"] = safe_str_convert(row.get("勤務偈1", ""))
        output_df.loc[output_idx, "契約者勤務先TEL"] = safe_str_convert(row.get("勤務先TEL1", ""))
        
        # 回収口座情報（バーチャル口座情報を含む）
        output_df.loc[output_idx, "回収口座金融機関CD"] = ArkConfig.FIXED_VALUES["回収口座金融機関CD"]
        output_df.loc[output_idx, "回収口座金融機関名"] = ArkConfig.FIXED_VALUES["回収口座金融機関名"]
        output_df.loc[output_idx, "回収口座支店CD"] = ""  # 空
        output_df.loc[output_idx, "回収口座支店名"] = safe_str_convert(row.get("バーチャル口座(支店)", ""))
        output_df.loc[output_idx, "回収口座種類"] = ArkConfig.FIXED_VALUES["回収口座種類"]
        output_df.loc[output_idx, "回収口座番号"] = safe_str_convert(row.get("バーチャル口座(口座番号)", ""))
        output_df.loc[output_idx, "回収口座名義"] = ArkConfig.FIXED_VALUES["回収口座名義"]
        
        # 固定値設定
        for fixed_key, fixed_value in ArkConfig.FIXED_VALUES.items():
            if fixed_key in output_df.columns:
                output_df.loc[output_idx, fixed_key] = fixed_value
        
        # 今日の日付
        today = datetime.now().strftime("%Y/%m/%d")
        output_df.loc[output_idx, "管理受託日"] = today
        output_df.loc[output_idx, "申請者確認日"] = today
        
        # 保証人・緊急連絡人判定（GitHubコード踏襲）
        contact_info = process_guarantor_emergency_full(row)
        
        # 保証人1の設定
        if contact_info["guarantor1"]:
            guarantor_count += 1
            g1 = contact_info["guarantor1"]
            output_df.loc[output_idx, "保証人1氏名"] = g1.get("氏名", "")
            output_df.loc[output_idx, "保証人1カナ"] = g1.get("カナ", "")
            output_df.loc[output_idx, "保証人1生年月日"] = g1.get("生年月日", "")
            output_df.loc[output_idx, "保証人1続柄"] = g1.get("続柄", "")
            output_df.loc[output_idx, "保証人1TEL自宅"] = g1.get("自宅TEL", "")
            output_df.loc[output_idx, "保証人1TEL携帯"] = g1.get("携帯TEL", "")
            output_df.loc[output_idx, "保証人1現住所郵便番号"] = g1.get("郵便番号", "")
            output_df.loc[output_idx, "保証人1現住所1"] = g1.get("住所1", "")
            output_df.loc[output_idx, "保証人1現住所2"] = g1.get("住所2", "")
            output_df.loc[output_idx, "保証人1現住所3"] = g1.get("住所3", "")
        
        # 緊急連絡人1の設定
        if contact_info["emergency1"]:
            emergency_count += 1
            e1 = contact_info["emergency1"]
            output_df.loc[output_idx, "緊急連絡人1氏名"] = e1.get("氏名", "")
            output_df.loc[output_idx, "緊急連絡人1カナ"] = e1.get("カナ", "")
            output_df.loc[output_idx, "緊急連絡人1生年月日"] = e1.get("生年月日", "")
            output_df.loc[output_idx, "緊急連絡人1続柄"] = e1.get("続柄", "")
            output_df.loc[output_idx, "緊急連絡人1TEL自宅"] = e1.get("自宅TEL", "")
            output_df.loc[output_idx, "緊急連絡人1TEL携帯"] = e1.get("携帯TEL", "")
            output_df.loc[output_idx, "緊急連絡人1現住所郵便番号"] = e1.get("郵便番号", "")
            output_df.loc[output_idx, "緊急連絡人1現住所1"] = e1.get("住所1", "")
            output_df.loc[output_idx, "緊急連絡人1現住所2"] = e1.get("住所2", "")
            output_df.loc[output_idx, "緊急連絡人1現住所3"] = e1.get("住所3", "")
    
    logs.append(f"保証人: {guarantor_count}人, 緊急連絡人: {emergency_count}人")
    logs.append(f"データ変換完了: {len(output_df)}件")
    return output_df, logs


def process_ark_data(report_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, list, str]:
    """
    アーク新規登録データの処理メイン関数（111列フル仕様）
    
    Args:
        report_content: 案件取込用レポートのファイル内容
        contract_content: ContractListのファイル内容
        
    Returns:
        tuple: (変換済みDF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み
        logs.append("ファイル読み込み開始")
        report_df = read_csv_auto_encoding(report_content)
        contract_df = read_csv_auto_encoding(contract_content)
        
        logs.append(f"案件取込用レポート: {len(report_df)}件")
        logs.append(f"ContractList: {len(contract_df)}件")
        
        # 2. 完全版データ変換（111列）
        output_df, transform_logs = transform_data_to_ark_format_full(report_df, contract_df)
        logs.extend(transform_logs)
        
        # 3. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{ArkConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return output_df, logs, output_filename
        
    except Exception as e:
        raise Exception(f"アークデータ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（111列フル仕様）"""
    return pd.DataFrame(columns=get_template_headers())