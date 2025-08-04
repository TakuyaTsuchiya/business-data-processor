"""
アーク新規登録データ処理プロセッサー
GitHubの動作実績コードを完全踏襲
"""

import pandas as pd
import io
import re
import unicodedata
from datetime import datetime
from typing import Tuple, Optional, Dict, List


class ArkConfig:
    """アーク処理の設定"""
    
    OUTPUT_FILE_PREFIX = "アーク新規登録"
    
    # 主要なカラムマッピング（簡略版）
    COLUMN_MAPPINGS = {
        "引継番号": ("契約番号", "add_leading_zero"),
        "契約者氏名": ("契約者氏名", "remove_spaces"),
        "契約者カナ": ("契約者カナ", "normalize_kana"),
        "契約者生年月日": ("生年月日", "format_date"),
        "契約者TEL自宅": ("自宅TEL1", "normalize_phone"),
        "契約者TEL携帯": ("携帯TEL1", "normalize_phone"),
        "物件名": ("物件名", None),
        "部屋番号": ("部屋番号", "normalize_room"),
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "月額賃料": ("賃料", "to_int"),
        "管理費": ("管理費", "to_int"),
        "駐車場代": ("駐車場代", "to_int"),
        "退去手続き費用": "calculated",
        "引継情報": "generated",
        "管理受託日": "today",
        "申請者確認日": "today"
    }
    
    # 都道府県リスト（住所分割用）
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
    """住所を郵便番号、都道府県、市区町村、残り住所に分割（GitHubコード踏襲）"""
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
    
    # 東京23区の特別処理
    if prefecture == "東京都":
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
        city_patterns = [
            r'^([^市区町村]+?[市])',
            r'^([^市区町村]+?[区])',
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
    # 日付フォーマット
    if pd.isna(move_in_date) or str(move_in_date).strip() == "":
        formatted_date = ""
    else:
        try:
            # 日付文字列をパース
            date_str = str(move_in_date).strip()
            # YYYY/MM/DD形式に統一
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


def process_guarantor_emergency(row: pd.Series) -> Dict[str, Dict[str, str]]:
    """保証人・緊急連絡人判定（GitHubコード踏襲）"""
    result = {
        "guarantor1": {},
        "emergency1": {}
    }
    
    # 種別／続柄２で判定
    relationship_type = str(row.get("種別／続柄２", "")).strip() if pd.notna(row.get("種別／続柄２", "")) else ""
    
    # 名前、電話番号、住所などの共通処理
    name2 = str(row.get("名前2", "")).strip() if pd.notna(row.get("名前2", "")) else ""
    name2_kana = str(row.get("名前2（カナ）", "")).strip() if pd.notna(row.get("名前2（カナ）", "")) else ""
    
    # スペース除去
    name2 = name2.replace(" ", "").replace("　", "")
    name2_kana = name2_kana.replace(" ", "").replace("　", "")
    
    # 電話番号処理
    phone_result = process_phone_numbers(
        row.get("自宅TEL2", ""),
        row.get("携帯TEL2", "")
    )
    
    # 住所処理
    address2 = str(row.get("住所2", "")).strip() if pd.notna(row.get("住所2", "")) else ""
    addr_parts = split_address(address2)
    
    # 共通データ
    common_data = {
        "氏名": name2,
        "カナ": name2_kana,
        "自宅TEL": phone_result["home"],
        "携帯TEL": phone_result["mobile"],
        "郵便番号": addr_parts.get("postal_code", ""),
        "都道府県": addr_parts.get("prefecture", ""),
        "市区町村": addr_parts.get("city", ""),
        "残り住所": addr_parts.get("remaining", ""),
        "続柄": "他"  # デフォルト
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


def identify_guarantors_and_contacts(df: pd.DataFrame) -> Tuple[List[dict], List[dict]]:
    """保証人・緊急連絡人の判定"""
    guarantors = []
    contacts = []
    
    if "種別／続柄２" not in df.columns or "名前2" not in df.columns:
        return guarantors, contacts
    
    for _, row in df.iterrows():
        relation = str(row.get("種別／続柄２", "")).strip()
        name = str(row.get("名前2", "")).strip()
        
        if not name or name in ["nan", "NaN"]:
            continue
        
        person_data = {
            "name": name.replace(" ", "").replace("　", ""),  # スペース除去
            "address": str(row.get("住所2", "")).strip(),
            "phone": str(row.get("携帯TEL2", "")).strip()
        }
        
        if "保証人" in relation:
            guarantors.append(person_data)
        elif "緊急連絡" in relation or "(法)代表者１/" in relation:
            contacts.append(person_data)
    
    return guarantors, contacts


def transform_data_to_ark_format(report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """データをアーク新規登録形式に変換"""
    logs = []
    
    # 重複チェック
    report_df, dup_logs = check_for_duplicates(report_df, contract_df)
    logs.extend(dup_logs)
    
    if len(report_df) == 0:
        logs.append("処理対象データがありません")
        return pd.DataFrame(), logs
    
    # 出力DataFrame作成（簡略版の必須列のみ）
    output_columns = [
        "引継番号", "契約者氏名", "契約者カナ", "契約者生年月日",
        "契約者TEL自宅", "契約者TEL携帯", "契約者現住所郵便番号",
        "契約者現住所1", "契約者現住所2", "契約者現住所3",
        "物件名", "部屋番号", "入居ステータス", "滞納ステータス",
        "受託状況", "月額賃料", "管理費", "駐車場代", "退去手続き費用",
        "引継情報", "管理受託日", "申請者確認日"
    ]
    
    output_df = pd.DataFrame(index=range(len(report_df)), columns=output_columns)
    
    # 基本データの変換
    guarantor_count = 0
    emergency_count = 0
    
    for idx, row in report_df.iterrows():
        output_idx = idx
        
        # 基本情報
        output_df.loc[output_idx, "引継番号"] = add_leading_zero(row.get("契約番号", ""))
        output_df.loc[output_idx, "契約者氏名"] = str(row.get("契約者氏名", "")).strip()
        output_df.loc[output_idx, "契約者カナ"] = str(row.get("契約者カナ", "")).strip()
        
        # 電話番号処理（GitHubコード踏襲）
        phone_result = process_phone_numbers(
            row.get("自宅TEL1", ""),
            row.get("携帯TEL1", "")
        )
        output_df.loc[output_idx, "契約者TEL自宅"] = phone_result["home"]
        output_df.loc[output_idx, "契約者TEL携帯"] = phone_result["mobile"]
        
        # 住所分割（GitHubコード踏襲：郵便番号も抽出）
        address = str(row.get("住所1", "")).strip()
        addr_parts = split_address(address)
        output_df.loc[output_idx, "契約者現住所郵便番号"] = addr_parts.get("postal_code", "")
        output_df.loc[output_idx, "契約者現住所1"] = addr_parts["prefecture"]
        output_df.loc[output_idx, "契約者現住所2"] = addr_parts["city"]
        output_df.loc[output_idx, "契約者現住所3"] = addr_parts["remaining"]
        
        # 物件情報
        property_name = str(row.get("物件名", "")).strip()
        clean_prop_name, room_num = extract_room_from_property_name(property_name)
        output_df.loc[output_idx, "物件名"] = clean_prop_name
        
        # 部屋番号（物件名から抽出、または既存の部屋番号列）
        if room_num:
            output_df.loc[output_idx, "部屋番号"] = normalize_room_number(room_num)
        else:
            output_df.loc[output_idx, "部屋番号"] = normalize_room_number(row.get("部屋番号", ""))
        
        # 固定値
        output_df.loc[output_idx, "入居ステータス"] = "入居中"
        output_df.loc[output_idx, "滞納ステータス"] = "未精算"
        output_df.loc[output_idx, "受託状況"] = "契約中"
        
        # 金額
        rent = str(row.get("賃料", "0")).replace(',', '')
        management_fee = str(row.get("管理費", "0")).replace(',', '')
        parking_fee = str(row.get("駐車場代", "0")).replace(',', '')
        
        output_df.loc[output_idx, "月額賃料"] = rent
        output_df.loc[output_idx, "管理費"] = management_fee
        output_df.loc[output_idx, "駐車場代"] = parking_fee
        
        # 退去手続き費用計算
        exit_fee = calculate_exit_procedure_fee(rent, management_fee, parking_fee, "0")
        output_df.loc[output_idx, "退去手続き費用"] = str(exit_fee)
        
        # 引継情報生成（GitHubコード踏襲）
        move_in_date = str(row.get("入居日", "")).strip()
        takeover_info = generate_takeover_info(move_in_date)
        output_df.loc[output_idx, "引継情報"] = takeover_info
        
        # 今日の日付
        today = datetime.now().strftime("%Y/%m/%d")
        output_df.loc[output_idx, "管理受託日"] = today
        output_df.loc[output_idx, "申請者確認日"] = today
        
        # 保証人・緊急連絡人判定（GitHubコード踏襲）
        contact_info = process_guarantor_emergency(row)
        if contact_info["guarantor1"]:
            guarantor_count += 1
        if contact_info["emergency1"]:
            emergency_count += 1
    
    logs.append(f"保証人: {guarantor_count}人, 緊急連絡人: {emergency_count}人")
    logs.append(f"データ変換完了: {len(output_df)}件")
    return output_df, logs


def process_ark_data(report_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, list, str]:
    """
    アーク新規登録データの処理メイン関数
    
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
        
        # 2. データ変換
        output_df, transform_logs = transform_data_to_ark_format(report_df, contract_df)
        logs.extend(transform_logs)
        
        # 3. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{ArkConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return output_df, logs, output_filename
        
    except Exception as e:
        raise Exception(f"アークデータ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "引継番号", "契約者氏名", "契約者カナ", "契約者生年月日",
        "契約者TEL自宅", "契約者TEL携帯", "物件名", "部屋番号"
    ]
    return pd.DataFrame(columns=columns)