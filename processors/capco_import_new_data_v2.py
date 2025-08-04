"""
CAPCO新規登録データ処理プロセッサー（完全版）
業務マニュアル100%準拠 - 111列完全対応
"""

import pandas as pd
import io
import re
from datetime import datetime
from typing import Tuple, Optional, Dict, List


class CapcoConfig:
    """CAPCO処理の設定・定数管理クラス"""
    
    OUTPUT_FILE_PREFIX = "カプコ新規登録"
    
    # 111列完全定義（ContractInfoSample（final）.csv準拠）
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
        "", "", "", "登録フラグ"  # 空白列3つ + 登録フラグ
    ]
    
    # 空白設定列（マニュアル明示の37項目）
    EMPTY_COLUMNS = {
        "契約者生年月日", "契約確認日", "更新契約手数料", "保証開始日", "パートナーCD", 
        "引落預金種別", "登録フラグ",
        # 金額関連（9項目）
        "月額賃料", "管理費", "共益費", "水道代", "駐車場代", 
        "その他費用1", "その他費用2", "敷金", "礼金",
        # 勤務先情報（8項目）
        "契約者勤務先名", "契約者勤務先カナ", "契約者勤務先TEL", "勤務先業種", 
        "契約者勤務先郵便番号", "契約者勤務先住所1", "契約者勤務先住所2", "契約者勤務先住所3",
        # 保証人１情報（10項目）  
        "保証人１氏名", "保証人１カナ", "保証人１契約者との関係", "保証人１生年月日", 
        "保証人１郵便番号", "保証人１住所1", "保証人１住所2", "保証人１住所3", 
        "保証人１TEL自宅", "保証人１TEL携帯",
        # 保証人２情報（10項目）
        "保証人２氏名", "保証人２カナ", "保証人２契約者との関係", "保証人２生年月日", 
        "保証人２郵便番号", "保証人２住所1", "保証人２住所2", "保証人２住所3", 
        "保証人２TEL自宅", "保証人２TEL携帯",
        # 緊急連絡人１情報（8項目）
        "緊急連絡人１氏名", "緊急連絡人１カナ", "緊急連絡人１契約者との関係", 
        "緊急連絡人１郵便番号", "緊急連絡人１現住所1", "緊急連絡人１現住所2", "緊急連絡人１現住所3", 
        "緊急連絡人１TEL自宅", "緊急連絡人１TEL携帯",
        # 緊急連絡人２情報（8項目）
        "緊急連絡人２氏名", "緊急連絡人２カナ", "緊急連絡人２契約者との関係", 
        "緊急連絡人２郵便番号", "緊急連絡人２現住所1", "緊急連絡人２現住所2", "緊急連絡人２現住所3", 
        "緊急連絡人２TEL自宅", "緊急連絡人２TEL携帯"
    }
    
    # 固定値設定
    FIXED_VALUES = {
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算", 
        "受託状況": "契約中",
        "回収口座金融機関CD": "9",
        "回収口座種類": "普通",
        "契約種類": "バックレント",
        "退去済手数料": "25",
        "入居中滞納手数料": "0", 
        "入居中正常手数料": "0"
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
    
    # 支店コードマッピング
    BRANCH_CODES = {
        "中央支店": "763",
        "東海支店": "730"
    }
    
    # クライアントCD判定用（約定日マッピング）
    CLIENT_CD_MAPPING = {
        "1004-01-01": "1",  # 1004年1月1日 → 1
        "1005-01-01": "4"   # 1005年1月1日 → 4
    }


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def validate_input_files(capco_df: pd.DataFrame, contract_df: pd.DataFrame) -> List[str]:
    """入力ファイルの必須列存在チェック"""
    logs = []
    
    # カプコ元データの必須列チェック
    required_capco_columns = ["契約No", "契約者名", "建物名", "部屋名"]
    missing_capco = [col for col in required_capco_columns if col not in capco_df.columns]
    if missing_capco:
        raise ValueError(f"カプコ元データに必須列が不足: {missing_capco}")
    
    # ContractListの必須列チェック  
    required_contract_columns = ["引継番号"]
    missing_contract = [col for col in required_contract_columns if col not in contract_df.columns]
    if missing_contract:
        raise ValueError(f"ContractListに必須列が不足: {missing_contract}")
    
    logs.append(f"入力ファイル検証完了: カプコ{len(capco_df)}件, ContractList{len(contract_df)}件")
    return logs


def check_for_duplicates(capco_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """重複チェック（契約No ⟷ 引継番号での照合）"""
    logs = []
    original_count = len(capco_df)
    
    # ContractListの引継番号セット作成
    existing_ids = set()
    if "引継番号" in contract_df.columns:
        existing_ids = set(contract_df["引継番号"].dropna().astype(str))
    
    # カプコ元データの契約Noで重複チェック
    capco_df["契約No_str"] = capco_df["契約No"].astype(str)
    duplicates = capco_df[capco_df["契約No_str"].isin(existing_ids)]
    new_records = capco_df[~capco_df["契約No_str"].isin(existing_ids)]
    
    # 一時列削除
    new_records = new_records.drop("契約No_str", axis=1)
    
    logs.append(f"重複チェック結果: 元データ{original_count}件")
    logs.append(f"既存データと重複: {len(duplicates)}件（除外）")
    logs.append(f"新規登録対象: {len(new_records)}件")
    
    return new_records, logs


def normalize_name(name: str) -> str:
    """契約者氏名の正規化（スペース除去）"""
    if pd.isna(name) or str(name).strip() == "":
        return ""
    return str(name).strip().replace(" ", "").replace("　", "")


def normalize_kana(kana: str) -> str:
    """契約者カナの正規化（ひらがな→カタカナ変換、スペース除去）"""
    if pd.isna(kana) or str(kana).strip() == "":
        return ""
    
    # ひらがな→カタカナ変換（小文字も含む完全版）
    kana_str = str(kana).strip()
    hiragana_to_katakana = str.maketrans(
        # 通常文字 + 小文字 + 濁音・半濁音
        'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん'
        'ぁぃぅぇぉゃゅょっ'  # 小文字追加
        'がぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽ',
        'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン'
        'ァィゥェォャュョッ'  # 小文字カタカナ追加
        'ガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポ'
    )
    
    katakana_str = kana_str.translate(hiragana_to_katakana)
    return katakana_str.replace(" ", "").replace("　", "")


def process_phone_numbers(home_tel: str, mobile_tel: str) -> Tuple[str, str]:
    """電話番号処理（携帯優先ロジック）
    
    Returns:
        Tuple[str, str]: (自宅TEL, 携帯TEL)
    """
    home_clean = str(home_tel).strip() if pd.notna(home_tel) else ""
    mobile_clean = str(mobile_tel).strip() if pd.notna(mobile_tel) else ""
    
    # 携帯番号が空欄で自宅番号がある場合、自宅番号を携帯欄に移動
    if not mobile_clean and home_clean:
        return "", home_clean  # (自宅を空欄, 携帯に自宅番号)
    
    return home_clean, mobile_clean


def normalize_phone_number(phone: str) -> str:
    """電話番号の正規化（ハイフン保持、基本的な形式チェックのみ）"""
    if pd.isna(phone) or str(phone).strip() == "":
        return ""
    
    phone_clean = str(phone).strip()
    
    # 基本的な文字数チェック（ハイフンを除いた数字のみで10桁以上）
    digits_only = re.sub(r'[^\d]', '', phone_clean)
    if len(digits_only) < 10:
        return ""
    
    # 元の形式をそのまま返す（ハイフン保持）
    return phone_clean


def split_address(address: str) -> Dict[str, str]:
    """住所を都道府県、市区町村、残り住所に分割"""
    if pd.isna(address) or str(address).strip() == "":
        return {"prefecture": "", "city": "", "remaining": ""}
    
    addr = str(address).strip()
    
    # 都道府県を検索
    prefecture = ""
    for pref in CapcoConfig.PREFECTURES:
        if addr.startswith(pref):
            prefecture = pref
            addr = addr[len(pref):]
            break
    
    # 市区町村を抽出（政令指定都市の区も含む）
    city = ""
    
    # 政令指定都市の区を優先的にマッチ（例：横浜市港北区、千葉市美浜区）
    city_ward_pattern = r'([^市]*市[^区]*区)'
    match = re.search(city_ward_pattern, addr)
    if match:
        city = match.group(1)
        addr = addr[len(city):]
    else:
        # 通常の市区町村パターン
        city_patterns = ['市', '区', '町', '村']
        for pattern in city_patterns:
            match = re.search(f'([^{pattern}]*{pattern})', addr)
            if match:
                city = match.group(1)
                addr = addr[len(city):]
                break
    
    return {
        "prefecture": prefecture,
        "city": city, 
        "remaining": addr.strip()
    }


def generate_property_address_3(contractor_address_3: str, building_name: str, room_name: str) -> str:
    """物件住所3生成（契約者現住所3から物件名・部屋番号を除去）"""
    if pd.isna(contractor_address_3) or str(contractor_address_3).strip() == "":
        return ""
    
    address_3 = str(contractor_address_3).strip()
    
    # 建物名を除去
    if pd.notna(building_name) and str(building_name).strip():
        building_clean = str(building_name).strip()
        address_3 = address_3.replace(building_clean, "").strip()
    
    # 部屋名を除去
    if pd.notna(room_name) and str(room_name).strip():
        room_clean = str(room_name).strip()
        address_3 = address_3.replace(room_clean, "").strip()
    
    # 余分なスペースを削除
    address_3 = re.sub(r'\s+', ' ', address_3).strip()
    
    return address_3


def determine_branch_code(branch_name: str) -> str:
    """支店名から支店CD判定"""
    if pd.isna(branch_name):
        return ""
    
    branch_clean = str(branch_name).strip()
    return CapcoConfig.BRANCH_CODES.get(branch_clean, "")


def determine_client_cd(yakutei_date: str) -> str:
    """約定日からクライアントCD判定"""
    if pd.isna(yakutei_date) or str(yakutei_date).strip() == "":
        return ""
    
    date_str = str(yakutei_date).strip()
    
    # 日付フォーマットを標準化（YYYY-MM-DD形式に変換）
    try:
        # 様々な日付形式を試す
        date_formats = ['%Y/%m/%d', '%Y-%m-%d', '%Y年%m月%d日', '%Y%m%d']
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                formatted_date = date_obj.strftime('%Y-%m-%d')
                return CapcoConfig.CLIENT_CD_MAPPING.get(formatted_date, "")
            except:
                continue
    except:
        pass
    
    return ""


def normalize_amount(amount: str) -> str:
    """金額の正規化（カンマ・円記号除去）"""
    if pd.isna(amount) or str(amount).strip() == "":
        return ""
    
    amount_clean = str(amount).replace(',', '').replace('￥', '').replace('円', '').strip()
    try:
        if amount_clean.isdigit():
            return amount_clean
    except:
        pass
    
    return ""


def generate_takeover_info(contract_start: str) -> str:
    """引継情報生成（カプコ一括登録 ●保証開始日：{契約開始}）"""
    contract_start_clean = str(contract_start).strip() if pd.notna(contract_start) else ""
    return f"カプコ一括登録　●保証開始日：{contract_start_clean}"


def transform_capco_to_mirail_format(capco_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """カプコデータをミライル形式（111列）に変換"""
    logs = []
    
    if len(capco_df) == 0:
        logs.append("変換対象データがありません")
        return pd.DataFrame(columns=CapcoConfig.OUTPUT_COLUMNS), logs
    
    # 出力DataFrame初期化（111列）
    output_df = pd.DataFrame(index=range(len(capco_df)), columns=CapcoConfig.OUTPUT_COLUMNS)
    
    # 今日の日付
    today = datetime.now().strftime("%Y/%m/%d")
    
    logs.append(f"データ変換開始: {len(capco_df)}件")
    
    # pandas警告回避のため、コピーを明示的に作成
    output_df = output_df.copy()
    
    # 行ごとに変換処理
    for idx, (_, row) in enumerate(capco_df.iterrows()):
        try:
            # 1. 基本情報変換
            output_df.loc[idx, "引継番号"] = str(row.get("契約No", "")).strip()
            output_df.loc[idx, "契約者氏名"] = normalize_name(row.get("契約者名", ""))
            output_df.loc[idx, "契約者カナ"] = normalize_kana(row.get("契約者ふりがな", ""))
            
            # 2. 電話番号処理（携帯優先ロジック）
            home_tel, mobile_tel = process_phone_numbers(
                row.get("契約者：電話番号", ""), 
                row.get("契約者：携帯番号", "")
            )
            output_df.iloc[idx]["契約者TEL自宅"] = normalize_phone_number(home_tel)
            output_df.iloc[idx]["契約者TEL携帯"] = normalize_phone_number(mobile_tel)
            
            # 3. 住所処理（契約者現住所）
            building_address = str(row.get("建物：住所", "")).strip()
            output_df.iloc[idx]["契約者現住所郵便番号"] = str(row.get("建物：郵便番号", "")).strip()
            
            if building_address:
                addr_parts = split_address(building_address)
                output_df.iloc[idx]["契約者現住所1"] = addr_parts["prefecture"]
                output_df.iloc[idx]["契約者現住所2"] = addr_parts["city"]
                
                # 現住所3 = 残り住所 + 建物名 + 部屋名
                building_name = str(row.get("建物名", "")).strip()
                room_name = str(row.get("部屋名", "")).strip()
                
                address_3_parts = [addr_parts["remaining"]]
                if building_name: address_3_parts.append(building_name)
                if room_name: address_3_parts.append(room_name)
                
                output_df.iloc[idx]["契約者現住所3"] = " ".join(filter(None, address_3_parts))
            
            # 4. 物件情報
            output_df.iloc[idx]["物件名"] = str(row.get("建物名", "")).strip()
            output_df.iloc[idx]["部屋番号"] = str(row.get("部屋名", "")).strip()
            
            # 5. 物件住所（契約者住所をコピー）
            output_df.iloc[idx]["物件住所郵便番号"] = output_df.iloc[idx]["契約者現住所郵便番号"]
            output_df.iloc[idx]["物件住所1"] = output_df.iloc[idx]["契約者現住所1"]
            output_df.iloc[idx]["物件住所2"] = output_df.iloc[idx]["契約者現住所2"]
            
            # 物件住所3（物件名・部屋番号除去）
            output_df.iloc[idx]["物件住所3"] = generate_property_address_3(
                output_df.iloc[idx]["契約者現住所3"],
                str(row.get("建物名", "")),
                str(row.get("部屋名", ""))
            )
            
            # 6. 固定値設定
            for col, value in CapcoConfig.FIXED_VALUES.items():
                output_df.iloc[idx][col] = value
            
            # 7. 空白値設定
            for col in CapcoConfig.EMPTY_COLUMNS:
                output_df.iloc[idx][col] = ""
            
            # 8. 口座情報変換
            output_df.iloc[idx]["回収口座金融機関名"] = str(row.get("V口座銀行名", "")).strip()
            output_df.iloc[idx]["回収口座支店CD"] = determine_branch_code(row.get("V口振支店名", ""))
            output_df.iloc[idx]["回収口座支店名"] = str(row.get("V口振支店名", "")).strip()
            output_df.iloc[idx]["回収口座番号"] = str(row.get("V口振番号", "")).strip()
            output_df.iloc[idx]["回収口座名義"] = str(row.get("V口座振込先", "")).strip()
            
            # 9. クライアントCD判定
            output_df.iloc[idx]["クライアントCD"] = determine_client_cd(row.get("約定日", ""))
            
            # 10. 管理前滞納額
            output_df.iloc[idx]["管理前滞納額"] = normalize_amount(row.get("滞納額合計", ""))
            
            # 11. 管理会社（クライアントCD条件付き設定）
            client_cd = output_df.iloc[idx]["クライアントCD"]
            management_company = row.get("管理会社", "")
            
            if str(client_cd).strip() == "1":
                # クライアントCD = 1 の場合は「株式会社前田」に設定
                output_df.iloc[idx]["管理会社"] = "株式会社前田"
            elif pd.isna(management_company) or str(management_company).strip() == "":
                # クライアントCD ≠ 1 かつ 元データが空白の場合は空白のまま
                output_df.iloc[idx]["管理会社"] = ""
            else:
                # クライアントCD ≠ 1 かつ 元データに値がある場合はそのまま使用
                output_df.iloc[idx]["管理会社"] = str(management_company).strip()
            
            # 12. 計算値
            output_df.iloc[idx]["管理受託日"] = today
            output_df.iloc[idx]["引継情報"] = generate_takeover_info(row.get("契約開始", ""))
            
        except Exception as e:
            logs.append(f"行{idx+1}の変換でエラー: {str(e)}")
            continue
    
    logs.append(f"データ変換完了: {len(output_df)}件")
    return output_df, logs


def process_capco_import_new_data_v2(capco_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    CAPCO新規登録データ処理メイン関数（完全版）
    
    Args:
        capco_content: カプコ元データ.csvの内容
        contract_content: ContractList_*.csvの内容
        
    Returns:
        tuple: (変換済みDF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. ファイル読み込み
        logs.append("=== CAPCO新規登録処理開始 ===")
        capco_df = read_csv_auto_encoding(capco_content)
        contract_df = read_csv_auto_encoding(contract_content)
        
        # 2. 入力ファイル検証
        validation_logs = validate_input_files(capco_df, contract_df)
        logs.extend(validation_logs)
        
        # 3. 重複チェック
        new_capco_df, duplicate_logs = check_for_duplicates(capco_df, contract_df)
        logs.extend(duplicate_logs)
        
        # 4. データ変換（111列完全対応）
        output_df, transform_logs = transform_capco_to_mirail_format(new_capco_df)
        logs.extend(transform_logs)
        
        # 5. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{CapcoConfig.OUTPUT_FILE_PREFIX}.csv"
        
        logs.append(f"=== 処理完了: {output_filename} ===")
        
        return output_df, logs, output_filename
        
    except Exception as e:
        error_msg = f"CAPCO処理エラー: {str(e)}"
        return pd.DataFrame(), [error_msg], "error.csv"


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレート（111列完全版）"""
    return pd.DataFrame(columns=CapcoConfig.OUTPUT_COLUMNS)