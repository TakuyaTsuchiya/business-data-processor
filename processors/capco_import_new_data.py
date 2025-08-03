"""
カプコ新規登録データ処理プロセッサー
アーク新規登録と同様の2ファイル処理構造を持つ
"""

import pandas as pd
import io
import re
from datetime import datetime
from typing import Tuple, Optional, Dict, List


class CapcoConfig:
    """カプコ処理の設定"""
    
    OUTPUT_FILE_PREFIX = "カプコ新規登録"
    
    # カプコ特有のカラムマッピング
    COLUMN_MAPPINGS = {
        "管理番号": ("契約番号", "add_prefix"),
        "契約者名": ("契約者氏名", "normalize_name"),
        "契約者カナ": ("契約者カナ", "normalize_kana"),
        "契約者生年月日": ("生年月日", "format_date"),
        "契約者電話番号": ("TEL", "normalize_phone"),
        "契約者携帯番号": ("携帯TEL", "normalize_phone"),
        "物件名称": ("物件名", None),
        "部屋番号": ("部屋番号", "normalize_room"),
        "契約状態": "契約中",
        "滞納状態": "未精算",
        "月額家賃": ("賃料", "to_int"),
        "共益費": ("管理費", "to_int"),
        "駐車場料金": ("駐車場代", "to_int"),
        "処理費用": "calculated",
        "備考情報": "generated",
        "登録日": "today",
        "確認日": "today"
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


def add_capco_prefix(value: str) -> str:
    """管理番号生成（カプコ特有のプレフィックス追加）"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    return f"CAP-{str(value).strip()}"


def normalize_name(value: str) -> str:
    """氏名の正規化（スペース除去）"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    return str(value).strip().replace(" ", "").replace("　", "")


def normalize_kana(value: str) -> str:
    """カナの正規化"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    # 半角カナを全角カナに変換する処理などを追加可能
    return str(value).strip().replace(" ", "").replace("　", "")


def normalize_phone_number(value: str) -> str:
    """電話番号の正規化"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    
    phone = str(value).strip()
    # ハイフンを除去
    phone = re.sub(r'[-−ー]', '', phone)
    # 数字以外を除去
    phone = re.sub(r'[^\d]', '', phone)
    
    return phone if len(phone) >= 10 else ""


def normalize_room_number(value: str) -> str:
    """部屋番号の正規化"""
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


def format_date(value: str) -> str:
    """日付のフォーマット統一"""
    if pd.isna(value) or str(value).strip() == "":
        return ""
    
    try:
        # 様々な日付形式を試す
        date_formats = ['%Y/%m/%d', '%Y-%m-%d', '%Y年%m月%d日', '%Y%m%d']
        date_str = str(value).strip()
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime('%Y/%m/%d')
            except:
                continue
        
        return date_str  # 変換できない場合は元の値を返す
    except:
        return str(value).strip()


def to_int_value(value: str) -> str:
    """金額を整数値に変換"""
    if pd.isna(value) or str(value).strip() == "":
        return "0"
    
    try:
        # カンマと円記号を除去
        clean_value = str(value).replace(',', '').replace('￥', '').replace('円', '').strip()
        if clean_value.isdigit():
            return clean_value
        return "0"
    except:
        return "0"


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
    
    # 市区町村を抽出
    city = ""
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
        "remaining": addr
    }


def calculate_processing_fee(rent: str, management_fee: str, parking_fee: str) -> int:
    """処理費用計算（カプコ特有のルール）"""
    try:
        total = 0
        for fee in [rent, management_fee, parking_fee]:
            if pd.notna(fee) and str(fee).strip():
                clean_fee = str(fee).replace(',', '').replace('￥', '').strip()
                if clean_fee.isdigit():
                    total += int(clean_fee)
        
        # カプコ特有の計算ルール（例：最低50,000円）
        return max(total, 50000)
    except:
        return 50000


def check_for_duplicates(report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """重複チェック（ContractListとの照合）"""
    logs = []
    
    # カプコでは管理番号で重複チェック
    if "管理番号" not in contract_df.columns:
        logs.append("ContractListに管理番号列がありません。重複チェックをスキップします。")
        return report_df, logs
    
    # レポートの管理番号を生成
    report_df["管理番号_temp"] = report_df["契約番号"].apply(add_capco_prefix)
    
    # 重複している管理番号を特定
    existing_ids = set(contract_df["管理番号"].dropna().astype(str))
    duplicates = report_df[report_df["管理番号_temp"].isin(existing_ids)]
    
    if len(duplicates) > 0:
        logs.append(f"重複データ {len(duplicates)}件を除外しました")
        report_df = report_df[~report_df["管理番号_temp"].isin(existing_ids)]
    
    # 一時的な列を削除
    report_df = report_df.drop("管理番号_temp", axis=1)
    
    logs.append(f"重複チェック後: {len(report_df)}件")
    return report_df, logs


def transform_data_to_capco_format(report_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """データをカプコ新規登録形式に変換"""
    logs = []
    
    # 重複チェック
    report_df, dup_logs = check_for_duplicates(report_df, contract_df)
    logs.extend(dup_logs)
    
    if len(report_df) == 0:
        logs.append("処理対象データがありません")
        return pd.DataFrame(), logs
    
    # 出力DataFrame作成
    output_columns = [
        "管理番号", "契約者名", "契約者カナ", "契約者生年月日",
        "契約者電話番号", "契約者携帯番号", "契約者郵便番号",
        "契約者住所1", "契約者住所2", "契約者住所3",
        "物件名称", "部屋番号", "契約状態", "滞納状態",
        "月額家賃", "共益費", "駐車場料金", "処理費用",
        "備考情報", "登録日", "確認日"
    ]
    
    output_df = pd.DataFrame(index=range(len(report_df)), columns=output_columns)
    
    # データ変換
    for idx, row in report_df.iterrows():
        output_idx = idx
        
        # 基本情報
        output_df.loc[output_idx, "管理番号"] = add_capco_prefix(row.get("契約番号", ""))
        output_df.loc[output_idx, "契約者名"] = normalize_name(row.get("契約者氏名", ""))
        output_df.loc[output_idx, "契約者カナ"] = normalize_kana(row.get("契約者カナ", ""))
        output_df.loc[output_idx, "契約者生年月日"] = format_date(row.get("生年月日", ""))
        
        # 電話番号処理
        output_df.loc[output_idx, "契約者電話番号"] = normalize_phone_number(row.get("TEL", ""))
        output_df.loc[output_idx, "契約者携帯番号"] = normalize_phone_number(row.get("携帯TEL", ""))
        
        # 住所処理
        address = str(row.get("住所", "")).strip()
        if address:
            addr_parts = split_address(address)
            output_df.loc[output_idx, "契約者住所1"] = addr_parts["prefecture"]
            output_df.loc[output_idx, "契約者住所2"] = addr_parts["city"]
            output_df.loc[output_idx, "契約者住所3"] = addr_parts["remaining"]
        
        # 郵便番号
        output_df.loc[output_idx, "契約者郵便番号"] = str(row.get("郵便番号", "")).strip()
        
        # 物件情報
        output_df.loc[output_idx, "物件名称"] = str(row.get("物件名", "")).strip()
        output_df.loc[output_idx, "部屋番号"] = normalize_room_number(row.get("部屋番号", ""))
        
        # 固定値
        output_df.loc[output_idx, "契約状態"] = "契約中"
        output_df.loc[output_idx, "滞納状態"] = "未精算"
        
        # 金額
        rent = to_int_value(row.get("賃料", "0"))
        management_fee = to_int_value(row.get("管理費", "0"))
        parking_fee = to_int_value(row.get("駐車場代", "0"))
        
        output_df.loc[output_idx, "月額家賃"] = rent
        output_df.loc[output_idx, "共益費"] = management_fee
        output_df.loc[output_idx, "駐車場料金"] = parking_fee
        
        # 処理費用計算
        processing_fee = calculate_processing_fee(rent, management_fee, parking_fee)
        output_df.loc[output_idx, "処理費用"] = str(processing_fee)
        
        # 備考情報生成
        contract_date = str(row.get("契約日", "")).strip()
        memo_info = f"カプコシステムより移行。契約日: {contract_date}"
        output_df.loc[output_idx, "備考情報"] = memo_info
        
        # 日付
        today = datetime.now().strftime("%Y/%m/%d")
        output_df.loc[output_idx, "登録日"] = today
        output_df.loc[output_idx, "確認日"] = today
    
    logs.append(f"データ変換完了: {len(output_df)}件")
    return output_df, logs


def process_capco_import_new_data(report_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, list, str]:
    """
    カプコ新規登録データの処理メイン関数
    
    Args:
        report_content: カプコレポートファイルの内容
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
        
        logs.append(f"カプコレポート: {len(report_df)}件")
        logs.append(f"ContractList: {len(contract_df)}件")
        
        # 2. データ変換
        output_df, transform_logs = transform_data_to_capco_format(report_df, contract_df)
        logs.extend(transform_logs)
        
        # 3. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{CapcoConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return output_df, logs, output_filename
        
    except Exception as e:
        raise Exception(f"カプコデータ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "管理番号", "契約者名", "契約者カナ", "契約者生年月日",
        "契約者電話番号", "契約者携帯番号", "物件名称", "部屋番号"
    ]
    return pd.DataFrame(columns=columns)