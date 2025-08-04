"""
CAPCO新規登録データ処理プロセッサー（完全版）
GitHubの動作実績コード（capco_import_new_data）を完全踏襲
"""

import pandas as pd
import io
import re
import unicodedata
from datetime import datetime
from typing import Tuple, Optional, Dict, List
import logging


class CapcoConfig:
    """CAPCO処理の設定・定数管理クラス（GitHub完全踏襲）"""
    
    OUTPUT_FILE_PREFIX = "カプコ新規登録"
    
    # GitHub完全踏襲 - 固定値設定
    FIXED_VALUES = {
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "契約種類": "バックレント",
        "回収口座種類": "普通",
        "退去済手数料": 25,
        "入居中滞納手数料": 0,
        "入居中正常手数料": 0,
        "回収口座金融機関CD": 9
    }
    
    # 支店コードマッピング（GitHub完全踏襲）
    BRANCH_CODE_MAPPING = {
        "中央支店": "763",
        "東海支店": "730"
    }
    
    # クライアントCDマッピング（GitHub完全踏襲）
    CLIENT_CD_MAPPING = {
        "1004-01-01": "1",
        "1005-01-01": "4"
    }
    
    # 47都道府県リスト（GitHub完全踏襲）
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
    """データ読み込みクラス（GitHub完全踏襲）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_encoding(self, file_content: bytes) -> str:
        """エンコーディング自動検出（GitHub完全踏襲）"""
        encodings_to_try = ['shift_jis', 'utf-8', 'cp932']
        
        for encoding in encodings_to_try:
            try:
                test_content = file_content[:1000]
                test_content.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        
        return 'cp932'
    
    def load_csv_from_bytes(self, file_content: bytes) -> pd.DataFrame:
        """バイト形式CSVファイル読み込み（GitHub完全踏襲）"""
        encoding = self.detect_encoding(file_content)
        
        try:
            df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, dtype=str)
            return df
        except Exception as e:
            raise ValueError(f"CSVファイルの読み込みに失敗: {str(e)}")
    
    def load_capco_data(self, file_content: bytes) -> pd.DataFrame:
        """カプコ元データ読み込み（GitHub完全踏襲）"""
        df = self.load_csv_from_bytes(file_content)
        
        # 必須カラム確認
        required_columns = ["契約No", "契約者名", "契約者ふりがな"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"カプコ元データに必須カラムが不足: {missing_columns}")
        
        return df
    
    def load_contract_lists(self, file_content: bytes) -> pd.DataFrame:
        """ContractList読み込み（GitHub完全踏襲）"""
        df = self.load_csv_from_bytes(file_content)
        
        if "引継番号" not in df.columns:
            raise ValueError("ContractListに'引継番号'カラムが見つかりません")
        
        # 重複除去
        df = df.drop_duplicates(subset=["引継番号"], keep='first')
        return df


class DataMatcher:
    """データマッチング・重複チェッククラス（GitHub完全踏襲）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def find_new_contracts(self, capco_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
        """新規契約の特定（GitHub完全踏襲）"""
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
    """データ変換クラス（GitHub完全踏襲）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.fixed_values = CapcoConfig.FIXED_VALUES
        self.branch_code_mapping = CapcoConfig.BRANCH_CODE_MAPPING
        self.client_cd_mapping = CapcoConfig.CLIENT_CD_MAPPING
        self.prefectures = CapcoConfig.PREFECTURES
    
    def _convert_name(self, name: str) -> str:
        """氏名変換（スペース除去）（GitHub完全踏襲）"""
        if pd.isna(name) or str(name).strip() == "":
            return ""
        return str(name).strip().replace(" ", "").replace("　", "")
    
    def _convert_kana(self, kana: str) -> str:
        """かな変換（ひらがな→カタカナ）（GitHub完全踏襲）"""
        if pd.isna(kana) or str(kana).strip() == "":
            return ""
        
        kana_str = str(kana).strip()
        
        # ひらがな→カタカナ変換
        hiragana = 'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをんがぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽぁぃぅぇぉゃゅょっ'
        katakana = 'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲンガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポァィゥェォャュョッ'
        
        kana_map = str.maketrans(hiragana, katakana)
        katakana_result = kana_str.translate(kana_map)
        
        # スペース除去
        return katakana_result.replace(" ", "").replace("　", "")
    
    def _process_phone_numbers(self, home_tel: str, mobile_tel: str) -> Tuple[str, str]:
        """電話番号処理（GitHub完全踏襲）"""
        home_clean = str(home_tel).strip() if pd.notna(home_tel) else ""
        mobile_clean = str(mobile_tel).strip() if pd.notna(mobile_tel) else ""
        
        # 携帯番号が空で自宅番号がある場合、自宅を携帯に移動
        if not mobile_clean and home_clean:
            return "", home_clean
        
        return home_clean, mobile_clean
    
    def _split_address(self, address: str) -> Dict[str, str]:
        """住所分割（GitHub完全踏襲）"""
        if pd.isna(address) or str(address).strip() == "":
            return {"prefecture": "", "city": "", "remaining": ""}
        
        addr = str(address).strip()
        
        # 都道府県抽出
        prefecture = ""
        for pref in self.prefectures:
            if addr.startswith(pref):
                prefecture = pref
                addr = addr[len(pref):]
                break
        
        # 市区町村抽出
        city = ""
        city_patterns = [r'([^市区町村]*[市区町村])']
        
        for pattern in city_patterns:
            match = re.search(pattern, addr)
            if match:
                city = match.group(1)
                addr = addr[len(city):]
                break
        
        return {
            "prefecture": prefecture,
            "city": city,
            "remaining": addr.strip()
        }
    
    def _create_takeover_info(self, contract_start: str) -> str:
        """引継情報生成（GitHub完全踏襲）"""
        contract_start_clean = str(contract_start).strip() if pd.notna(contract_start) else ""
        return f"カプコ一括登録　●保証開始日：{contract_start_clean}"
    
    def _get_branch_code(self, branch_name: str) -> str:
        """支店コード取得（GitHub完全踏襲）"""
        if pd.isna(branch_name):
            return ""
        return self.branch_code_mapping.get(str(branch_name).strip(), "")
    
    def _get_client_cd(self, yakutei_date: str) -> str:
        """クライアントコード取得（GitHub完全踏襲）"""
        if pd.isna(yakutei_date) or str(yakutei_date).strip() == "":
            return ""
        
        date_str = str(yakutei_date).strip()
        
        # 日付の正規化
        try:
            date_formats = ['%Y/%m/%d', '%Y-%m-%d', '%Y年%m月%d日']
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                    return self.client_cd_mapping.get(formatted_date, "")
                except ValueError:
                    continue
        except Exception:
            pass
        
        return ""
    
    def convert_to_mirail_format(self, capco_df: pd.DataFrame) -> pd.DataFrame:
        """カプコフォーマットからミライルフォーマットへ変換（GitHub完全踏襲）"""
        output_df = pd.DataFrame()
        
        for _, row in capco_df.iterrows():
            converted_row = {}
            
            # 基本情報
            converted_row["引継番号"] = str(row.get("契約No", "")).strip()
            converted_row["契約者氏名"] = self._convert_name(row.get("契約者名", ""))
            converted_row["契約者カナ"] = self._convert_kana(row.get("契約者ふりがな", ""))
            
            # 電話番号処理
            home_tel, mobile_tel = self._process_phone_numbers(
                row.get("契約者：電話番号", ""),
                row.get("契約者：携帯番号", "")
            )
            converted_row["契約者TEL自宅"] = home_tel
            converted_row["契約者TEL携帯"] = mobile_tel
            
            # 住所情報
            address_parts = self._split_address(row.get("建物：住所", ""))
            converted_row["契約者現住所郵便番号"] = str(row.get("建物：郵便番号", "")).strip()
            converted_row["契約者現住所1"] = address_parts["prefecture"]
            converted_row["契約者現住所2"] = address_parts["city"]
            
            # 現住所3 = 残り住所 + 建物名 + 部屋名
            building_name = str(row.get("建物名", "")).strip()
            room_name = str(row.get("部屋名", "")).strip()
            
            address_3_parts = [address_parts["remaining"]]
            if building_name: address_3_parts.append(building_name)
            if room_name: address_3_parts.append(room_name)
            converted_row["契約者現住所3"] = " ".join(filter(None, address_3_parts))
            
            # 物件情報
            converted_row["物件名"] = building_name
            converted_row["部屋番号"] = room_name
            
            # 物件住所（契約者住所と同じ）
            converted_row["物件住所郵便番号"] = converted_row["契約者現住所郵便番号"]
            converted_row["物件住所1"] = converted_row["契約者現住所1"]
            converted_row["物件住所2"] = converted_row["契約者現住所2"]
            converted_row["物件住所3"] = address_parts["remaining"]  # 建物名、部屋名は除外
            
            # 引継情報
            converted_row["引継情報"] = self._create_takeover_info(row.get("契約開始", ""))
            
            # 固定値設定
            for key, value in self.fixed_values.items():
                converted_row[key] = value
            
            # 口座情報
            converted_row["回収口座金融機関名"] = str(row.get("V口座銀行名", "")).strip()
            converted_row["回収口座支店CD"] = self._get_branch_code(row.get("V口振支店名", ""))
            converted_row["回収口座支店名"] = str(row.get("V口振支店名", "")).strip()
            converted_row["回収口座番号"] = str(row.get("V口振番号", "")).strip()
            converted_row["回収口座名義"] = str(row.get("V口座振込先", "")).strip()
            
            # クライアントCD
            client_cd = self._get_client_cd(row.get("約定日", ""))
            converted_row["クライアントCD"] = client_cd
            
            # 管理会社（クライアントCD条件付き）
            if client_cd == "1":
                converted_row["管理会社"] = "株式会社前田"
            else:
                management_company = row.get("管理会社", "")
                converted_row["管理会社"] = str(management_company).strip() if pd.notna(management_company) else ""
            
            # 管理前滞納額
            arrears_amount = str(row.get("滞納額合計", "")).replace(',', '').replace('¥', '').strip()
            converted_row["管理前滞納額"] = arrears_amount if arrears_amount.isdigit() else ""
            
            # 管理受託日
            converted_row["管理受託日"] = datetime.now().strftime("%Y/%m/%d")
            
            output_df = pd.concat([output_df, pd.DataFrame([converted_row])], ignore_index=True)
        
        return output_df


def process_capco_data(capco_content: bytes, contract_content: bytes) -> Tuple[pd.DataFrame, List[str], str]:
    """
    CAPCO新規登録データ処理メイン関数（GitHub完全版）
    GitHubのmain.pyの処理フローを完全踏襲
    
    Args:
        capco_content: カプコ元データ.csvの内容
        contract_content: ContractList_*.csvの内容
        
    Returns:
        tuple: (変換済みDF, 処理ログ, 出力ファイル名)
    """
    # ログ設定（GitHub完全踏襲）
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        logs = []
        start_time = datetime.now()
        
        # GitHub処理フロー完全踏襲
        logs.append("=== CAPCO新規登録処理開始 ===")
        logger.info("CAPCO新規登録処理開始")
        
        # 1. データ読み込み（GitHubのDataLoaderクラス使用）
        data_loader = DataLoader()
        capco_df = data_loader.load_capco_data(capco_content)
        contract_df = data_loader.load_contract_lists(contract_content)
        
        logs.append(f"ファイル読み込み完了: カプコ{len(capco_df)}件, ContractList{len(contract_df)}件")
        
        # 2. 重複チェック（GitHubのDataMatcherクラス使用）
        data_matcher = DataMatcher()
        new_contracts, existing_contracts, match_stats = data_matcher.find_new_contracts(capco_df, contract_df)
        
        logs.append(f"重複チェック結果: 新規{match_stats['new_contracts']}件, 既存{match_stats['existing_contracts']}件")
        
        # 3. データ変換（GitHubのDataConverterクラス使用）
        data_converter = DataConverter()
        output_df = data_converter.convert_to_mirail_format(new_contracts)
        
        logs.append(f"データ変換完了: {len(output_df)}件")
        
        # 4. 出力ファイル名生成（GitHub命名規則踏襲）
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{CapcoConfig.OUTPUT_FILE_PREFIX}.csv"
        
        # 5. 統計情報作成
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logs.append(f"=== 処理完了: {output_filename} ({processing_time:.2f}秒) ===")
        logger.info(f"CAPCO処理完了: {len(output_df)}件出力")
        
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
        error_msg = f"CAPCO処理エラー: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(), [error_msg], "error.csv"


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレート（GitHub完全版）"""
    # GitHub完全準拠のカラム構成
    template_columns = [
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
        "管理会社", "委託先法人ID", "登録フラグ"
    ]
    return pd.DataFrame(columns=template_columns)