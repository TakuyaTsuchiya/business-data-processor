"""
SMS共通プロセッサモジュール

8種類のSMS処理を共通化した実装。
プラザ契約者SMSのみ特殊処理のため除外。
"""

import pandas as pd
import chardet
from datetime import datetime, date
from typing import Tuple, Dict, Any, List, Optional
import io
import re
import os

from .config import SMSConfig
from .validators import validate_phone_number, is_mobile_number
from .utils import format_phone_number, clean_text, generate_output_filename


# 列番号マッピング（0ベースインデックス）
COLUMN_MAPPINGS = {
    'contract': {
        'phone_column_idx': 27,      # AB列
        'person_name_field': None,   # 使用しない
    },
    'guarantor': {
        'phone_column_idx': 46,      # AU列
        'person_name_field': '保証人１氏名',
    },
    'emergency': {
        'phone_column_idx': 56,      # BE列
        'person_name_field': '緊急連絡人１氏名',
    }
}

# その他の固定列番号
FIXED_COLUMNS = {
    '入金予定日': 72,    # BU列
    '入金予定金額': 73,  # BV列
    '回収ランク': 86,    # CI列
    '委託先法人ID': 118  # DO列
}


def load_sms_template_headers() -> List[str]:
    """SMS共通テンプレートヘッダーを読み込み"""
    # 複数のパス候補を試行
    current_dir = os.path.dirname(__file__)
    
    # パス候補リスト
    template_paths = [
        os.path.join(current_dir, '..', '..', 'templates', 'sms_template_headers.txt'),  # 相対パス
        os.path.abspath(os.path.join(current_dir, '..', '..', 'templates', 'sms_template_headers.txt')),  # 絶対パス
        'templates/sms_template_headers.txt',  # カレントディレクトリから
        './templates/sms_template_headers.txt',  # カレントディレクトリから
    ]
    
    # 各パスを試行
    for template_path in template_paths:
        if os.path.exists(template_path):
            try:
                # 複数のエンコーディングで試行
                encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis']
                header_line = None
                
                for encoding in encodings:
                    try:
                        with open(template_path, 'r', encoding=encoding) as f:
                            header_line = f.read().strip()
                        break
                    except UnicodeDecodeError:
                        continue
                        
                if header_line is None:
                    raise Exception("ヘッダーファイルの読み込みに失敗しました（エンコーディングエラー）")
                    
                return header_line.split(',')
            except Exception as e:
                continue  # 次のパスを試行
    
    # すべてのパスで失敗した場合
    raise FileNotFoundError(f"SMSテンプレートヘッダーファイルが見つかりません。試行パス: {template_paths}")


def format_payment_deadline(date_input: date) -> str:
    """
    日付オブジェクトを日本語形式に変換
    
    Args:
        date_input: datetimeのdateオブジェクト
        
    Returns:
        str: 'YYYY年MM月DD日' 形式の文字列
    """
    return date_input.strftime("%Y年%m月%d日")


class CommonSMSProcessor:
    """
    SMS処理の共通プロセッサ
    
    8種類のSMS処理を統一的に処理:
    - フェイス（契約者/保証人/緊急連絡人）
    - ミライル（契約者/保証人/緊急連絡人）
    - プラザ（保証人/緊急連絡人）
    """
    
    # クラス変数としてテンプレートをキャッシュ
    _template_cache = None
    
    def __init__(self, system_type: str, target_type: str, payment_deadline=None):
        """
        Args:
            system_type: システム種別 ('faith', 'mirail', 'plaza')
            target_type: 対象者種別 ('contract', 'guarantor', 'emergency')
            payment_deadline: 支払期限日付（オプション）
        """
        self.system_type = system_type
        self.target_type = target_type
        self.payment_deadline = payment_deadline
        self.config = SMSConfig()
        self.logs = []
        
        # システム別設定
        self.system_config = self._get_system_config()
        # 対象者別設定
        self.target_config = self._get_target_config()
        
    def _get_system_config(self) -> Dict[str, Any]:
        """システム別の設定を取得"""
        configs = {
            'faith': {
                'client_ids': [1, 2, 3, 4],
                'exclude_amounts': [],
                'date_filter': None,
                'output_prefix': 'フェイス',
                'output_columns': [
                    '顧客番号',
                    '送信先携帯番号', 
                    '契約者氏名',
                    '物件名',
                    '号室'
                ]
            },
            'mirail': {
                'client_ids': ['', '5'],  # 空白と5
                'exclude_amounts': [],
                'date_filter': None,
                'output_prefix': 'ミライル',
                'output_columns': [
                    '顧客管理番号',
                    '携帯電話番号',
                    '氏名',
                    '建物名',
                    '部屋番号'
                ]
            },
            'plaza': {
                'client_ids': [6],
                'exclude_amounts': [2, 3, 5, 12],
                'date_filter': 'before_today',  # 前日以前
                'output_prefix': 'プラザ',
                'output_columns': [
                    '顧客管理コード',
                    '電話番号',
                    '契約者（カナ）',
                    '物件所在地１',
                    '部屋番号'
                ]
            }
        }
        return configs.get(self.system_type, {})
    
    def _get_target_config(self) -> Dict[str, Any]:
        """対象者別の設定を取得"""
        configs = {
            'contract': {
                'phone_column': '契約者携帯電話',
                'name_column': '契約者氏名',
                'kana_column': '契約者カナ',
                'output_suffix': '契約者'
            },
            'guarantor': {
                'phone_column': 'TEL携帯.1',
                'name_column': '保証人氏名.1',
                'kana_column': '保証人カナ.1',
                'output_suffix': '保証人'
            },
            'emergency': {
                'phone_column': 'TEL携帯.2',
                'name_column': '緊急連絡先氏名.2',
                'kana_column': '緊急連絡先カナ.2',
                'output_suffix': '緊急連絡人'
            }
        }
        return configs.get(self.target_type, {})
    
    def process(self, uploaded_file, payment_deadline_date: date) -> Tuple[pd.DataFrame, str, List[str], dict]:
        """
        SMS処理のメイン処理
        
        Args:
            uploaded_file: アップロードされたファイル
            payment_deadline_date: 支払期限日付
            
        Returns:
            処理済みDataFrame, 出力ファイル名, 処理ログ, 統計情報
        """
        try:
            # ファイル読み込み
            df = self._read_file(uploaded_file)
            original_count = len(df)
            self.logs.append(f"ファイル読み込み完了: {original_count}件")
            
            # フィルタリング処理
            df = self._apply_filters(df)
            
            # 携帯電話番号の検証
            df = self._validate_phone_numbers(df)
            
            # 出力データの生成
            output_df = self._generate_output(df, payment_deadline_date)
            
            # ファイル名生成
            output_filename = self._generate_filename()
            
            # 統計情報の生成
            stats = {
                'initial_rows': original_count,
                'processed_rows': len(output_df)
            }
            
            # 処理完了ログ
            self.logs.append(f"処理完了: {len(output_df)}件")
            
            return output_df, self.logs, output_filename, stats
            
        except Exception as e:
            self.logs.append(f"エラー発生: {str(e)}")
            raise
    
    def _read_file(self, uploaded_file) -> pd.DataFrame:
        """ファイル読み込み（エンコーディング自動判定）- 最適化版"""
        # ファイルがbytesの場合はそのまま、それ以外はread()
        if isinstance(uploaded_file, bytes):
            content = uploaded_file
        else:
            content = uploaded_file.read()
        
        # 日本語CSVは99%がcp932かshift_jisなので、最初に試す
        for enc in ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']:
            try:
                df = pd.read_csv(
                    io.BytesIO(content), 
                    encoding=enc,
                    dtype=str,  # すべて文字列として読み込み（型推論を避ける）
                    low_memory=False  # メモリを事前確保して高速化
                )
                self.logs.append(f"エンコーディング: {enc}で読み込み成功")
                return df
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        
        # それでもダメならchardetを使用（最終手段）
        detected = chardet.detect(content[:10000])  # 最初の10KBのみで判定
        encoding = detected.get('encoding', 'cp932')
        
        try:
            df = pd.read_csv(
                io.BytesIO(content), 
                encoding=encoding,
                dtype=str,
                low_memory=False
            )
            self.logs.append(f"エンコーディング: {encoding}で読み込み成功（chardet使用）")
            return df
        except:
            raise ValueError("CSVファイルの読み込みに失敗しました。")
    
    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """フィルタリング処理"""
        # 1. 委託先法人IDフィルタ
        df = self._filter_client_ids(df)
        
        # 2. 入金予定日フィルタ（プラザのみ）
        if self.system_config.get('date_filter'):
            df = self._filter_payment_date(df)
        
        # 3. 入金予定金額フィルタ（プラザのみ）
        if self.system_config.get('exclude_amounts'):
            df = self._filter_payment_amount(df)
        
        # 4. 回収ランクフィルタ
        df = self._filter_collection_rank(df)
        
        return df
    
    def _filter_client_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """委託先法人IDフィルタ - 最適化版"""
        client_ids = self.system_config['client_ids']
        
        if self.system_type == 'mirail':
            # ミライルは空白と5（一度だけ変換）
            client_id_str = df['委託先法人ID'].astype(str).str.strip()
            df_filtered = df[
                (df['委託先法人ID'].isna()) | 
                (client_id_str == '') |
                (client_id_str == '5')
            ]
        else:
            # フェイス、プラザは数値で比較（インプレース変換を避ける）
            client_id_numeric = pd.to_numeric(df['委託先法人ID'], errors='coerce')
            df_filtered = df[client_id_numeric.isin(client_ids)]
        
        self.logs.append(f"委託先法人IDフィルタ後: {len(df_filtered)}件")
        return df_filtered
    
    def _filter_payment_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """入金予定日フィルタ（前日以前）"""
        today = pd.Timestamp.now().normalize()
        
        # DataFrameのコピーを作成して警告を回避
        df = df.copy()
        
        # 入金予定日をdatetime型に変換
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], errors='coerce')
        
        # 前日以前（当日除外）
        df_filtered = df[
            (df['入金予定日'] < today) | 
            (df['入金予定日'].isna())
        ]
        
        self.logs.append(f"入金予定日フィルタ後: {len(df_filtered)}件")
        return df_filtered
    
    def _filter_payment_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        """入金予定金額フィルタ"""
        exclude_amounts = self.system_config['exclude_amounts']
        
        # 除外金額を取り除く
        df_filtered = df[~df['入金予定金額'].isin(exclude_amounts)]
        
        self.logs.append(f"入金予定金額フィルタ後: {len(df_filtered)}件（除外: {exclude_amounts}円）")
        return df_filtered
    
    def _filter_collection_rank(self, df: pd.DataFrame) -> pd.DataFrame:
        """回収ランクフィルタ"""
        exclude_ranks = self.config.exclude_collection_ranks
        
        # 除外ランクを含む行を削除
        pattern = '|'.join([re.escape(rank) for rank in exclude_ranks])
        df_filtered = df[~df['回収ランク'].astype(str).str.contains(pattern, na=False, regex=True)]
        
        self.logs.append(f"回収ランクフィルタ後: {len(df_filtered)}件")
        return df_filtered
    
    def _validate_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """携帯電話番号の検証"""
        # 列番号マッピングを取得
        column_mapping = COLUMN_MAPPINGS.get(self.target_type, {})
        phone_idx = column_mapping.get('phone_column_idx', 27)
        
        # 列番号が範囲内かチェック
        if phone_idx >= len(df.columns):
            # 列名でアクセス（テスト用）
            phone_column = self.target_config['phone_column']
            if phone_column not in df.columns:
                self.logs.append(f"電話番号列が見つかりません: {phone_column}")
                return pd.DataFrame()
            
            # 携帯電話番号が存在する行のみ
            df = df[df[phone_column].notna()]
            
            # 携帯番号形式の検証
            valid_mask = df[phone_column].apply(lambda x: is_mobile_number(str(x)))
            df_filtered = df[valid_mask]
        else:
            # 列番号でアクセス（本番用）
            phone_values = df.iloc[:, phone_idx].astype(str)
            
            # 携帯電話番号が存在する行のみ
            not_na_mask = phone_values != 'nan'
            df = df[not_na_mask]
            phone_values = phone_values[not_na_mask]
            
            # 携帯番号形式の検証
            valid_mask = phone_values.apply(lambda x: is_mobile_number(str(x)))
            df_filtered = df[valid_mask]
        
        self.logs.append(f"携帯電話番号検証後: {len(df_filtered)}件")
        return df_filtered
    
    def _generate_output(self, df: pd.DataFrame, payment_deadline_date: date) -> pd.DataFrame:
        """出力データの生成（59列形式）- ベクトル演算版"""
        # テンプレートヘッダーをキャッシュから取得
        if CommonSMSProcessor._template_cache is None:
            CommonSMSProcessor._template_cache = load_sms_template_headers()
        output_columns = CommonSMSProcessor._template_cache
        
        # 列番号マッピングを取得
        column_mapping = COLUMN_MAPPINGS.get(self.target_type, {})
        phone_idx = column_mapping.get('phone_column_idx', 27)
        
        # 支払期限の文字列（全行同じ）
        payment_deadline_str = format_payment_deadline(payment_deadline_date)
        
        # ベクトル演算で必要なデータのみ処理
        result = {}
        
        # 1. 電話番号（列番号または列名でアクセス）
        if phone_idx < len(df.columns):
            result['電話番号'] = df.iloc[:, phone_idx].astype(str)
        else:
            result['電話番号'] = df[self.target_config['phone_column']].astype(str)
        
        # 2. (info1)契約者名
        result['(info1)契約者名'] = df['契約者氏名'].fillna('').astype(str)
        
        # 3. (info2)物件名（物件名＋全角スペース＋部屋番号）
        property_name = df['物件名'].fillna('').astype(str)
        room_number = df.get('物件番号', df.get('部屋番号', pd.Series([''] * len(df))))
        room_number = room_number.fillna('').astype(str)
        # 部屋番号が空でない場合のみ全角スペースを追加
        result['(info2)物件名'] = property_name + room_number.apply(lambda x: f'　{x}' if x and x != 'nan' else '')
        
        # 4. (info3)金額（滞納残債をカンマ区切り）
        amount_col = df.get('滞納残債', df.get('入金予定金額', pd.Series([0] * len(df))))
        # 数値に変換してフォーマット
        result['(info3)金額'] = pd.to_numeric(amount_col, errors='coerce').fillna(0).astype(int).apply(lambda x: f'{x:,}')
        
        # 5. (info4)銀行口座（5項目を全角スペースで結合）
        bank_cols = ['回収口座銀行名', '回収口座支店名', '回収口座種類', '回収口座番号', '回収口座名義人']
        bank_data = []
        for col in bank_cols:
            if col in df.columns:
                data = df[col].fillna('').astype(str)
                if col == '回収口座番号':
                    # 口座番号の特殊処理
                    data = data.str.replace('="', '').str.replace('"', '')
                bank_data.append(data)
            else:
                bank_data.append(pd.Series([''] * len(df)))
        # 全角スペースで結合
        result['(info4)銀行口座'] = bank_data[0]
        for i in range(1, 5):
            result['(info4)銀行口座'] = result['(info4)銀行口座'] + '　' + bank_data[i]
        
        # 6. (info5)メモ（管理番号）
        result['(info5)メモ'] = df.get('管理番号', df.get('契約者ID', pd.Series([''] * len(df)))).fillna('').astype(str)
        
        # 7-56. 空列（50個）を追加
        for i in range(1, 51):
            result[f''] = ''
        
        # 57. 保証人（保証人SMSの場合のみ）
        if self.target_type == 'guarantor' and COLUMN_MAPPINGS[self.target_type].get('person_name_field'):
            result['保証人'] = df[COLUMN_MAPPINGS[self.target_type]['person_name_field']].fillna('').astype(str)
        else:
            result['保証人'] = ''
        
        # 58. 連絡人（緊急連絡人SMSの場合のみ）
        if self.target_type == 'emergency' and COLUMN_MAPPINGS[self.target_type].get('person_name_field'):
            result['連絡人'] = df[COLUMN_MAPPINGS[self.target_type]['person_name_field']].fillna('').astype(str)
        else:
            result['連絡人'] = ''
        
        # 59. 支払期限（全行同じ値）
        result['支払期限'] = payment_deadline_str
        
        # DataFrameを一度に作成（列順序を保持）
        output_df = pd.DataFrame(result)
        
        # 正しい列順序に並び替え（テンプレートの順序に合わせる）
        ordered_columns = []
        for col in output_columns:
            if col in output_df.columns:
                ordered_columns.append(col)
            else:
                # 空列の場合
                ordered_columns.append('')
        
        # 列名を正しい順序で設定
        output_df = output_df.reindex(columns=ordered_columns, fill_value='')
        output_df.columns = output_columns
        
        return output_df
    
    def _create_output_row(self, row: pd.Series) -> List[str]:
        """出力行の作成（システム別）"""
        phone_column = self.target_config['phone_column']
        name_column = self.target_config['name_column'] 
        kana_column = self.target_config['kana_column']
        
        if self.system_type == 'faith':
            return [
                str(row.get('契約者ID', '')),
                format_phone_number(str(row.get(phone_column, ''))),
                clean_text(str(row.get(name_column, ''))),
                clean_text(str(row.get('物件名', ''))),
                clean_text(str(row.get('部屋番号', '')))
            ]
        elif self.system_type == 'mirail':
            return [
                str(row.get('契約者ID', '')),
                format_phone_number(str(row.get(phone_column, ''))),
                clean_text(str(row.get(name_column, ''))),
                clean_text(str(row.get('物件名', ''))),
                clean_text(str(row.get('部屋番号', '')))
            ]
        else:  # plaza
            return [
                str(row.get('顧客管理コード', '')),
                format_phone_number(str(row.get(phone_column, ''))),
                clean_text(str(row.get(kana_column, ''))),
                clean_text(str(row.get('物件所在地１', ''))),
                clean_text(str(row.get('部屋番号', '')))
            ]
    
    def _create_full_output_row(self, row: pd.Series, raw_row: pd.Series, phone_idx: int, payment_deadline_date: date) -> List[str]:
        """59列の完全な出力行を作成 - 最適化版"""
        # 事前に59要素のリストを作成（高速）
        output_row = [''] * 59
        
        # 1. 電話番号（列番号から取得）
        if phone_idx < len(raw_row):
            output_row[0] = raw_row.iat[phone_idx] if pd.notna(raw_row.iat[phone_idx]) else ''
        else:
            # 列名でアクセス（テスト用）
            phone_val = row.get(self.target_config['phone_column'], '')
            output_row[0] = phone_val if pd.notna(phone_val) else ''
        
        # 2. (info1)契約者名（常に契約者氏名）
        name_val = row.get('契約者氏名', '')
        output_row[1] = name_val if pd.notna(name_val) else ''
        
        # 3. (info2)物件名（物件名＋全角スペース＋部屋番号）
        property_name = row.get('物件名', '')
        property_name = property_name if pd.notna(property_name) else ''
        room_number = row.get('物件番号', row.get('部屋番号', ''))
        if pd.notna(room_number) and str(room_number) != 'nan' and room_number:
            output_row[2] = f"{property_name}　{room_number}"
        else:
            output_row[2] = property_name
        
        # 4. (info3)金額（滞納残債をカンマ区切り）
        amount_val = row.get('滞納残債', row.get('入金予定金額', 0))
        if pd.notna(amount_val):
            # 数値として処理を試みる
            if isinstance(amount_val, (int, float)):
                output_row[3] = f"{int(amount_val):,}"
            else:
                # 文字列の場合はクリーニング
                amount_str = re.sub(r'[^0-9]', '', str(amount_val))
                if amount_str:
                    output_row[3] = f"{int(amount_str):,}"
                else:
                    output_row[3] = "0"
        else:
            output_row[3] = "0"
        
        # 5. (info4)銀行口座（5項目を全角スペースで結合）
        bank_fields = ['回収口座銀行名', '回収口座支店名', '回収口座種類', '回収口座番号', '回収口座名義人']
        bank_values = []
        for field in bank_fields:
            val = row.get(field, '')
            if pd.notna(val):
                if field == '回収口座番号':
                    # 口座番号の特殊処理
                    val = str(val).replace('="', '').replace('"', '')
                bank_values.append(str(val))
            else:
                bank_values.append('')
        output_row[4] = '　'.join(bank_values)
        
        # 6. (info5)メモ（管理番号）
        memo_val = row.get('管理番号', row.get('契約者ID', ''))
        output_row[5] = str(memo_val) if pd.notna(memo_val) else ''
        
        # 7-56. 空列（50個）は既に''で初期化済み
        
        # 57. 保証人（保証人SMSの場合のみ）
        if self.target_type == 'guarantor':
            person_name_field = COLUMN_MAPPINGS[self.target_type].get('person_name_field')
            if person_name_field:
                person_val = row.get(person_name_field, '')
                output_row[56] = str(person_val) if pd.notna(person_val) else ''
        
        # 58. 連絡人（緊急連絡人SMSの場合のみ）
        elif self.target_type == 'emergency':
            person_name_field = COLUMN_MAPPINGS[self.target_type].get('person_name_field')
            if person_name_field:
                person_val = row.get(person_name_field, '')
                output_row[57] = str(person_val) if pd.notna(person_val) else ''
        
        # 59. 支払期限（事前計算済み）
        output_row[58] = format_payment_deadline(payment_deadline_date)
        
        return output_row
    
    def _generate_filename(self) -> str:
        """出力ファイル名の生成（MMDDシステム名SMS対象者.csv形式）"""
        date_str = datetime.now().strftime('%m%d')
        prefix = self.system_config['output_prefix']
        
        # 対象者名のマッピング
        target_names = {
            'contract': '契約者',
            'guarantor': '保証人',
            'emergency': '緊急連絡人'
        }
        target_name = target_names.get(self.target_type, '')
        
        # MMDDシステム名SMS対象者.csv 形式
        return f"{date_str}{prefix}SMS{target_name}.csv"