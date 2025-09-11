import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import logging

logger = logging.getLogger(__name__)


class AddressSplitter:
    """辞書ベースの住所分割処理クラス（実運用版）"""
    
    def __init__(self):
        self.municipalities: Dict[str, List[str]] = {}
        self.prefectures: List[str] = []
        self._load_municipalities()
        
    def _load_municipalities(self):
        """市区町村データを読み込む（環境非依存）"""
        try:
            # 環境に依存しないパス解決
            current_file = Path(__file__)
            data_path = current_file.parent.parent.parent / 'data' / 'municipalities.json'
            
            # UTF-8で読み込み（BOMも考慮）
            with open(data_path, 'r', encoding='utf-8-sig') as f:
                self.municipalities = json.load(f)
                
            self.prefectures = list(self.municipalities.keys())
            logger.debug(f"Loaded {len(self.municipalities)} prefectures")
            
        except FileNotFoundError:
            logger.error(f"municipalities.json not found at {data_path}")
            # フォールバック: 空のデータで初期化（例外を出さない）
            self.municipalities = {}
            self.prefectures = []
        except Exception as e:
            logger.error(f"Error loading municipalities: {e}")
            self.municipalities = {}
            self.prefectures = []
    
    def normalize(self, text: str) -> str:
        """表記ゆれの正規化（NFKC→軽い置換）"""
        if not text:
            return ""
        
        # 1) 全角/半角・濁点結合などを統一
        text = unicodedata.normalize('NFKC', text)
        
        # 2) スペース除去
        text = text.replace(' ', '').replace('　', '')
        
        # カタカナの正規化（主要なゆらぎのみ）
        replacements = {
            'ヶ': 'ケ',  # 「ヶ/ケ」ゆらぎを統一
            'ヵ': 'カ',  # 必要ならこれも統一
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        return text
    
    def extract_prefecture(self, address: str) -> Tuple[str, str]:
        """都道府県を抽出（文字間の空白も許容）"""
        for pref in self.prefectures:
            # 例: "東\s*京\s*都" のように各文字の間の空白を許容
            pattern = r'^' + r'\s*'.join(map(re.escape, list(pref)))
            m = re.match(pattern, address)
            if m:
                # 残り住所の先頭スペースも削除
                remaining = address[m.end():].lstrip()
                return pref, remaining
        return "", address
    
    def extract_municipality(self, prefecture: str, address: str) -> Tuple[str, str]:
        """市区町村を抽出（辞書ベース）"""
        if prefecture not in self.municipalities:
            return "", address
        
        normalized_addr = self.normalize(address)
        
        # 都道府県の全市区町村でマッチング（長い順）
        municipalities = sorted(
            self.municipalities[prefecture], 
            key=len, 
            reverse=True
        )
        
        for municipality in municipalities:
            normalized_muni = self.normalize(municipality)
            if normalized_addr.startswith(normalized_muni):
                # 元の住所から正規化前の長さ分を切り取る
                # スペースを考慮して元の住所での終端位置を見つける
                pattern = r'^' + r'\s*'.join(map(re.escape, list(municipality)))
                m = re.match(pattern, address)
                if m:
                    # 残り住所の先頭スペースも削除
                    remaining = address[m.end():].lstrip()
                    return municipality, remaining
                else:
                    # フォールバック（スペースなしの場合）
                    remaining = address[len(municipality):].lstrip()
                    return municipality, remaining
        
        return "", address
    
    def split_address(self, address: str) -> Dict[str, str]:
        """住所を分割する（郵便番号、都道府県、市区町村、残り）"""
        result = {
            "postal_code": "",
            "prefecture": "",
            "city": "",
            "remaining": ""
        }
        
        if not address:
            return result
        
        # 前後の空白を削除
        address = address.strip()
        
        # 郵便番号の抽出
        postal_match = re.match(r'^(\d{3}-?\d{4})\s*', address)
        if postal_match:
            result["postal_code"] = postal_match.group(1)
            address = address[postal_match.end():]
        
        # 都道府県の抽出
        prefecture, remaining = self.extract_prefecture(address)
        if prefecture:
            result["prefecture"] = prefecture
            address = remaining
        
        # 市区町村の抽出
        if prefecture:
            city, remaining = self.extract_municipality(prefecture, address)
            if city:
                result["city"] = city
                address = remaining
        
        # 残りの住所
        result["remaining"] = address.strip()
        
        return result
    
    def get_statistics(self) -> Dict[str, int]:
        """データ統計情報を返す（空データ対応）"""
        if not self.municipalities:
            return {
                "prefectures": 0,
                "total_municipalities": 0,
                "max_municipalities": 0,
                "min_municipalities": 0,
            }
        
        totals = [len(m) for m in self.municipalities.values()]
        return {
            "prefectures": len(self.municipalities),
            "total_municipalities": sum(totals),
            "max_municipalities": max(totals),
            "min_municipalities": min(totals),
        }