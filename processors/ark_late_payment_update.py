"""
アーク残債更新処理
ark-late-payment-updateプロジェクトの統合版プロセッサー
"""

import pandas as pd
import streamlit as st
import chardet
import io
import os
from datetime import datetime
from typing import Tuple, Optional, Dict, Any


def detect_encoding(file_content: bytes) -> str:
    """ファイルのエンコーディングを自動検出"""
    result = chardet.detect(file_content[:10000])
    detected_encoding = result['encoding']
    confidence = result['confidence']
    
    # Streamlitコンテキストがある場合のみUI表示
    try:
        # st._get_script_run_ctx()でコンテキスト存在確認
        if hasattr(st, '_get_script_run_ctx') and st._get_script_run_ctx() is not None:
            st.info(f"エンコーディング検出: {detected_encoding} (信頼度: {confidence:.2f})")
    except:
        # コンテキストなし または エラー時は静かに処理続行
        pass
    
    # 信頼度が低い場合は日本語エンコーディングを試行
    if confidence < 0.7:
        for encoding in ['cp932', 'shift_jis', 'utf-8']:
            try:
                file_content.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
    
    return detected_encoding or 'cp932'


def normalize_key_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """キー項目の正規化（空白除去、型統一）"""
    if column_name not in df.columns:
        raise KeyError(f"カラムが見つかりません: {column_name}")
    
    df_normalized = df.copy()
    
    # 文字列変換
    df_normalized[column_name] = df_normalized[column_name].astype(str)
    
    # 空白除去（全角・半角）
    df_normalized[column_name] = df_normalized[column_name].str.strip()
    df_normalized[column_name] = df_normalized[column_name].str.replace('　', '', regex=False)
    
    # NaN、空文字、'nan'の処理
    df_normalized = df_normalized[
        (df_normalized[column_name] != '') & 
        (df_normalized[column_name] != 'nan') &
        (df_normalized[column_name].notna())
    ]
    
    return df_normalized


def process_ark_late_payment_data(arc_file, contract_file) -> Optional[Tuple[pd.DataFrame, str]]:
    """
    アーク残債更新データ処理のメイン関数
    
    Args:
        arc_file: アーク残債CSVファイル（Streamlit UploadedFile）
        contract_file: ContractListファイル（Streamlit UploadedFile）
        
    Returns:
        Tuple[pd.DataFrame, str]: (処理済みデータフレーム, 出力ファイル名)
    """
    try:
        # Phase 1: ファイル読み込み
        st.info("📂 Phase 1: ファイル読み込み")
        
        # アーク残債CSV読み込み（bytes形式対応）
        try:
            arc_content = arc_file.getvalue()
            arc_encoding = detect_encoding(arc_content)
            arc_df = pd.read_csv(io.BytesIO(arc_content), encoding=arc_encoding)
            st.success(f"✅ アーク残債CSV読み込み完了: {len(arc_df):,}行")
        except Exception as e:
            st.error(f"❌ アーク残債CSVファイルの読み込みに失敗しました: {str(e)}")
            return None
        
        # ContractList読み込み（bytes形式対応）
        try:
            contract_content = contract_file.getvalue()
            contract_encoding = detect_encoding(contract_content)
            contract_df = pd.read_csv(io.BytesIO(contract_content), encoding=contract_encoding)
            st.success(f"✅ ContractList読み込み完了: {len(contract_df):,}行")
        except Exception as e:
            st.error(f"❌ ContractListファイルの読み込みに失敗しました: {str(e)}")
            return None
        
        # Phase 2: カラム確認
        st.info("🔍 Phase 2: 必須カラム確認")
        
        # アーク残債の必須カラム
        ark_required = {
            'contract_number': '契約番号',
            'amount': '管理前滞納額'
        }
        
        # ContractListの必須カラム
        contract_required = {
            'takeover_number': '引継番号',
            'management_number': '管理番号'
        }
        
        # カラム存在確認
        missing_ark = [col for col in ark_required.values() if col not in arc_df.columns]
        missing_contract = [col for col in contract_required.values() if col not in contract_df.columns]
        
        if missing_ark:
            st.error(f"❌ アーク残債CSVに必要なカラムが見つかりません: {missing_ark}")
            st.error(f"利用可能なカラム: {list(arc_df.columns)}")
            return None
            
        if missing_contract:
            st.error(f"❌ ContractListに必要なカラムが見つかりません: {missing_contract}")
            st.error(f"利用可能なカラム: {list(contract_df.columns)}")
            return None
        
        st.success("✅ 必須カラム確認完了")
        
        # Phase 3: データ処理・紐付け
        st.info("🔗 Phase 3: データ処理・紐付け")
        
        # キー項目の正規化
        arc_df = normalize_key_column(arc_df, '契約番号')
        contract_df = normalize_key_column(contract_df, '引継番号')
        
        # データ結合（契約番号と引継番号で紐付け）
        merged_df = pd.merge(
            arc_df[['契約番号', '管理前滞納額']],
            contract_df[['引継番号', '管理番号']],
            left_on='契約番号',
            right_on='引継番号',
            how='inner'
        )
        
        # 紐付け統計
        matched_count = len(merged_df)
        arc_total = len(arc_df)
        unmatch_count = arc_total - matched_count
        match_ratio = (matched_count / arc_total * 100) if arc_total > 0 else 0
        
        st.write(f"📊 紐付け結果:")
        st.write(f"- アーク残債総数: {arc_total:,}件")
        st.write(f"- 紐付け成功: {matched_count:,}件 ({match_ratio:.1f}%)")
        st.write(f"- 紐付け失敗: {unmatch_count:,}件")
        
        if matched_count == 0:
            st.error("❌ 紐付け処理の結果、出力データが0件になりました")
            st.error("原因: 契約番号と引継番号の値が一致していない可能性があります")
            return None
        
        # 出力データ作成（管理番号、管理前滞納額の2列のみ）
        output_df = merged_df[['管理番号', '管理前滞納額']].copy()
        
        # データ型変換
        output_df['管理番号'] = output_df['管理番号'].astype(str)
        output_df['管理前滞納額'] = pd.to_numeric(output_df['管理前滞納額'], errors='coerce').fillna(0).astype(int)
        
        # 重複除去（管理番号でグループ化し、管理前滞納額は合計）
        output_df = output_df.groupby('管理番号', as_index=False).agg({
            '管理前滞納額': 'sum'
        })
        
        # ソート
        output_df = output_df.sort_values('管理番号')
        
        # 出力ファイル名生成
        current_date = datetime.now()
        output_filename = f"{current_date.strftime('%m%d')}アーク残債.csv"
        
        # 処理サマリー表示
        st.success("✅ Phase 4: 処理完了")
        st.write(f"📊 最終出力:")
        st.write(f"- レコード数: {len(output_df):,}件")
        st.write(f"- 管理前滞納額合計: ¥{output_df['管理前滞納額'].sum():,}")
        st.write(f"- 平均滞納額: ¥{output_df['管理前滞納額'].mean():,.0f}")
        
        return output_df, output_filename
        
    except Exception as e:
        st.error(f"❌ 処理中にエラーが発生しました: {str(e)}")
        st.error("エラーの詳細:")
        st.exception(e)
        return None