"""
アーク残債更新処理
管理番号と管理前滞納額の2列のみを出力
"""

import pandas as pd
import io
from datetime import datetime
from typing import Optional, Tuple, Union
import chardet

# Streamlitのインポートを条件付きにする
try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

# エンコーディング候補リスト（GitHubコードから踏襲）
ENCODING_CANDIDATES = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']


def detect_encoding(file_content: Union[bytes, str]) -> str:
    """ファイルのエンコーディングを検出する"""
    if isinstance(file_content, str):
        # ファイルパスの場合
        file_path = file_content
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
    else:
        # バイトデータの場合
        raw_data = file_content[:10000] if len(file_content) > 10000 else file_content
        file_path = None
    
    # chardetで検出
    result = chardet.detect(raw_data)
    detected_encoding = result['encoding']
    confidence = result.get('confidence', 0)
    
    if HAS_STREAMLIT:
        st.info(f"📊 エンコーディング検出: {detected_encoding} (信頼度: {confidence:.2f})")
    
    # 信頼度が低い場合は代替エンコーディングを試す
    if confidence < 0.7:
        if HAS_STREAMLIT:
            st.warning(f"⚠️ エンコーディング信頼度が低いため、代替エンコーディングを試します")
        
        # ファイルパスがある場合のみ実際の読み込みテストが可能
        if file_path:
            for encoding in ENCODING_CANDIDATES:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        f.read(1000)  # テスト読み込み
                    if HAS_STREAMLIT:
                        st.success(f"✅ エンコーディング '{encoding}' で読み込み成功")
                    return encoding
                except UnicodeDecodeError:
                    continue
        else:
            # バイトデータの場合は候補から選択
            for encoding in ENCODING_CANDIDATES:
                try:
                    raw_data.decode(encoding)
                    if HAS_STREAMLIT:
                        st.success(f"✅ エンコーディング '{encoding}' でデコード成功")
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
        # フォールバック
        if HAS_STREAMLIT:
            st.warning("⚠️ エンコーディングを検出できません。cp932を使用します")
        return 'cp932'


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
        arc_file: アーク残債CSVファイル（ファイルパス、UploadedFile、またはバイトデータ）
        contract_file: ContractListファイル（ファイルパス、UploadedFile、またはバイトデータ）
        
    Returns:
        Tuple[pd.DataFrame, str]: (処理済みデータフレーム, 出力ファイル名)
        エラー時はNone
    """
    try:
        # Phase 1: ファイル読み込み
        if HAS_STREAMLIT:
            st.info("📂 Phase 1: ファイル読み込み")
        
        # アーク残債CSV読み込み
        try:
            if hasattr(arc_file, 'getvalue'):
                # Streamlit UploadedFileの場合
                arc_content = arc_file.getvalue()
                arc_encoding = detect_encoding(arc_content)
                arc_df = pd.read_csv(io.BytesIO(arc_content), encoding=arc_encoding)
            elif isinstance(arc_file, bytes):
                # バイトデータの場合
                arc_encoding = detect_encoding(arc_file)
                arc_df = pd.read_csv(io.BytesIO(arc_file), encoding=arc_encoding)
            else:
                # ファイルパスの場合
                arc_encoding = detect_encoding(arc_file)
                arc_df = pd.read_csv(arc_file, encoding=arc_encoding)
            
            if HAS_STREAMLIT:
                st.success(f"✅ アーク残債CSV読み込み完了: {len(arc_df):,}行")
                # デバッグ情報：カラム名を表示
                with st.expander("🔍 アーク残債CSVカラム一覧"):
                    st.write(f"カラム数: {len(arc_df.columns)}")
                    st.write(f"カラム名: {list(arc_df.columns)}")
        except Exception as e:
            if HAS_STREAMLIT:
                st.error(f"❌ アーク残債CSVファイルの読み込みに失敗しました: {str(e)}")
            return None
        
        # ContractList読み込み
        try:
            if hasattr(contract_file, 'getvalue'):
                # Streamlit UploadedFileの場合
                contract_content = contract_file.getvalue()
                contract_encoding = detect_encoding(contract_content)
                contract_df = pd.read_csv(io.BytesIO(contract_content), encoding=contract_encoding)
            elif isinstance(contract_file, bytes):
                # バイトデータの場合
                contract_encoding = detect_encoding(contract_file)
                contract_df = pd.read_csv(io.BytesIO(contract_file), encoding=contract_encoding)
            else:
                # ファイルパスの場合
                contract_encoding = detect_encoding(contract_file)
                contract_df = pd.read_csv(contract_file, encoding=contract_encoding)
            
            if HAS_STREAMLIT:
                st.success(f"✅ ContractList読み込み完了: {len(contract_df):,}行")
                # デバッグ情報：カラム名を表示
                with st.expander("🔍 ContractListカラム一覧"):
                    st.write(f"カラム数: {len(contract_df.columns)}")
                    st.write(f"カラム名: {list(contract_df.columns)}")
        except Exception as e:
            if HAS_STREAMLIT:
                st.error(f"❌ ContractListファイルの読み込みに失敗しました: {str(e)}")
            return None
        
        # Phase 2: カラム確認
        if HAS_STREAMLIT:
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
            if HAS_STREAMLIT:
                st.error(f"❌ アーク残債CSVに必要なカラムが見つかりません: {missing_ark}")
                st.error(f"利用可能なカラム: {list(arc_df.columns)}")
            return None
            
        if missing_contract:
            if HAS_STREAMLIT:
                st.error(f"❌ ContractListに必要なカラムが見つかりません: {missing_contract}")
                st.error(f"利用可能なカラム: {list(contract_df.columns)}")
            return None
        
        if HAS_STREAMLIT:
            st.success("✅ 必須カラム確認完了")
        
        # Phase 3: データ処理・紐付け
        if HAS_STREAMLIT:
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
        
        if HAS_STREAMLIT:
            st.write(f"📊 紐付け結果:")
            st.write(f"- アーク残債総数: {arc_total:,}件")
            st.write(f"- 紐付け成功: {matched_count:,}件 ({match_ratio:.1f}%)")
            st.write(f"- 紐付け失敗: {unmatch_count:,}件")
        
        if matched_count == 0:
            if HAS_STREAMLIT:
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
        if HAS_STREAMLIT:
            st.success("✅ Phase 4: 処理完了")
            st.write(f"📊 最終出力:")
            st.write(f"- レコード数: {len(output_df):,}件")
            st.write(f"- 管理前滞納額合計: ¥{output_df['管理前滞納額'].sum():,}")
            st.write(f"- 平均滞納額: ¥{output_df['管理前滞納額'].mean():,.0f}")
        
        return output_df, output_filename
        
    except Exception as e:
        if HAS_STREAMLIT:
            st.error(f"❌ 処理中にエラーが発生しました: {str(e)}")
            st.error("エラーの詳細:")
            st.exception(e)
        return None