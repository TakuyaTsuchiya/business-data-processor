"""
ファイル処理サービス - Services Layer

このモジュールは、ファイルアップロード、読み込み、検証に関する
共通処理を提供し、UI層でのファイル処理を統一します。

統合対象：
- 単一ファイルアップロード処理（13関数で重複）
- 複数ファイルアップロード処理（8関数で重複）
- ファイル読み込み成功表示（全関数で重複）
- ファイル不足警告表示（複数ファイル処理で重複）
"""

import streamlit as st
from typing import List, Optional, Tuple, Dict, Any
import pandas as pd


class FileUploadResult:
    """ファイルアップロード結果を格納するデータクラス"""
    
    def __init__(
        self,
        success: bool,
        file_contents: Optional[List[bytes]] = None,
        filenames: Optional[List[str]] = None,
        error_message: Optional[str] = None
    ):
        self.success = success
        self.file_contents = file_contents or []
        self.filenames = filenames or []
        self.error_message = error_message
    
    @property
    def single_file_content(self) -> Optional[bytes]:
        """単一ファイル用のファイル内容を取得"""
        return self.file_contents[0] if self.file_contents else None
    
    @property
    def file_count(self) -> int:
        """アップロードされたファイル数"""
        return len(self.file_contents)


class FileUploadService:
    """
    ファイルアップロード処理を統合管理するサービスクラス
    
    Streamlitのfile_uploaderの共通処理と結果管理を提供します。
    """
    
    @staticmethod
    def handle_single_file_upload(
        label: str,
        key: str,
        file_types: List[str] = ["csv"],
        show_success: bool = True
    ) -> FileUploadResult:
        """
        単一ファイルアップロード処理
        
        Args:
            label (str): アップローダーのラベル
            key (str): Streamlitウィジェットのキー
            file_types (List[str]): 許可するファイル拡張子
            show_success (bool): 成功メッセージを表示するか
            
        Returns:
            FileUploadResult: アップロード結果
            
        Examples:
            >>> result = FileUploadService.handle_single_file_upload(
            ...     "CSVファイルをアップロードしてください",
            ...     "mirail_contract_file"
            ... )
            >>> if result.success:
            ...     content = result.single_file_content
        """
        uploaded_file = st.file_uploader(label, type=file_types, key=key)
        
        if uploaded_file is not None:
            try:
                file_content = uploaded_file.read()
                
                if show_success:
                    st.success(f"✅ {uploaded_file.name}: 読み込み完了")
                
                return FileUploadResult(
                    success=True,
                    file_contents=[file_content],
                    filenames=[uploaded_file.name]
                )
                
            except Exception as e:
                error_msg = f"ファイル読み込みエラー: {str(e)}"
                st.error(error_msg)
                
                return FileUploadResult(
                    success=False,
                    error_message=error_msg
                )
        
        return FileUploadResult(success=False)
    
    @staticmethod
    def handle_multiple_file_upload(
        file_configs: List[Dict[str, str]],
        show_success: bool = True
    ) -> FileUploadResult:
        """
        複数ファイルアップロード処理
        
        Args:
            file_configs (List[Dict[str, str]]): ファイル設定のリスト
                各設定: {"label": "ラベル", "key": "キー", "description": "説明"}
            show_success (bool): 成功メッセージを表示するか
            
        Returns:
            FileUploadResult: アップロード結果
            
        Examples:
            >>> configs = [
            ...     {"label": "案件取込用レポート.csvをアップロード", "key": "ark_file1", "description": "📄 ファイル1: 案件取込用レポート"},
            ...     {"label": "ContractList_*.csvをアップロード", "key": "ark_file2", "description": "📄 ファイル2: ContractList"}
            ... ]
            >>> result = FileUploadService.handle_multiple_file_upload(configs)
        """
        uploaded_files = []
        file_contents = []
        filenames = []
        
        # 複数列レイアウトを作成
        cols = st.columns(len(file_configs))
        
        for i, config in enumerate(file_configs):
            with cols[i]:
                if "description" in config:
                    st.markdown(f"**{config['description']}**")
                
                uploaded_file = st.file_uploader(
                    config["label"],
                    type=config.get("file_types", ["csv"]),
                    key=config["key"]
                )
                
                uploaded_files.append(uploaded_file)
        
        # 全ファイルがアップロードされているかチェック
        if all(f is not None for f in uploaded_files):
            try:
                for uploaded_file in uploaded_files:
                    content = uploaded_file.read()
                    file_contents.append(content)
                    filenames.append(uploaded_file.name)
                
                if show_success:
                    for filename in filenames:
                        st.success(f"✅ {filename}: 読み込み完了")
                
                return FileUploadResult(
                    success=True,
                    file_contents=file_contents,
                    filenames=filenames
                )
                
            except Exception as e:
                error_msg = f"ファイル読み込みエラー: {str(e)}"
                st.error(error_msg)
                
                return FileUploadResult(
                    success=False,
                    error_message=error_msg
                )
        
        elif any(f is not None for f in uploaded_files):
            # 一部のファイルのみアップロード済み
            st.warning(f"{len(file_configs)}つのCSVファイルをアップロードしてください。")
            
            return FileUploadResult(
                success=False,
                error_message="ファイル不足"
            )
        
        # ファイルが全くアップロードされていない
        return FileUploadResult(success=False)
    
    @staticmethod
    def show_file_requirements(requirements: str):
        """
        必要ファイルの説明を表示
        
        Args:
            requirements (str): ファイル要件の説明
            
        Examples:
            >>> FileUploadService.show_file_requirements(
            ...     "📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）"
            ... )
        """
        st.info(requirements)
    
    @staticmethod
    def show_processing_time_warning(minutes: int):
        """
        処理時間の警告を表示
        
        Args:
            minutes (int): 予想処理時間（分）
            
        Examples:
            >>> FileUploadService.show_processing_time_warning(5)
        """
        st.warning(f"⏱️ **処理時間**: 処理には{minutes}分ほどかかります。お待ちください。")
    
    @staticmethod
    def create_file_upload_section(
        section_title: str,
        single_file_config: Optional[Dict[str, str]] = None,
        multiple_file_configs: Optional[List[Dict[str, str]]] = None,
        requirements: Optional[str] = None,
        processing_minutes: Optional[int] = None
    ) -> FileUploadResult:
        """
        ファイルアップロードセクション全体を作成
        
        Args:
            section_title (str): セクションタイトル
            single_file_config (Dict, optional): 単一ファイル設定
            multiple_file_configs (List[Dict], optional): 複数ファイル設定
            requirements (str, optional): ファイル要件説明
            processing_minutes (int, optional): 処理時間警告（分）
            
        Returns:
            FileUploadResult: アップロード結果
            
        Examples:
            >>> # 単一ファイル用
            >>> result = FileUploadService.create_file_upload_section(
            ...     "ミライル契約者処理",
            ...     single_file_config={"label": "CSVファイルをアップロード", "key": "mirail_file"}
            ... )
            >>> 
            >>> # 複数ファイル用
            >>> result = FileUploadService.create_file_upload_section(
            ...     "アーク新規登録",
            ...     multiple_file_configs=[
            ...         {"label": "案件取込用レポート", "key": "ark_file1"},
            ...         {"label": "ContractList", "key": "ark_file2"}
            ...     ],
            ...     requirements="📂 必要ファイル: 2ファイル処理",
            ...     processing_minutes=2
            ... )
        """
        # 要件説明
        if requirements:
            FileUploadService.show_file_requirements(requirements)
        
        # 処理時間警告
        if processing_minutes:
            FileUploadService.show_processing_time_warning(processing_minutes)
        
        # ファイルアップロード処理
        if single_file_config:
            return FileUploadService.handle_single_file_upload(**single_file_config)
        elif multiple_file_configs:
            return FileUploadService.handle_multiple_file_upload(multiple_file_configs)
        else:
            return FileUploadResult(
                success=False,
                error_message="ファイル設定が不正です"
            )