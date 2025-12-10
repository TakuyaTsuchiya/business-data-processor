"""
pytest設定ファイル
テスト全体で使用する共通設定とフィクスチャを定義
"""

import pytest
import pandas as pd
from io import StringIO, BytesIO
import tempfile
import os


@pytest.fixture
def sample_ark_data():
    """アーク残債テスト用サンプルデータ"""
    data = """契約番号,管理前滞納額
A001,50000
A002,75000
A003,30000
A004,25000"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def sample_contract_data():
    """ContractListテスト用サンプルデータ"""
    data = """引継番号,管理番号
A001,M001
A002,M002
A003,M003
A004,M004"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def sample_mirail_data():
    """ミライルオートコール用サンプルデータ"""
    data = """クライアントCD,電話番号,契約者氏名,残債
1,03-1234-5678,田中太郎,5000
1,090-1234-5678,佐藤花子,10000
1,080-1234-5678,鈴木次郎,11000
1,,山田美咲,15000
2,06-1234-5678,高橋健太,20000"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def invalid_ark_data():
    """不正なアーク残債データ（必須カラム不足）"""
    data = """wrong_column,another_wrong
X001,invalid
X002,invalid"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def invalid_contract_data():
    """不正なContractListデータ（必須カラム不足）"""
    data = """wrong_col1,wrong_col2
Y001,invalid
Y002,invalid"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def no_match_ark_data():
    """紐付けできないアーク残債データ"""
    data = """契約番号,管理前滞納額
Z001,50000
Z002,75000"""
    return pd.read_csv(StringIO(data))


@pytest.fixture
def sample_autocall_history_data():
    """オートコール履歴テスト用サンプルデータ（list_export形式）"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        'fixtures', 'autocall_history', 'list_export_sample.csv'
    )
    return pd.read_csv(fixture_path)


@pytest.fixture
def expected_autocall_history_output():
    """オートコール履歴の期待される出力データ"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        'fixtures', 'autocall_history', 'expected_output.csv'
    )
    return pd.read_csv(fixture_path, keep_default_na=False)


@pytest.fixture
def sample_fine_history_data():
    """ファイン履歴テスト用サンプルデータ（携帯Mirail社納品データ形式）"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        'fixtures', 'fine_history', 'input_sample.csv'
    )
    return pd.read_csv(fixture_path)


@pytest.fixture
def expected_fine_history_output():
    """ファイン履歴の期待される出力データ"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        'fixtures', 'fine_history', 'expected_output.csv'
    )
    return pd.read_csv(fixture_path, keep_default_na=False)


class MockUploadedFile:
    """Streamlit UploadedFileのモッククラス"""
    
    def __init__(self, data_frame, filename="test.csv", encoding="utf-8"):
        self.name = filename
        self._df = data_frame
        self._encoding = encoding
        self._position = 0
        self._create_content()
    
    def _create_content(self):
        """DataFrameからCSVコンテンツを作成"""
        csv_string = self._df.to_csv(index=False, encoding=self._encoding)
        self._content = csv_string.encode(self._encoding)
    
    def read(self, size=-1):
        """ファイル内容を読み込み"""
        if size == -1:
            content = self._content[self._position:]
            self._position = len(self._content)
        else:
            content = self._content[self._position:self._position + size]
            self._position += len(content)
        return content
    
    def seek(self, position):
        """ファイルポジションを設定"""
        self._position = position
    
    def tell(self):
        """現在のファイルポジションを取得"""
        return self._position


@pytest.fixture
def mock_file_factory():
    """MockUploadedFileを作成するファクトリ"""
    def _create_mock_file(data_frame, filename="test.csv", encoding="utf-8"):
        return MockUploadedFile(data_frame, filename, encoding)
    return _create_mock_file


# テスト設定
pytest_plugins = []


# テスト実行時の設定
def pytest_configure(config):
    """pytest実行時の初期設定"""
    import sys
    import os
    
    # プロジェクトルートをPythonパスに追加
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


# カスタムマーカーの定義
def pytest_configure(config):
    """カスタムマーカーを登録"""
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"  
    )