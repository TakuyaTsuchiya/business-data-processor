"""
カプコ残債更新 - detect_encoding() のテスト

テスト対象: processors/capco_debt_update.py の detect_encoding() 関数
問題: 大きなファイルに対してchardet.detect()がファイル全体を解析し、タイムアウトする
修正: 先頭10KBのみをサンプリングして解析する
"""

from unittest.mock import patch
from processors.capco_debt_update import detect_encoding


# === テスト用データ生成ヘルパー ===


def _make_cp932_bytes(size_bytes: int) -> bytes:
    """指定サイズのCP932エンコードされた日本語テキストを生成"""
    # 日本語テキスト（CP932で1文字2バイト）
    base_text = "契約番号,滞納額合計\n12345,50000\n"
    encoded = base_text.encode("cp932")
    # 指定サイズになるまで繰り返し
    repeat_count = (size_bytes // len(encoded)) + 1
    return (encoded * repeat_count)[:size_bytes]


def _make_utf8_bytes(size_bytes: int) -> bytes:
    """指定サイズのUTF-8エンコードされた日本語テキストを生成"""
    base_text = "契約番号,滞納額合計\n12345,50000\n"
    encoded = base_text.encode("utf-8")
    repeat_count = (size_bytes // len(encoded)) + 1
    return (encoded * repeat_count)[:size_bytes]


# === サンプリングのテスト ===


class TestDetectEncodingSampling:
    """detect_encodingが大きなファイルで先頭10KBのみをサンプリングすることを検証"""

    def test_large_file_passes_only_sample_to_chardet(self):
        """50MBのファイルでchardet.detectに渡されるデータが10KB以下であること"""
        large_content = _make_cp932_bytes(50 * 1024 * 1024)  # 50MB

        with patch("processors.capco_debt_update.chardet.detect") as mock_detect:
            mock_detect.return_value = {"encoding": "CP932", "confidence": 0.99}
            detect_encoding(large_content)

            # chardet.detectに渡されたデータのサイズを検証
            call_args = mock_detect.call_args[0][0]
            assert len(call_args) <= 10000, (
                f"chardet.detectに{len(call_args)}バイト渡された（期待: ≤10000バイト）。"
                f"大きなファイル全体を渡すとタイムアウトの原因になる。"
            )

    def test_small_file_passes_entire_content_to_chardet(self):
        """10KB未満のファイルはそのまま全体をchardetに渡すこと"""
        small_content = _make_cp932_bytes(5000)  # 5KB

        with patch("processors.capco_debt_update.chardet.detect") as mock_detect:
            mock_detect.return_value = {"encoding": "CP932", "confidence": 0.99}
            detect_encoding(small_content)

            call_args = mock_detect.call_args[0][0]
            assert len(call_args) == len(small_content), (
                f"小さなファイルは全体を渡すべき。渡されたサイズ: {len(call_args)}, "
                f"ファイルサイズ: {len(small_content)}"
            )

    def test_exactly_10kb_file(self):
        """ちょうど10KBのファイルの境界値テスト"""
        content_10kb = _make_cp932_bytes(10000)

        with patch("processors.capco_debt_update.chardet.detect") as mock_detect:
            mock_detect.return_value = {"encoding": "CP932", "confidence": 0.99}
            detect_encoding(content_10kb)

            call_args = mock_detect.call_args[0][0]
            assert len(call_args) <= 10000


# === エンコーディング検出精度のテスト ===


class TestDetectEncodingAccuracy:
    """各エンコーディングの日本語テキストを正しく検出できることを検証"""

    def test_detect_cp932_encoding(self):
        """CP932エンコードされた日本語テキストを正しく検出"""
        text = "契約番号,滞納額合計\n12345,50000\n67890,30000\n"
        content = text.encode("cp932")
        result = detect_encoding(content)
        # CP932系のエンコーディングとして検出されること
        assert result.lower() in ("cp932", "shift_jis", "shift-jis", "windows-31j"), (
            f"CP932テキストの検出結果が'{result}'。CP932系のエンコーディングであるべき。"
        )

    def test_detect_utf8_encoding(self):
        """UTF-8エンコードされた日本語テキストを正しく検出"""
        text = "契約番号,滞納額合計\n12345,50000\n67890,30000\n"
        content = text.encode("utf-8")
        result = detect_encoding(content)
        assert result.lower() in ("utf-8", "utf-8-sig", "ascii"), (
            f"UTF-8テキストの検出結果が'{result}'。UTF-8系のエンコーディングであるべき。"
        )

    def test_detect_utf8_bom_encoding(self):
        """UTF-8 BOM付きテキストを正しく検出"""
        text = "契約番号,滞納額合計\n12345,50000\n"
        content = b"\xef\xbb\xbf" + text.encode("utf-8")  # BOM付き
        result = detect_encoding(content)
        assert result.lower() in ("utf-8", "utf-8-sig"), (
            f"UTF-8-BOMテキストの検出結果が'{result}'。UTF-8系であるべき。"
        )


# === フォールバック時のサンプリングテスト ===


class TestDetectEncodingFallback:
    """chardet信頼度が低い場合のフォールバック処理を検証"""

    def test_fallback_returns_valid_encoding_for_cp932(self):
        """chardet信頼度が低くてもCP932テキストのエンコーディングを正しく返す"""
        text = "契約番号,滞納額合計\n12345,50000\n67890,30000\n"
        content = text.encode("cp932")

        with patch("processors.capco_debt_update.chardet.detect") as mock_detect:
            # 信頼度を低くしてフォールバックを発生させる
            mock_detect.return_value = {"encoding": None, "confidence": 0.1}
            result = detect_encoding(content)

            # フォールバックでCP932系として検出されること
            assert result.lower() in ("cp932", "shift_jis", "shift-jis"), (
                f"フォールバック結果が'{result}'。CP932テキストなのでCP932系であるべき。"
            )

    def test_fallback_completes_quickly_for_large_file(self):
        """大きなファイルのフォールバック処理が高速に完了すること（タイムアウトしない）"""
        import time

        large_content = _make_cp932_bytes(20 * 1024 * 1024)  # 20MB

        with patch("processors.capco_debt_update.chardet.detect") as mock_detect:
            mock_detect.return_value = {"encoding": None, "confidence": 0.1}

            start = time.time()
            detect_encoding(large_content)
            elapsed = time.time() - start

            # 10KBサンプリングなら1秒以内に完了するはず
            # ファイル全体をデコード試行すると数秒〜数十秒かかる
            assert elapsed < 2.0, (
                f"フォールバック処理に{elapsed:.1f}秒かかった（期待: <2秒）。"
                f"ファイル全体をデコード試行している可能性がある。"
            )


# === エッジケースのテスト ===


class TestDetectEncodingEdgeCases:
    """エッジケースの処理を検証"""

    def test_empty_bytes(self):
        """空のバイト列でエラーにならずデフォルトエンコーディングを返す"""
        result = detect_encoding(b"")
        assert isinstance(result, str)
        assert len(result) > 0, "空ファイルでもエンコーディング文字列を返すべき"

    def test_ascii_only_content(self):
        """ASCII文字のみのコンテンツを処理できる"""
        content = b"contract_no,amount\n12345,50000\n67890,30000\n"
        result = detect_encoding(content)
        assert isinstance(result, str)
        # ASCII互換のエンコーディングであること
        assert result.lower() in (
            "ascii",
            "utf-8",
            "utf-8-sig",
            "cp932",
            "shift_jis",
        ), f"ASCIIテキストの検出結果が'{result}'。ASCII互換であるべき。"
