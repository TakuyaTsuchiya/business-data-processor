"""
Service層用のロガー設定

統一されたログフォーマットと設定を提供し、
サービス層全体で一貫したログ出力を実現する。
"""

import logging
from typing import Optional


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    サービス層用の統一されたロガーを取得
    
    Args:
        name: ロガー名（通常は__name__）
        level: ログレベル（省略時はINFO）
        
    Returns:
        設定済みのロガーインスタンス
        
    Example:
        >>> from services.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("処理を開始します")
        2024-01-01 10:00:00 - services.autocall - INFO - 処理を開始します
    """
    logger = logging.getLogger(name)
    
    # 既にハンドラーが設定されている場合はスキップ
    if not logger.handlers:
        # コンソール出力用ハンドラー
        handler = logging.StreamHandler()
        
        # フォーマット設定
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        # ロガーに設定を適用
        logger.addHandler(handler)
        logger.setLevel(level or logging.INFO)
        
        # 親ロガーへの伝播を防ぐ（重複ログ防止）
        logger.propagate = False
    
    return logger