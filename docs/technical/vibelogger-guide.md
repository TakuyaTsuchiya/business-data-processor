# ViveLogger使用ガイドライン

## 概要

ViveLoggerはAIコーディングエージェント向けに設計された構造化ログシステムです。本プロジェクトでは、既存のProcessLoggerクラスにViveLoggerを統合し、AI分析可能なログを出力します。

## 基本的な使い方

### 1. 従来通りのログ（後方互換性）

```python
from infra.logging.logger import create_logger

logger = create_logger("プロセッサー名")
logger.info("処理完了")
```

### 2. ViveLogger対応版（推奨）

```python
logger.info(
    "フィルタリング完了",
    operation="filter_trustee_company",
    context={
        "before_count": 1000,
        "after_count": 800,
        "filter_conditions": {
            "trustee_company_id": ["空白", "5"]
        }
    },
    human_note="AI-INFO: 除外率20%は正常範囲"
)
```

## ログレベル別使用方法

### info() - 情報ログ
```python
logger.info(
    message="処理ステップの説明",
    operation="処理の種類",  # optional
    context={"key": "value"},  # optional
    human_note="AI向けメモ"  # optional
)
```

### warning() - 警告ログ
```python
logger.warning(
    message="注意すべき状況",
    operation="warning_type",
    context={"issue_type": "data_quality"},
    human_note="AI-WARNING: データ品質に問題がある可能性"
)
```

### error() - エラーログ
```python
try:
    # 処理
    pass
except Exception as e:
    logger.error(
        message="エラーが発生しました",
        operation="error_handling",
        context={"error_details": "詳細情報"},
        human_note="AI-ERROR: エラー分析と対処法の提案が必要",
        exception=e  # 例外オブジェクトを渡すと自動でトレースバックを記録
    )
```

## 専用メソッド

### log_data_processing() - データ処理ログ
```python
logger.log_data_processing(
    step="フィルタリング",
    before_count=1000,
    after_count=800,
    details="委託先法人IDによる絞り込み",
    conditions={
        "filter_type": "trustee_company_id",
        "allowed_values": ["空白", "5"]
    },
    human_note="AI-ANALYZE: 除外率をチェック"
)
```

### log_filter_result() - フィルタ結果ログ
```python
logger.log_filter_result(
    filter_name="委託先法人ID",
    before_count=1000,
    after_count=800,
    condition="空白または5のみ",
    filter_conditions={
        "column": "委託先法人ID",
        "logic": "IN",
        "values": [None, "", "5"]
    },
    human_note="AI-INFO: 通常の絞り込み率"
)
```

## human_noteの書き方

### プレフィックス規則
- `AI-INFO:` - 一般的な情報
- `AI-WARNING:` - 注意が必要な状況
- `AI-ERROR:` - エラー分析が必要
- `AI-TODO:` - AIが対応すべきタスク
- `AI-ANALYZE:` - データ分析の指示
- `AI-SUCCESS:` - 正常完了の報告

### 書き方例
```python
# 良い例
human_note="AI-INFO: 除外率20%は正常範囲。50%を超えた場合は条件見直しが必要"
human_note="AI-TODO: このエラー率が5%を超えた場合は、入力データの品質を確認"
human_note="AI-ANALYZE: 処理時間が平均より3倍遅い。パフォーマンス改善を検討"

# 悪い例（情報が不足）
human_note="エラー"
human_note="処理完了"
```

## contextの構造化

### 推奨するcontextの構造
```python
context = {
    # 基本情報
    "operation_type": "data_processing",
    "processor_name": "mirail_contract_without10k",
    
    # 数値情報
    "before_count": 1000,
    "after_count": 800,
    "processing_time_ms": 1500,
    
    # 条件情報
    "filter_conditions": {
        "trustee_company_id": ["空白", "5"],
        "debt_exclusion": [10000, 11000]
    },
    
    # パフォーマンス情報
    "memory_usage_mb": 150,
    "cpu_time_ms": 800
}
```

## ファイル出力先

ViveLoggerのログは以下に出力されます：
```
logs/vibe/
└── [日付]/
    └── [プロセッサー名].json
```

## AIでの分析方法

1. ログファイルをAIに読み込ませる
```bash
# ログファイルの場所を確認
ls logs/vibe/
```

2. AIに分析を依頼
```
このViveLoggerログを分析して、以下を教えて：
- 処理のボトルネック
- 異常なパターン
- 改善提案
```

## 注意事項

### 1. 後方互換性
既存のログ呼び出し（operation、context、human_noteなしの呼び出し）も引き続き動作します。

### 2. エラー耐性
ViveLoggerが利用できない環境でも、従来のログは正常に動作します。

### 3. パフォーマンス
ViveLoggerはオプショナルです。追加のログ出力によるパフォーマンス影響は最小限に抑えられています。

### 4. セキュリティ
個人情報や機密データをcontextやhuman_noteに含めないよう注意してください。

## 実装例

実装済みのプロセッサー例：
- `processors/mirail_autocall/contract/without10k.py`

この例を参考に、他のプロセッサーにも段階的にViveLogger対応を追加してください。