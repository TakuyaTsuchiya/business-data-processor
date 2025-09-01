# screens/ディレクトリ移行 Phase 2 - 要件定義書

## 1. 背景と目的

Phase 1でオートコール系12関数の移行が完了し、app.pyが1397行から1088行に削減された。
Phase 2では残りのshow関数を移行し、さらなるコード整理を行う。

## 2. 現状分析

### 2.1 移行済み（Phase 1）
- ミライルオートコール: 6関数 → screens/mirail_autocall.py
- フェイスオートコール: 3関数 → screens/faith_autocall.py  
- プラザオートコール: 3関数 → screens/plaza_autocall.py

### 2.2 残存show関数（実際の17個）
app.py内に残っているshow関数を以下のカテゴリに分類：

#### SMS系（9個）
- show_faith_sms_vacated() - フェイス契約者SMS（退去済み）
- show_faith_sms_guarantor() - フェイス保証人SMS
- show_faith_sms_emergency_contact() - フェイス緊急連絡人SMS
- show_mirail_sms_contract() - ミライル契約者SMS
- show_mirail_sms_guarantor() - ミライル保証人SMS
- show_mirail_sms_emergencycontact() - ミライル緊急連絡人SMS
- show_plaza_sms_contract() - プラザ契約者SMS
- show_plaza_sms_guarantor() - プラザ保証人SMS
- show_plaza_sms_contact() - プラザ緊急連絡人SMS

#### データ変換系（新規登録）（6個）
- show_ark_registration_tokyo() - アーク新規登録（東京）
- show_ark_registration_osaka() - アーク新規登録（大阪）
- show_ark_registration_hokkaido() - アーク新規登録（北海道）
- show_ark_registration_kitakanto() - アーク新規登録（北関東）
- show_arktrust_registration_tokyo() - アークトラスト新規登録（東京）
- show_capco_registration() - カプコ新規登録

#### その他（2個）
- show_ark_late_payment() - アーク遅延損害金
- show_capco_debt_update() - カプコ債務更新

## 3. 設計方針

### 3.1 ディレクトリ構造
```
screens/
├── __init__.py
├── mirail_autocall.py     # 完了
├── faith_autocall.py      # 完了
├── plaza_autocall.py      # 完了
├── sms/                   # 新規作成
│   ├── __init__.py
│   ├── faith.py          # フェイスSMS 3関数
│   ├── mirail.py         # ミライルSMS 3関数
│   └── plaza.py          # プラザSMS 3関数
├── registration/          # 新規作成
│   ├── __init__.py
│   ├── ark.py            # アーク新規登録 4関数（東京・大阪・北海道・北関東）
│   ├── arktrust.py       # アークトラスト新規登録 1関数
│   └── capco.py          # カプコ新規登録 1関数
└── others/                # 新規作成
    ├── __init__.py
    ├── ark_late_payment.py   # アーク遅延損害金 1関数
    └── capco_debt_update.py  # カプコ債務更新 1関数
```

### 3.2 モジュール設計原則
1. 各モジュールは関連する機能をまとめる
2. 共通のインポートは各モジュール内で管理
3. 関数名は既存のものを維持（後方互換性）
4. エラーハンドリングは既存の方式を継承

## 4. 実装タスクリスト

### Phase 2-1: SMS系の移行
- [ ] screens/sms/__init__.py作成
- [ ] screens/sms/faith.py作成
  - [ ] show_faith_sms_vacated()移動
  - [ ] show_faith_guarantor_sms()移動
  - [ ] show_faith_emergency_sms()移動
- [ ] screens/sms/plaza.py作成
  - [ ] show_plaza_sms_contract()移動
  - [ ] show_plaza_guarantor_sms()移動
  - [ ] show_plaza_emergency_sms()移動
- [ ] app.pyから関数削除・インポート追加

### Phase 2-2: データ変換系の移行
- [ ] screens/registration/__init__.py作成
- [ ] screens/registration/ark.py作成
  - [ ] show_ark_registration()移動
- [ ] screens/registration/capco.py作成
  - [ ] show_capco_registration()移動
- [ ] screens/registration/rrr.py作成
  - [ ] show_rrr_registration()移動
- [ ] screens/registration/plaza_update.py作成
  - [ ] show_plaza_updated_data()移動
- [ ] app.pyから関数削除・インポート追加

### Phase 2-3: 残債更新系の移行
- [ ] screens/arrears_update/__init__.py作成
- [ ] screens/arrears_update/mirail.py作成
  - [ ] show_ark_arrears_update_mirail()移動
  - [ ] show_ark_arrears_update_mirail_from_new()移動
- [ ] screens/arrears_update/faith.py作成
  - [ ] show_ark_arrears_update_faith()移動
  - [ ] show_ark_arrears_update_faith_from_new()移動
- [ ] screens/arrears_update/plaza.py作成
  - [ ] show_ark_arrears_update_plaza()移動
  - [ ] show_ark_arrears_update_plaza_from_new()移動
  - [ ] show_ark_arrears_update_plaza_from_cancellation()移動
- [ ] app.pyから関数削除・インポート追加

### Phase 2-4: 検証とデプロイ
- [ ] 全インポートの動作確認
- [ ] PROCESSOR_MAPPINGの参照確認
- [ ] app.pyの最終行数確認
- [ ] git commit/push
- [ ] VPSでgit pull

## 5. 期待される成果

### 5.1 定量的成果
- app.py: 1088行 → 約600行（推定500行削減）
- 機能別モジュール数: 3 → 10
- 1ファイルあたりの平均行数: 約100行

### 5.2 定性的成果
- コードの見通し向上
- 機能単位でのメンテナンス性向上
- 新機能追加時の作業効率化
- チーム開発での並行作業が容易に

## 6. リスクと対策

### 6.1 リスク
1. インポートパスの誤り
2. 関数名の不一致
3. プロセッサーインポートの重複
4. 実行時エラー

### 6.2 対策
1. 段階的な移行とテスト
2. 既存の関数名を完全に維持
3. 不要なインポートの削除
4. 各フェーズでの動作確認

## 7. 実装順序の根拠

1. **SMS系を最初に**: 関数数が明確で、カテゴリが分かりやすい
2. **データ変換系を次に**: 独立性が高く、影響範囲が限定的
3. **残債更新系を最後に**: 最も複雑で関数数も多いため

この順序により、リスクを最小化しながら段階的に移行を進められる。