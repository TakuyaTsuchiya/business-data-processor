# Docker Desktop インストールガイド（Windows版）

## 📋 目次
1. [事前確認](#事前確認)
2. [ダウンロード](#ダウンロード)
3. [インストール](#インストール)
4. [初期設定](#初期設定)
5. [動作確認](#動作確認)
6. [よくある質問](#よくある質問)

---

## 事前確認

### システム要件
- **OS**: Windows 10 64-bit（バージョン1903以上）または Windows 11
- **メモリ**: 4GB以上（推奨8GB）
- **ディスク空き容量**: 10GB以上

### 確認方法
1. `Windows キー + Pause` を押す
2. 「システムの種類」が「64 ビット」であることを確認
3. Windows のバージョンを確認

---

## ダウンロード

### 手順1: 公式サイトアクセス
1. ブラウザで以下のURLにアクセス
   ```
   https://www.docker.com/products/docker-desktop
   ```

### 手順2: ダウンロード
1. 「Download for Windows」ボタンをクリック
2. ダウンロードが開始されます（約500MB）
3. ダウンロード完了まで待機

![Docker Desktop Download](https://docs.docker.com/desktop/images/docker-desktop-download.png)
*※ 画像は参考イメージです*

---

## インストール

### 手順1: インストーラー実行
1. ダウンロードした `Docker Desktop Installer.exe` をダブルクリック
2. 「ユーザーアカウント制御」が表示されたら「はい」をクリック

### 手順2: インストールオプション
1. 以下のオプションをチェック（推奨）
   - ✅ Enable WSL 2 Windows Features
   - ✅ Add shortcut to desktop
2. 「Ok」をクリック

### 手順3: インストール進行
1. インストールが自動的に進行します（5-10分）
2. 完了画面が表示されたら「Close and restart」をクリック
3. **パソコンが自動的に再起動します**

---

## 初期設定

### 手順1: Docker Desktop 起動
1. 再起動後、Docker Desktop が自動的に起動します
2. 起動しない場合：
   - スタートメニューから「Docker Desktop」を検索
   - クリックして起動

### 手順2: 利用規約
1. 利用規約が表示されたら内容を確認
2. 「I accept the terms」にチェック
3. 「Accept」をクリック

### 手順3: 初期設定完了
1. 「Docker Desktop is running」と表示されたら準備完了
2. タスクトレイ（画面右下）にクジラのアイコンが表示されます

![Docker Desktop Running](https://docs.docker.com/desktop/images/docker-desktop-running.png)
*※ クジラアイコンが緑色なら正常動作中*

---

## 動作確認

### 手順1: コマンドプロンプトで確認
1. `Windows キー + R` を押す
2. `cmd` と入力して Enter
3. 以下のコマンドを実行：
   ```
   docker --version
   ```
4. バージョン情報が表示されれば成功

### 手順2: Docker Desktop の設定確認
1. タスクトレイのクジラアイコンを右クリック
2. 「Settings」をクリック
3. 以下を確認：
   - General: Start Docker Desktop when you log in ✅
   - Resources: Memory 2GB以上割り当て

---

## よくある質問

### Q1: WSL 2 のインストールが必要と表示される
**A**: 以下の手順で WSL 2 をインストールしてください
1. 管理者権限でコマンドプロンプトを開く
2. 以下のコマンドを実行：
   ```
   wsl --install
   ```
3. パソコンを再起動
4. Docker Desktop を再度起動

### Q2: 仮想化が有効でないとエラーが出る
**A**: BIOS で仮想化を有効にする必要があります
1. パソコンを再起動
2. 起動時に F2 または Del キーを押して BIOS に入る
3. 「Virtualization Technology」を「Enabled」に変更
4. 設定を保存して再起動

### Q3: Docker Desktop が起動しない
**A**: 以下を順番に試してください
1. パソコンを再起動
2. ウイルス対策ソフトを一時的に無効化
3. Docker Desktop を再インストール

### Q4: 「Docker daemon is not running」エラー
**A**: Docker サービスを手動で起動
1. タスクトレイのクジラアイコンを右クリック
2. 「Restart」をクリック
3. 30秒ほど待つ

---

## 🎉 インストール完了！

Docker Desktop のインストールが完了しました。
Business Data Processor を使用する準備が整いました。

次のステップ：
1. Business Data Processor のフォルダに移動
2. 「🚀起動.bat」をダブルクリック
3. アプリケーションが自動的に起動します

---

## サポート情報

### Docker 公式ドキュメント
- [Docker Desktop for Windows](https://docs.docker.com/desktop/windows/install/)
- [トラブルシューティング](https://docs.docker.com/desktop/troubleshoot/overview/)

### 問題が解決しない場合
1. エラーメッセージをメモ
2. Windows のバージョンを確認
3. システム管理者に相談