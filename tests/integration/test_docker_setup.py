#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Business Data Processor - Docker環境テストスクリプト
全15種類のプロセッサーと主要機能の動作確認を行います
"""

import os
import sys
import time
import subprocess
import json
from datetime import datetime

class DockerTester:
    def __init__(self):
        self.results = []
        self.start_time = datetime.now()
        
    def log(self, message, status="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{status}] {message}")
        self.results.append({
            "time": timestamp,
            "status": status,
            "message": message
        })
    
    def run_command(self, command, description):
        """コマンドを実行して結果を返す"""
        self.log(f"実行中: {description}")
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=60
            )
            if result.returncode == 0:
                self.log(f"✅ 成功: {description}", "SUCCESS")
                return True, result.stdout
            else:
                self.log(f"❌ 失敗: {description} - {result.stderr}", "ERROR")
                return False, result.stderr
        except subprocess.TimeoutExpired:
            self.log(f"⏱️ タイムアウト: {description}", "ERROR")
            return False, "Timeout"
        except Exception as e:
            self.log(f"❌ エラー: {description} - {str(e)}", "ERROR")
            return False, str(e)
    
    def test_docker_installation(self):
        """Docker Desktop のインストール確認"""
        self.log("=== Docker Desktop インストール確認 ===")
        
        # Docker バージョン確認
        success, output = self.run_command(
            "docker --version",
            "Docker バージョン確認"
        )
        if success:
            self.log(f"Docker バージョン: {output.strip()}")
        
        # Docker Compose バージョン確認
        success, output = self.run_command(
            "docker-compose --version",
            "Docker Compose バージョン確認"
        )
        if success:
            self.log(f"Docker Compose バージョン: {output.strip()}")
        
        # Docker デーモン確認
        success, _ = self.run_command(
            "docker ps",
            "Docker デーモン動作確認"
        )
        
        return success
    
    def test_container_build(self):
        """コンテナのビルドテスト"""
        self.log("=== コンテナビルドテスト ===")
        
        # ビルド実行
        success, _ = self.run_command(
            "docker-compose build --no-cache",
            "コンテナイメージのビルド"
        )
        
        if success:
            # イメージ確認
            success, output = self.run_command(
                "docker images | grep business-data-processor",
                "ビルドされたイメージの確認"
            )
            if success and output:
                self.log("ビルドされたイメージ:")
                self.log(output.strip())
        
        return success
    
    def test_container_startup(self):
        """コンテナの起動テスト"""
        self.log("=== コンテナ起動テスト ===")
        
        # 既存コンテナの停止
        self.run_command(
            "docker-compose down",
            "既存コンテナの停止"
        )
        
        # コンテナ起動
        success, _ = self.run_command(
            "docker-compose up -d",
            "コンテナの起動"
        )
        
        if success:
            # 起動待機
            self.log("コンテナの起動を待っています...")
            time.sleep(10)
            
            # ヘルスチェック
            for i in range(6):  # 最大30秒待機
                success, _ = self.run_command(
                    'docker exec business-data-processor curl -f http://localhost:8501/_stcore/health 2>/dev/null',
                    f"ヘルスチェック (試行 {i+1}/6)"
                )
                if success:
                    break
                time.sleep(5)
            
            # コンテナ状態確認
            success, output = self.run_command(
                "docker ps --filter name=business-data-processor",
                "実行中のコンテナ確認"
            )
            if success and output:
                self.log("実行中のコンテナ:")
                self.log(output.strip())
        
        return success
    
    def test_volume_mounts(self):
        """ボリュームマウントのテスト"""
        self.log("=== ボリュームマウントテスト ===")
        
        # テストファイル作成
        test_dirs = ["data", "downloads", "logs"]
        for dir_name in test_dirs:
            os.makedirs(dir_name, exist_ok=True)
            test_file = f"{dir_name}/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(test_file, "w", encoding="utf-8") as f:
                f.write(f"テストファイル - {dir_name}")
            
            # コンテナ内から確認
            success, output = self.run_command(
                f'docker exec business-data-processor ls -la /app/{dir_name}/',
                f"{dir_name} フォルダのマウント確認"
            )
            
            if success and "test_" in output:
                self.log(f"✅ {dir_name} フォルダは正常にマウントされています")
            else:
                self.log(f"❌ {dir_name} フォルダのマウントに問題があります", "ERROR")
                return False
        
        return True
    
    def test_japanese_environment(self):
        """日本語環境のテスト"""
        self.log("=== 日本語環境テスト ===")
        
        # ロケール確認
        success, output = self.run_command(
            'docker exec business-data-processor locale',
            "ロケール設定確認"
        )
        
        if success:
            self.log("ロケール設定:")
            self.log(output.strip())
            
            # 日本語が含まれているか確認
            if "ja_JP.UTF-8" in output:
                self.log("✅ 日本語環境が正しく設定されています")
            else:
                self.log("⚠️ 日本語環境の設定に問題がある可能性があります", "WARNING")
        
        # Python での日本語処理テスト
        test_command = '''docker exec business-data-processor python -c "
import sys
print(f'Python encoding: {sys.getdefaultencoding()}')
print(f'stdout encoding: {sys.stdout.encoding}')
test_str = 'テスト文字列：あいうえお漢字'
print(f'Test string: {test_str}')
"'''
        
        success, output = self.run_command(
            test_command,
            "Python 日本語処理テスト"
        )
        
        if success and "テスト文字列" in output:
            self.log("✅ Python での日本語処理は正常です")
            return True
        else:
            self.log("❌ Python での日本語処理に問題があります", "ERROR")
            return False
    
    def test_processor_imports(self):
        """プロセッサーモジュールのインポートテスト"""
        self.log("=== プロセッサーインポートテスト ===")
        
        # 15種類のプロセッサーをテスト
        processors = [
            # ミライル（6種類）
            ("mirail_contract_without10k", "from processors.mirail_autocall.contract.without10k_refactored import process_mirail_contract_without10k_data"),
            ("mirail_contract_with10k", "from processors.mirail_autocall.contract.with10k_refactored import process_mirail_contract_with10k_data"),
            ("mirail_guarantor_without10k", "from processors.mirail_autocall.guarantor.without10k_refactored import process_mirail_guarantor_without10k_data"),
            ("mirail_guarantor_with10k", "from processors.mirail_autocall.guarantor.with10k_refactored import process_mirail_guarantor_with10k_data"),
            ("mirail_emergencycontact_without10k", "from processors.mirail_autocall.emergency_contact.without10k_refactored import process_mirail_emergency_contact_without10k_data"),
            ("mirail_emergencycontact_with10k", "from processors.mirail_autocall.emergency_contact.with10k_refactored import process_mirail_emergency_contact_with10k_data"),
            
            # フェイス（3種類）
            ("faith_contract", "from processors.faith_autocall.contract.standard import process_faith_contract_data"),
            ("faith_guarantor", "from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data"),
            ("faith_emergencycontact", "from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data"),
            
            # プラザ（3種類）
            ("plaza_main", "from processors.plaza_autocall.main.standard import process_plaza_main_data"),
            ("plaza_guarantor", "from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data"),
            ("plaza_contact", "from processors.plaza_autocall.contact.standard import process_plaza_contact_data"),
            
            # アーク（1種類）
            ("ark_registration", "from processors.ark_registration import process_ark_data"),
        ]
        
        all_success = True
        for name, import_statement in processors:
            test_command = f'''docker exec business-data-processor python -c "{import_statement}; print('✅ {name} import success')"'''
            
            success, output = self.run_command(
                test_command,
                f"{name} インポートテスト"
            )
            
            if not success:
                all_success = False
                self.log(f"❌ {name} のインポートに失敗しました", "ERROR")
        
        if all_success:
            self.log("✅ すべてのプロセッサーが正常にインポートできました")
        
        return all_success
    
    def test_web_access(self):
        """Web アクセステスト"""
        self.log("=== Web アクセステスト ===")
        
        # curl でアクセステスト
        success, output = self.run_command(
            "curl -s -o /dev/null -w '%{http_code}' http://localhost:8501",
            "Web UI アクセステスト"
        )
        
        if success and "200" in output:
            self.log("✅ Web UI (http://localhost:8501) にアクセス可能です")
            return True
        else:
            self.log("❌ Web UI にアクセスできません", "ERROR")
            return False
    
    def generate_report(self):
        """テスト結果レポートの生成"""
        self.log("=== テスト結果サマリー ===")
        
        # 結果集計
        success_count = sum(1 for r in self.results if r["status"] == "SUCCESS")
        error_count = sum(1 for r in self.results if r["status"] == "ERROR")
        warning_count = sum(1 for r in self.results if r["status"] == "WARNING")
        
        # 実行時間
        duration = datetime.now() - self.start_time
        
        # サマリー表示
        print("\n" + "="*60)
        print("テスト完了")
        print("="*60)
        print(f"成功: {success_count} 件")
        print(f"エラー: {error_count} 件")
        print(f"警告: {warning_count} 件")
        print(f"実行時間: {duration}")
        print("="*60)
        
        # レポートファイル保存
        report_file = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump({
                "test_date": self.start_time.isoformat(),
                "duration": str(duration),
                "summary": {
                    "success": success_count,
                    "error": error_count,
                    "warning": warning_count
                },
                "results": self.results
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n詳細レポート: {report_file}")
        
        return error_count == 0
    
    def cleanup(self):
        """テスト環境のクリーンアップ"""
        self.log("=== クリーンアップ ===")
        self.run_command(
            "docker-compose down",
            "コンテナの停止と削除"
        )

def main():
    """メインテスト実行"""
    print("Business Data Processor - Docker環境テスト")
    print("="*60)
    
    tester = DockerTester()
    
    # テスト実行
    tests = [
        ("Docker インストール確認", tester.test_docker_installation),
        ("コンテナビルド", tester.test_container_build),
        ("コンテナ起動", tester.test_container_startup),
        ("ボリュームマウント", tester.test_volume_mounts),
        ("日本語環境", tester.test_japanese_environment),
        ("プロセッサーインポート", tester.test_processor_imports),
        ("Web アクセス", tester.test_web_access),
    ]
    
    all_success = True
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"テスト: {test_name}")
        print('='*60)
        
        try:
            success = test_func()
            if not success:
                all_success = False
        except Exception as e:
            tester.log(f"テスト実行エラー: {str(e)}", "ERROR")
            all_success = False
    
    # レポート生成
    tester.generate_report()
    
    # クリーンアップ
    tester.cleanup()
    
    # 終了コード
    sys.exit(0 if all_success else 1)

if __name__ == "__main__":
    main()