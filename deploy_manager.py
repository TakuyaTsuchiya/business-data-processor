#!/usr/bin/env python3
"""
Business Data Processor - Deploy Manager
Claude Codeから簡単にVPSへデプロイするためのツール
"""

import subprocess
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
import json
import getpass

# カラー出力
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

class DeployManager:
    def __init__(self):
        self.config_file = Path('.env.deploy')
        self.config = self.load_config()
        # 解決済みのSSH鍵パスを決定
        self.ssh_key = self.resolve_ssh_key()
        
    def load_config(self):
        """設定ファイルを読み込む"""
        config = {}
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        config[key] = value
        return config

    def resolve_ssh_key(self) -> Path:
        """SSH鍵の場所を解決（設定 > 既定 > フォールバック）"""
        # 1) 設定で明示されていれば最優先
        if 'SSH_KEY_PATH' in self.config and self.config['SSH_KEY_PATH']:
            return Path(os.path.expanduser(self.config['SSH_KEY_PATH']))

        # 2) 既定のデプロイ鍵
        default_key = Path.home() / '.ssh' / 'vps-deploy-key'
        if default_key.exists():
            return default_key

        # 3) よくある既存鍵へフォールバック
        candidates = [
            Path.home() / '.ssh' / 'id_ed25519',
            Path.home() / '.ssh' / 'id_rsa',
        ]
        for p in candidates:
            if p.exists():
                # 見つかった鍵を使用
                return p

        # 4) 見つからなければ既定のパスを返す（setupで作成させる）
        return default_key
    
    def save_config(self):
        """設定を保存する"""
        with open(self.config_file, 'w') as f:
            for key, value in self.config.items():
                f.write(f"{key}={value}\n")
    
    def print_header(self, message):
        print(f"\n{BLUE}{'='*60}")
        print(f"  {message}")
        print(f"{'='*60}{RESET}\n")
    
    def print_success(self, message):
        print(f"{GREEN}✓ {message}{RESET}")
    
    def print_error(self, message):
        print(f"{RED}✗ {message}{RESET}")
    
    def print_warning(self, message):
        print(f"{YELLOW}⚠ {message}{RESET}")
    
    def print_info(self, message):
        print(f"{BLUE}ℹ {message}{RESET}")
    
    def setup(self):
        """初期セットアップ"""
        self.print_header("VPS Deploy Setup")
        
        # VPS情報の入力
        print("Please enter your VPS information:")
        
        vps_host = input(f"VPS IP/Hostname [{self.config.get('VPS_HOST', '')}]: ").strip()
        if not vps_host and 'VPS_HOST' in self.config:
            vps_host = self.config['VPS_HOST']
        
        vps_user = input(f"VPS Username [{self.config.get('VPS_USER', 'ubuntu')}]: ").strip()
        if not vps_user:
            vps_user = self.config.get('VPS_USER', 'ubuntu')
        
        vps_port = input(f"SSH Port [{self.config.get('VPS_PORT', '22')}]: ").strip()
        if not vps_port:
            vps_port = self.config.get('VPS_PORT', '22')
        
        project_path = input(f"Project path on VPS [{self.config.get('PROJECT_PATH', f'/home/{vps_user}/business-data-processor')}]: ").strip()
        if not project_path:
            project_path = self.config.get('PROJECT_PATH', f'/home/{vps_user}/business-data-processor')
        
        # 設定を保存
        self.config.update({
            'VPS_HOST': vps_host,
            'VPS_USER': vps_user,
            'VPS_PORT': vps_port,
            'PROJECT_PATH': project_path
        })
        self.save_config()
        
        self.print_success("Configuration saved to .env.deploy")
        
        # SSH鍵のセットアップ
        if not self.ssh_key.exists():
            self.print_warning(f"SSH key not found at {self.ssh_key}")
            create_key = input("Create new SSH key? (y/n): ").lower() == 'y'
            
            if create_key:
                self.print_info("Creating SSH key...")
                subprocess.run([
                    'ssh-keygen', '-t', 'ed25519', 
                    '-f', str(self.ssh_key), 
                    '-N', ''
                ])
                
                self.print_info("Copy the following public key to your VPS:")
                print(f"\n{YELLOW}")
                with open(f"{self.ssh_key}.pub", 'r') as f:
                    print(f.read())
                print(f"{RESET}\n")
                
                input("Press Enter after you've added the key to your VPS...")
        
        # 接続テスト
        self.print_info("Testing SSH connection...")
        result = self.ssh_command("echo 'Connection successful!'", capture_output=True)
        
        if result.returncode == 0:
            self.print_success("SSH connection successful!")
        else:
            self.print_error("SSH connection failed!")
            print(result.stderr)
            
    def ssh_command(self, command, capture_output=False):
        """SSH経由でコマンドを実行（鍵を自動的に選択して試行）"""
        host = f"{self.config['VPS_USER']}@{self.config['VPS_HOST']}"
        port = self.config.get('VPS_PORT', '22')

        # 候補鍵の順序: 現在選択 > 設定のSSH_KEY_PATH > 既定 > フォールバック
        seen = set()
        candidates = []
        def add(p):
            s = str(p)
            if s not in seen:
                candidates.append(p)
                seen.add(s)

        add(self.ssh_key)
        if 'SSH_KEY_PATH' in self.config and self.config['SSH_KEY_PATH']:
            add(Path(os.path.expanduser(self.config['SSH_KEY_PATH'])))
        add(Path.home() / '.ssh' / 'vps-deploy-key')
        add(Path.home() / '.ssh' / 'id_ed25519')
        add(Path.home() / '.ssh' / 'id_rsa')

        chosen_key = None
        # まずは接続テストで使える鍵を特定
        for key in candidates:
            if not key.exists():
                continue
            test_cmd = [
                'ssh', '-i', str(key), '-p', port,
                '-o', 'BatchMode=yes',
                '-o', 'PasswordAuthentication=no',
                '-o', 'StrictHostKeyChecking=no',
                host, 'true'
            ]
            res = subprocess.run(test_cmd)
            if res.returncode == 0:
                chosen_key = key
                break

        # 使える鍵が見つからない場合はそのまま現在の鍵で実行（エラーを返す）
        key_to_use = chosen_key or self.ssh_key

        # 実コマンドを実行
        ssh_cmd = [
            'ssh',
            '-i', str(key_to_use),
            '-p', port,
            '-o', 'StrictHostKeyChecking=no',
            host,
            command
        ]

        result = subprocess.run(ssh_cmd, capture_output=capture_output, text=True) if capture_output else subprocess.run(ssh_cmd)

        # 成功した鍵を設定に保存（次回以降の接続を安定化）
        if chosen_key and str(chosen_key) != self.config.get('SSH_KEY_PATH'):
            self.config['SSH_KEY_PATH'] = str(chosen_key)
            # ベストエフォートで保存（書けない場合は無視）
            try:
                self.save_config()
            except Exception:
                pass

        return result
    
    def deploy(self, skip_git_pull=False):
        """VPSへデプロイ"""
        self.print_header("Deploying to VPS")
        
        if not self.config.get('VPS_HOST'):
            self.print_error("VPS not configured. Please run 'setup' first.")
            return False
        
        # デプロイ手順
        steps = []
        
        if not skip_git_pull:
            # ローカル変更（例: nginx/upstream.conf の書き換え）で pull が失敗しないように堅牢化
            # 1) 通常の pull を試し、失敗したら安全なハードリセットで最新化
            git_update_cmd = (
                f"cd {self.config['PROJECT_PATH']} && "
                # 事前に tracked ファイルのローカル変更を破棄したい場合は以下を有効化:
                # "git restore --staged --worktree nginx/upstream.conf >/dev/null 2>&1; "
                "git pull --rebase --autostash origin main || "
                "(echo '[INFO] git pull failed. Falling back to fetch+reset...' && "
                "git fetch origin main && git reset --hard origin/main)"
            )
            steps.append(("Pulling latest code from Git", git_update_cmd))
        
        steps.extend([
            ("Running Blue-Green deployment", 
             f"cd {self.config['PROJECT_PATH']} && ./deploy/deploy.sh"),
        ])
        
        # 各ステップを実行
        for step_name, command in steps:
            self.print_info(f"{step_name}...")
            result = self.ssh_command(command, capture_output=True)
            
            if result.returncode == 0:
                self.print_success(f"{step_name} - completed")
                if result.stdout:
                    print(result.stdout)
            else:
                self.print_error(f"{step_name} - failed")
                print(result.stderr)
                return False
        
        # スモークテスト
        self.print_info("Running smoke test...")
        result = self.ssh_command(
            f"cd {self.config['PROJECT_PATH']} && python3 deploy/smoke-test.py",
            capture_output=True
        )
        
        if result.returncode == 0:
            self.print_success("Smoke test passed!")
            print(result.stdout)
        else:
            self.print_warning("Smoke test had issues")
            print(result.stdout)
        
        self.print_success("Deployment completed!")
        return True
    
    def rollback(self):
        """ロールバック実行"""
        self.print_header("Rolling back deployment")
        
        result = self.ssh_command(
            f"cd {self.config['PROJECT_PATH']} && ./deploy/rollback.sh",
            capture_output=True
        )
        
        if result.returncode == 0:
            self.print_success("Rollback completed!")
            print(result.stdout)
        else:
            self.print_error("Rollback failed!")
            print(result.stderr)
    
    def status(self):
        """デプロイ状態を確認"""
        self.print_header("Deployment Status")
        
        # ヘルスチェック実行
        result = self.ssh_command(
            f"cd {self.config['PROJECT_PATH']} && ./deploy/health-check.sh",
            capture_output=True
        )
        
        print(result.stdout)
        
        # Dockerコンテナの状態
        self.print_info("\nDocker containers:")
        result = self.ssh_command(
            "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'",
            capture_output=True
        )
        print(result.stdout)
        
    def logs(self, environment='active', lines=50):
        """ログを表示"""
        self.print_header(f"Logs from {environment} environment")
        
        if environment == 'active':
            # 現在アクティブな環境を特定
            result = self.ssh_command(
                f"grep -oP 'server\\s+app-\\K(blue|green)' {self.config['PROJECT_PATH']}/nginx/upstream.conf | head -1",
                capture_output=True
            )
            environment = result.stdout.strip()
        
        # ログ表示
        result = self.ssh_command(
            f"docker logs --tail {lines} bdp-{environment}",
            capture_output=True
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)

def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='Business Data Processor Deploy Manager')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # setup コマンド
    setup_parser = subparsers.add_parser('setup', help='Setup VPS configuration')
    
    # deploy コマンド
    deploy_parser = subparsers.add_parser('deploy', help='Deploy to VPS')
    deploy_parser.add_argument('--skip-git', action='store_true', help='Skip git pull')
    
    # rollback コマンド
    rollback_parser = subparsers.add_parser('rollback', help='Rollback to previous version')
    
    # status コマンド
    status_parser = subparsers.add_parser('status', help='Check deployment status')
    
    # logs コマンド
    logs_parser = subparsers.add_parser('logs', help='Show application logs')
    logs_parser.add_argument('--env', choices=['active', 'blue', 'green'], 
                           default='active', help='Which environment')
    logs_parser.add_argument('--lines', type=int, default=50, 
                           help='Number of lines to show')
    
    args = parser.parse_args()
    
    # デプロイマネージャーインスタンス作成
    manager = DeployManager()
    
    # コマンド実行
    if args.command == 'setup':
        manager.setup()
    elif args.command == 'deploy':
        manager.deploy(skip_git_pull=args.skip_git)
    elif args.command == 'rollback':
        manager.rollback()
    elif args.command == 'status':
        manager.status()
    elif args.command == 'logs':
        manager.logs(environment=args.env, lines=args.lines)
    else:
        # コマンドが指定されていない場合はインタラクティブモード
        print(f"{BLUE}Business Data Processor - Deploy Manager{RESET}")
        print("\nAvailable commands:")
        print("  1. setup    - Configure VPS connection")
        print("  2. deploy   - Deploy to VPS")
        print("  3. rollback - Rollback to previous version")
        print("  4. status   - Check deployment status")
        print("  5. logs     - View application logs")
        print("  6. exit     - Exit")
        
        while True:
            choice = input("\nSelect command (1-6): ").strip()
            
            if choice == '1':
                manager.setup()
            elif choice == '2':
                manager.deploy()
            elif choice == '3':
                manager.rollback()
            elif choice == '4':
                manager.status()
            elif choice == '5':
                manager.logs()
            elif choice == '6':
                break
            else:
                print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()
