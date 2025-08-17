#!/usr/bin/env python3
"""
LocalStack Test Runner for DynamoDB Wrapper V2

This script manages LocalStack container lifecycle and runs integration tests.
Provides convenient commands for development and CI/CD workflows.

Usage:
    python scripts/run_localstack_tests.py [command] [options]

Commands:
    start       - Start LocalStack container
    stop        - Stop LocalStack container  
    restart     - Restart LocalStack container
    test        - Run integration tests (starts LocalStack if needed)
    test-unit   - Run unit tests only (no LocalStack required)
    test-all    - Run both unit and integration tests
    status      - Check LocalStack container status
    logs        - Show LocalStack container logs
    clean       - Stop container and remove volumes
"""

import sys
import time
import subprocess
import requests
import argparse
from pathlib import Path
from typing import Optional, List


class LocalStackTestRunner:
    """Manages LocalStack container and test execution."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.compose_file = self.project_root / "docker-compose.localstack.yml"
        self.localstack_url = "http://localhost:4566"
        
    def run_command(self, cmd: List[str], capture_output: bool = True, check: bool = True) -> subprocess.CompletedProcess:
        """Run a shell command with error handling."""
        try:
            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd, 
                capture_output=capture_output, 
                text=True, 
                check=check,
                cwd=self.project_root
            )
            
            if not capture_output:
                return result
                
            if result.stdout:
                print(f"Output: {result.stdout.strip()}")
            if result.stderr:
                print(f"Error: {result.stderr.strip()}")
                
            return result
            
        except subprocess.CalledProcessError as e:
            print(f"Command failed with exit code {e.returncode}")
            if e.stdout:
                print(f"Stdout: {e.stdout}")
            if e.stderr:
                print(f"Stderr: {e.stderr}")
            if check:
                raise
            return e
    
    def check_localstack_health(self) -> bool:
        """Check if LocalStack is running and healthy."""
        try:
            response = requests.get(f"{self.localstack_url}/_localstack/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                # Check if DynamoDB service is available
                services = health_data.get("services", {})
                dynamodb_status = services.get("dynamodb")
                print(f"LocalStack health check - DynamoDB status: {dynamodb_status}")
                return dynamodb_status in ["available", "running"]
            return False
        except requests.RequestException as e:
            print(f"LocalStack health check failed: {e}")
            return False
    
    def wait_for_localstack(self, max_retries: int = 60, delay: float = 2.0) -> bool:
        """Wait for LocalStack to be ready."""
        print("Waiting for LocalStack to be ready...")
        
        for attempt in range(max_retries):
            if self.check_localstack_health():
                print("✅ LocalStack is ready!")
                return True
            
            if attempt < 5:
                print(f"⏳ Attempt {attempt + 1}/{max_retries}: LocalStack starting...")
            elif attempt % 5 == 0:
                print(f"⏳ Attempt {attempt + 1}/{max_retries}: Still waiting for LocalStack...")
            
            time.sleep(delay)
        
        print("❌ LocalStack failed to start within the expected time")
        return False
    
    def get_container_status(self) -> Optional[str]:
        """Get LocalStack container status."""
        try:
            result = self.run_command([
                "docker-compose", "-f", str(self.compose_file), "ps", "-q"
            ])
            
            if not result.stdout.strip():
                return "stopped"
            
            container_id = result.stdout.strip()
            result = self.run_command([
                "docker", "inspect", container_id, 
                "--format", "{{.State.Status}}"
            ])
            
            return result.stdout.strip()
            
        except (subprocess.CalledProcessError, Exception):
            return "unknown"
    
    def start_localstack(self) -> bool:
        """Start LocalStack container."""
        print("🚀 Starting LocalStack container...")
        
        status = self.get_container_status()
        if status == "running":
            print("✅ LocalStack is already running")
            return self.check_localstack_health()
        
        try:
            self.run_command([
                "docker-compose", "-f", str(self.compose_file), "up", "-d"
            ])
            
            return self.wait_for_localstack()
            
        except subprocess.CalledProcessError:
            print("❌ Failed to start LocalStack container")
            return False
    
    def stop_localstack(self) -> bool:
        """Stop LocalStack container."""
        print("🛑 Stopping LocalStack container...")
        
        try:
            self.run_command([
                "docker-compose", "-f", str(self.compose_file), "down"
            ])
            print("✅ LocalStack container stopped")
            return True
            
        except subprocess.CalledProcessError:
            print("❌ Failed to stop LocalStack container")
            return False
    
    def restart_localstack(self) -> bool:
        """Restart LocalStack container."""
        print("🔄 Restarting LocalStack container...")
        self.stop_localstack()
        time.sleep(2)
        return self.start_localstack()
    
    def show_status(self) -> None:
        """Show LocalStack container status."""
        container_status = self.get_container_status()
        health_status = "healthy" if self.check_localstack_health() else "unhealthy"
        
        print(f"📊 LocalStack Status:")
        print(f"   Container: {container_status}")
        print(f"   Health: {health_status}")
        
        if container_status == "running":
            print(f"   URL: {self.localstack_url}")
            print(f"   Health Check: {self.localstack_url}/_localstack/health")
    
    def show_logs(self, tail: int = 50) -> None:
        """Show LocalStack container logs."""
        print(f"📝 LocalStack logs (last {tail} lines):")
        
        try:
            self.run_command([
                "docker-compose", "-f", str(self.compose_file), 
                "logs", "--tail", str(tail), "localstack"
            ], capture_output=False)
        except subprocess.CalledProcessError:
            print("❌ Failed to retrieve logs")
    
    def clean_localstack(self) -> bool:
        """Stop LocalStack and clean up volumes."""
        print("🧹 Cleaning up LocalStack...")
        
        try:
            self.run_command([
                "docker-compose", "-f", str(self.compose_file), 
                "down", "-v", "--remove-orphans"
            ])
            
            # Also remove any dangling volumes
            self.run_command([
                "docker", "volume", "prune", "-f"
            ], check=False)
            
            print("✅ LocalStack cleanup completed")
            return True
            
        except subprocess.CalledProcessError:
            print("❌ Failed to clean up LocalStack")
            return False
    
    def run_unit_tests(self, verbose: bool = False, pattern: Optional[str] = None) -> bool:
        """Run unit tests (no LocalStack required)."""
        print("🧪 Running unit tests...")
        
        cmd = ["uv", "run", "pytest", "tests/unit/"]
        
        if verbose:
            cmd.append("-v")
        
        if pattern:
            cmd.extend(["-k", pattern])
        
        try:
            self.run_command(cmd, capture_output=False)
            print("✅ Unit tests completed")
            return True
        except subprocess.CalledProcessError:
            print("❌ Unit tests failed")
            return False
    
    def run_integration_tests(self, verbose: bool = False, pattern: Optional[str] = None) -> bool:
        """Run integration tests (requires LocalStack)."""
        print("🧪 Running integration tests...")
        
        # Ensure LocalStack is running
        if not self.check_localstack_health():
            print("LocalStack not running, starting it...")
            if not self.start_localstack():
                return False
        
        cmd = ["uv", "run", "pytest", "tests/integration/"]
        
        if verbose:
            cmd.append("-v")
        
        if pattern:
            cmd.extend(["-k", pattern])
        
        try:
            self.run_command(cmd, capture_output=False)
            print("✅ Integration tests completed")
            return True
        except subprocess.CalledProcessError:
            print("❌ Integration tests failed")
            return False
    
    def run_all_tests(self, verbose: bool = False, pattern: Optional[str] = None) -> bool:
        """Run both unit and integration tests."""
        print("🧪 Running all tests...")
        
        # Run unit tests first (faster feedback)
        if not self.run_unit_tests(verbose, pattern):
            return False
        
        # Then run integration tests
        return self.run_integration_tests(verbose, pattern)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="LocalStack Test Runner for DynamoDB Wrapper V2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "command",
        choices=["start", "stop", "restart", "test", "test-unit", "test-all", "status", "logs", "clean"],
        help="Command to execute"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose test output"
    )
    
    parser.add_argument(
        "-k", "--pattern",
        help="Test pattern filter (pytest -k)"
    )
    
    parser.add_argument(
        "--tail",
        type=int,
        default=50,
        help="Number of log lines to show (default: 50)"
    )
    
    args = parser.parse_args()
    
    runner = LocalStackTestRunner()
    success = True
    
    try:
        if args.command == "start":
            success = runner.start_localstack()
            
        elif args.command == "stop":
            success = runner.stop_localstack()
            
        elif args.command == "restart":
            success = runner.restart_localstack()
            
        elif args.command == "test":
            success = runner.run_integration_tests(args.verbose, args.pattern)
            
        elif args.command == "test-unit":
            success = runner.run_unit_tests(args.verbose, args.pattern)
            
        elif args.command == "test-all":
            success = runner.run_all_tests(args.verbose, args.pattern)
            
        elif args.command == "status":
            runner.show_status()
            
        elif args.command == "logs":
            runner.show_logs(args.tail)
            
        elif args.command == "clean":
            success = runner.clean_localstack()
    
    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user")
        success = False
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()