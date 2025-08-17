#!/usr/bin/env python3
"""
Simple LocalStack Test Runner

This is a simpler version that just manages LocalStack container lifecycle
without requiring additional dependencies.
"""

import sys
import time
import subprocess
import json
from pathlib import Path


def run_command(cmd, capture_output=True):
    """Run a command and return the result."""
    try:
        result = subprocess.run(cmd, capture_output=capture_output, text=True, check=True)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(cmd)}")
        if e.stdout:
            print(f"Stdout: {e.stdout}")
        if e.stderr:
            print(f"Stderr: {e.stderr}")
        return None


def check_localstack():
    """Check if LocalStack is running."""
    try:
        result = run_command(["curl", "-s", "http://localhost:4566/_localstack/health"])
        if result and result.stdout:
            health = json.loads(result.stdout)
            dynamodb_status = health.get("services", {}).get("dynamodb", "")
            return dynamodb_status in ["available", "running"]
    except:
        pass
    return False


def start_localstack():
    """Start LocalStack container."""
    print("🚀 Starting LocalStack...")
    
    project_root = Path(__file__).parent.parent
    compose_file = project_root / "docker-compose.localstack.yml"
    
    # Start container
    result = run_command([
        "docker-compose", "-f", str(compose_file), "up", "-d"
    ], capture_output=False)
    
    if result is None:
        print("❌ Failed to start LocalStack")
        return False
    
    # Wait for health
    print("⏳ Waiting for LocalStack to be ready...")
    for i in range(30):
        if check_localstack():
            print("✅ LocalStack is ready!")
            return True
        time.sleep(2)
        if i % 5 == 0:
            print(f"   Still waiting... ({i+1}/30)")
    
    print("❌ LocalStack failed to start")
    return False


def stop_localstack():
    """Stop LocalStack container."""
    print("🛑 Stopping LocalStack...")
    
    project_root = Path(__file__).parent.parent
    compose_file = project_root / "docker-compose.localstack.yml"
    
    result = run_command([
        "docker-compose", "-f", str(compose_file), "down"
    ], capture_output=False)
    
    if result:
        print("✅ LocalStack stopped")
        return True
    else:
        print("❌ Failed to stop LocalStack")
        return False


def status_localstack():
    """Show LocalStack status."""
    if check_localstack():
        print("✅ LocalStack is running and healthy")
    else:
        print("❌ LocalStack is not running or not healthy")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/localstack_simple.py {start|stop|status}")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "start":
        success = start_localstack()
    elif command == "stop":
        success = stop_localstack()
    elif command == "status":
        status_localstack()
        success = True
    else:
        print(f"Unknown command: {command}")
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()