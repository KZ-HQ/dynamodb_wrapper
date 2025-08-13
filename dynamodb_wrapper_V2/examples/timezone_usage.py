#!/usr/bin/env python3
"""
Simple timezone usage examples for the DynamoDB wrapper library.

This example demonstrates basic Python datetime usage with the simplified
timezone utilities provided by the DynamoDB wrapper.
"""

from datetime import datetime, timezone
from dynamodb_wrapper.utils import (
    to_utc,
    ensure_timezone_aware,
    to_user_timezone,
)


def demonstrate_basic_timezone_operations():
    """Demonstrate basic timezone operations with simplified functions."""
    print("=== Basic Timezone Operations ===")
    
    # 1. Get current UTC time using Python's built-in
    now_utc = datetime.now(timezone.utc)
    print(f"Current UTC time: {now_utc}")
    
    # 2. Convert UTC to different user timezones
    eastern_time = to_user_timezone(now_utc, "America/New_York")
    pacific_time = to_user_timezone(now_utc, "America/Los_Angeles") 
    tokyo_time = to_user_timezone(now_utc, "Asia/Tokyo")
    
    print(f"Eastern time: {eastern_time}")
    print(f"Pacific time: {pacific_time}")
    print(f"Tokyo time: {tokyo_time}")


def demonstrate_architectural_contract():
    """Demonstrate the architectural contract: naive datetime = UTC."""
    print("\n=== Architectural Contract: Naive Datetime = UTC ===")
    
    # This is the key difference from Python's built-in behavior
    naive_dt = datetime(2024, 1, 1, 10, 0, 0)
    print(f"Naive datetime: {naive_dt}")
    
    # Python built-in treats naive as local timezone
    try:
        python_result = naive_dt.astimezone(timezone.utc)
        print(f"Python built-in result: {python_result} (treats naive as local time)")
    except:
        print("Python built-in might fail on some systems")
    
    # Our function treats naive as UTC (architectural contract)
    our_result = to_utc(naive_dt)
    print(f"DynamoDB wrapper result: {our_result} (treats naive as UTC)")
    
    print("This enforces the architectural boundary that naive datetimes = UTC")


def demonstrate_round_trip_conversion():
    """Demonstrate round-trip timezone conversion."""
    print("\n=== Round-Trip Timezone Conversion ===")
    
    # Start with UTC
    utc_dt = datetime.now(timezone.utc)
    print(f"1. UTC datetime: {utc_dt}")
    
    # Convert to user timezone
    user_dt = to_user_timezone(utc_dt, "America/Chicago")
    print(f"2. Chicago time: {user_dt}")
    
    # Convert back to UTC
    back_to_utc = to_utc(user_dt)
    print(f"3. Back to UTC: {back_to_utc}")
    
    # Verify they represent the same moment
    time_diff = abs((back_to_utc - utc_dt).total_seconds())
    print(f"Time difference: {time_diff} seconds (should be ~0)")


def main():
    """Run timezone usage examples."""
    print("🌍 DynamoDB Wrapper V2 - Simplified Timezone Usage")
    print("=" * 55)
    
    try:
        demonstrate_basic_timezone_operations()
        demonstrate_architectural_contract()
        demonstrate_round_trip_conversion()
        
        print("\n✅ All timezone examples completed successfully!")
        print("\nKey takeaways:")
        print("- Use datetime.now(timezone.utc) for current UTC time")
        print("- Use to_user_timezone() for display timezone conversion") 
        print("- Use to_utc() when you need to enforce UTC contract")
        print("- Use ensure_timezone_aware() to handle mixed timezone/naive input")
        
    except Exception as e:
        print(f"\n❌ Error running timezone examples: {e}")
        return 1
        
    return 0


if __name__ == "__main__":
    exit(main())