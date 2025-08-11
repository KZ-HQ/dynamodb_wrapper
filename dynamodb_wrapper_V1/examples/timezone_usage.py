#!/usr/bin/env python3
"""
Timezone support examples for the DynamoDB wrapper library.

This example demonstrates:
1. Configuring timezones globally and per-operation
2. Working with timezone-aware datetime fields
3. Converting between timezones
4. Using timezone context for operations
5. Best practices for global applications
"""

from dynamodb_wrapper_V1.dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigRepository,
    PipelineRunLogsRepository,
)
from dynamodb_wrapper_V1.dynamodb_wrapper.utils import (
    TimezoneManager,
    now_in_tz,
    set_global_timezone,
    to_user_timezone,
    utcnow,
)


def demonstrate_timezone_configuration():
    """Demonstrate different ways to configure timezones."""
    print("=== Timezone Configuration Examples ===")

    # Method 1: Default configuration (UTC)
    config_utc = DynamoDBConfig()
    print(f"Default timezone: {config_utc.default_timezone}")
    print(f"Store in UTC: {config_utc.store_timestamps_in_utc}")

    # Method 2: Specify timezone directly
    config_ny = DynamoDBConfig.with_timezone("America/New_York")
    print(f"New York timezone: {config_ny.default_timezone}")

    # Method 3: Configuration with user display timezone
    config_global = DynamoDBConfig(
        default_timezone="UTC",
        user_timezone="Europe/London",
        store_timestamps_in_utc=True
    )
    print(f"Storage timezone: {config_global.default_timezone}")
    print(f"User display timezone: {config_global.user_timezone}")

    # Method 4: Environment variables (simulated)
    import os
    os.environ["DYNAMODB_TIMEZONE"] = "Asia/Tokyo"
    os.environ["DYNAMODB_USER_TIMEZONE"] = "Australia/Sydney"

    config_env = DynamoDBConfig()
    print(f"From environment - Default: {config_env.default_timezone}")
    print(f"From environment - User: {config_env.user_timezone}")

    # Clean up environment
    del os.environ["DYNAMODB_TIMEZONE"]
    del os.environ["DYNAMODB_USER_TIMEZONE"]


def demonstrate_timezone_utilities():
    """Demonstrate timezone utility functions."""
    print("\n=== Timezone Utility Examples ===")

    # Set global timezone
    set_global_timezone("Europe/Berlin")

    # Get current time in different timezones
    berlin_time = now_in_tz("Europe/Berlin")
    tokyo_time = now_in_tz("Asia/Tokyo")
    utc_time = utcnow()

    print(f"Berlin time: {berlin_time}")
    print(f"Tokyo time: {tokyo_time}")
    print(f"UTC time: {utc_time}")

    # Convert between timezones
    ny_time = to_user_timezone(utc_time, "America/New_York")
    london_time = to_user_timezone(utc_time, "Europe/London")

    print(f"UTC {utc_time} converts to:")
    print(f"  New York: {ny_time}")
    print(f"  London: {london_time}")

    # Using TimezoneManager directly
    tm = TimezoneManager("Pacific/Auckland")
    auckland_time = tm.now()
    print(f"Auckland time: {auckland_time}")

    # Parse and convert ISO strings
    iso_string = "2024-01-15T14:30:00+00:00"
    parsed_utc = tm.parse_iso(iso_string)
    parsed_local = tm.parse_iso(iso_string, "Pacific/Auckland")

    print(f"Parsed as UTC: {parsed_utc}")
    print(f"Parsed as Auckland: {parsed_local}")


def demonstrate_repository_timezone_operations():
    """Demonstrate timezone operations with repositories."""
    print("\n=== Repository Timezone Operations ===")

    # Configure for global usage
    config = DynamoDBConfig(
        default_timezone="UTC",
        user_timezone="America/Chicago",  # Central Time
        store_timestamps_in_utc=True
    )

    pipeline_repo = PipelineConfigRepository(config)

    # Create a pipeline (timestamps will be in UTC in storage)
    current_time_chicago = now_in_tz("America/Chicago")
    print(f"Creating pipeline at Chicago time: {current_time_chicago}")

    # The repository will automatically handle timezone conversion
    pipeline_config = pipeline_repo.create_pipeline_config(
        pipeline_id="global-analytics-pipeline",
        pipeline_name="Global Analytics Pipeline",
        source_type="s3",
        destination_type="bigquery",
        created_by="data_engineer_chicago"
    )

    print(f"Pipeline created_at (in user timezone): {pipeline_config.created_at}")

    # Retrieve with different timezone context
    pipeline_tokyo = pipeline_repo.get_with_timezone(
        "global-analytics-pipeline",
        user_timezone="Asia/Tokyo"
    )

    if pipeline_tokyo:
        print(f"Same pipeline viewed from Tokyo: {pipeline_tokyo.created_at}")

    # Create with timezone context (for naive datetime fields)
    sydney_time = now_in_tz("Australia/Sydney")
    print(f"Current Sydney time: {sydney_time}")

    # If we had naive datetimes, we could specify the timezone context
    # pipeline_repo.create_with_timezone_context(pipeline, "Australia/Sydney")


def demonstrate_multi_user_scenario():
    """Demonstrate handling multiple users in different timezones."""
    print("\n=== Multi-User Global Scenario ===")

    # Simulate different users in different timezones
    users = [
        {"name": "Alice", "timezone": "America/New_York", "location": "New York"},
        {"name": "Bob", "timezone": "Europe/London", "location": "London"},
        {"name": "Charlie", "timezone": "Asia/Singapore", "location": "Singapore"},
        {"name": "Diana", "timezone": "Australia/Melbourne", "location": "Melbourne"}
    ]

    # Create a base configuration that stores everything in UTC
    base_config = DynamoDBConfig(
        default_timezone="UTC",
        store_timestamps_in_utc=True
    )

    logs_repo = PipelineRunLogsRepository(base_config)

    # Create a pipeline run (would typically be done by the system)
    run_log = logs_repo.create_run_log(
        run_id="global-run-001",
        pipeline_id="global-analytics-pipeline",
        trigger_type="schedule",
        created_by="system"
    )

    print(f"Pipeline run created at UTC: {run_log.start_time}")

    # Show how each user would see the same timestamp in their timezone
    for user in users:
        user_run_log = logs_repo.get_with_timezone(
            "global-run-001",
            user_timezone=user["timezone"]
        )

        if user_run_log:
            print(f"{user['name']} in {user['location']}: {user_run_log.start_time}")


def demonstrate_best_practices():
    """Demonstrate best practices for timezone handling."""
    print("\n=== Best Practices ===")

    print("1. Storage Strategy:")
    print("   - Always store timestamps in UTC in the database")
    print("   - Convert to user's timezone only for display")
    print("   - Use timezone-aware datetime objects throughout your application")

    print("\n2. Configuration Strategy:")
    config = DynamoDBConfig(
        default_timezone="UTC",           # Always UTC for storage
        store_timestamps_in_utc=True,     # Ensure UTC storage
        user_timezone=None                # Set per-request based on user
    )
    print(f"   - Default timezone: {config.default_timezone}")
    print(f"   - Store in UTC: {config.store_timestamps_in_utc}")
    print("   - User timezone: Set dynamically per request")

    print("\n3. API Design:")
    print("   - Accept timezone parameter in API calls")
    print("   - Return timestamps in requested timezone")
    print("   - Provide timezone metadata in responses")

    print("\n4. User Experience:")
    print("   - Auto-detect user timezone from browser/system")
    print("   - Allow users to set their preferred timezone")
    print("   - Show relative times ('2 hours ago') when appropriate")
    print("   - Always indicate the timezone in the UI")

    print("\n5. Testing Strategy:")
    print("   - Test with multiple timezones")
    print("   - Test DST transitions")
    print("   - Test with users in different timezones simultaneously")


def main():
    """Run all timezone examples."""
    print("DynamoDB Wrapper - Timezone Support Examples\n")

    try:
        demonstrate_timezone_configuration()
        demonstrate_timezone_utilities()
        demonstrate_repository_timezone_operations()
        demonstrate_multi_user_scenario()
        demonstrate_best_practices()

        print("\n=== Summary ===")
        print("✅ Timezone configuration - Multiple methods supported")
        print("✅ Timezone utilities - Comprehensive conversion functions")
        print("✅ Repository operations - Automatic timezone handling")
        print("✅ Multi-user support - Per-user timezone conversion")
        print("✅ Best practices - Production-ready patterns")

        print("\nThe DynamoDB wrapper now provides comprehensive timezone support")
        print("for global applications with users in different timezones!")

    except ImportError as e:
        print(f"⚠️ Import error (expected without DynamoDB tables): {e}")
        print("This example shows the timezone functionality that would work")
        print("with properly configured DynamoDB tables.")

    except Exception as e:
        print(f"❌ Error running timezone examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
