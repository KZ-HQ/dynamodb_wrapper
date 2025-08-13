"""
Timezone Assertion Helpers

Provides robust timezone comparison utilities for integration tests.
These helpers ensure proper timezone validation beyond simple `.tzinfo is not None` checks.
"""

from datetime import datetime, timezone
from typing import Optional, Union
import zoneinfo
from zoneinfo import ZoneInfo


def assert_timezone_equals(actual_dt: datetime, expected_timezone: Union[str, timezone, ZoneInfo], 
                          tolerance_seconds: float = 0.1) -> None:
    """
    Assert that a datetime has the expected timezone with robust comparison.
    
    Args:
        actual_dt: The datetime to check
        expected_timezone: Expected timezone (string name, timezone object, or ZoneInfo)
        tolerance_seconds: Tolerance for time comparison when converting between timezone representations
        
    Raises:
        AssertionError: If timezone doesn't match expected
    """
    if actual_dt is None:
        raise AssertionError("Cannot check timezone of None datetime")
    
    if actual_dt.tzinfo is None:
        raise AssertionError(f"Expected timezone-aware datetime, got naive datetime: {actual_dt}")
    
    # Normalize expected timezone to ZoneInfo for comparison
    expected_tz = _normalize_timezone(expected_timezone)
    actual_tz = actual_dt.tzinfo
    
    # For UTC, handle multiple representations
    if _is_utc_timezone(expected_tz):
        if not _is_utc_timezone(actual_tz):
            raise AssertionError(
                f"Expected UTC timezone, got {actual_tz} (type: {type(actual_tz)}) for datetime {actual_dt}"
            )
        return
    
    # For named timezones, compare zone names
    if hasattr(expected_tz, 'key') and hasattr(actual_tz, 'key'):
        if expected_tz.key != actual_tz.key:
            raise AssertionError(
                f"Expected timezone '{expected_tz.key}', got '{actual_tz.key}' for datetime {actual_dt}"
            )
        return
    
    # Fallback: compare by converting a test datetime to both timezones
    test_utc_dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)  # Use fixed date to avoid DST issues
    
    try:
        expected_dt = test_utc_dt.astimezone(expected_tz)
        actual_converted = test_utc_dt.astimezone(actual_tz)
        
        offset_diff = abs((expected_dt.utcoffset() - actual_converted.utcoffset()).total_seconds())
        if offset_diff > tolerance_seconds:
            raise AssertionError(
                f"Timezone offset mismatch. Expected {expected_tz} (offset: {expected_dt.utcoffset()}), "
                f"got {actual_tz} (offset: {actual_converted.utcoffset()}) for datetime {actual_dt}"
            )
    except Exception as e:
        # If conversion fails, fall back to string comparison
        expected_str = str(expected_tz)
        actual_str = str(actual_tz)
        if expected_str not in actual_str and actual_str not in expected_str:
            raise AssertionError(
                f"Could not verify timezone equivalence. Expected something like '{expected_str}', "
                f"got '{actual_str}' for datetime {actual_dt}. Conversion error: {e}"
            )


def assert_utc_timezone(actual_dt: datetime) -> None:
    """
    Assert that a datetime is in UTC timezone.
    
    Args:
        actual_dt: The datetime to check
        
    Raises:
        AssertionError: If not UTC timezone
    """
    assert_timezone_equals(actual_dt, timezone.utc)


def assert_timezones_equivalent(dt1: datetime, dt2: datetime, tolerance_seconds: float = 1.0) -> None:
    """
    Assert that two datetimes represent the same moment in time (timezone-equivalent).
    
    Args:
        dt1: First datetime
        dt2: Second datetime  
        tolerance_seconds: Tolerance for time difference
        
    Raises:
        AssertionError: If datetimes don't represent the same moment
    """
    if dt1 is None or dt2 is None:
        raise AssertionError(f"Cannot compare None datetimes: {dt1}, {dt2}")
    
    if dt1.tzinfo is None or dt2.tzinfo is None:
        raise AssertionError(f"Both datetimes must be timezone-aware: {dt1}, {dt2}")
    
    # Convert both to UTC for comparison
    dt1_utc = dt1.astimezone(timezone.utc)
    dt2_utc = dt2.astimezone(timezone.utc)
    
    time_diff = abs((dt1_utc - dt2_utc).total_seconds())
    if time_diff > tolerance_seconds:
        raise AssertionError(
            f"Datetimes are not equivalent. {dt1} ({dt1_utc} UTC) vs {dt2} ({dt2_utc} UTC). "
            f"Difference: {time_diff} seconds (tolerance: {tolerance_seconds})"
        )


def assert_timezone_aware(actual_dt: datetime) -> None:
    """
    Assert that a datetime is timezone-aware (has tzinfo).
    
    Args:
        actual_dt: The datetime to check
        
    Raises:
        AssertionError: If datetime is naive
    """
    if actual_dt is None:
        raise AssertionError("Cannot check timezone awareness of None datetime")
    
    if actual_dt.tzinfo is None:
        raise AssertionError(f"Expected timezone-aware datetime, got naive datetime: {actual_dt}")


def assert_stored_as_utc_string(raw_datetime_string: str) -> None:
    """
    Assert that a raw datetime string from DynamoDB is stored in UTC format.
    
    Args:
        raw_datetime_string: The raw string from DynamoDB
        
    Raises:
        AssertionError: If not valid UTC format
    """
    if not isinstance(raw_datetime_string, str):
        raise AssertionError(f"Expected string, got {type(raw_datetime_string)}: {raw_datetime_string}")
    
    # Check for UTC indicators
    utc_indicators = ['+00:00', 'Z', 'UTC']
    has_utc_indicator = any(indicator in raw_datetime_string for indicator in utc_indicators)
    
    if not has_utc_indicator:
        raise AssertionError(
            f"Datetime string lacks UTC indicator. Expected one of {utc_indicators}, "
            f"got: '{raw_datetime_string}'"
        )
    
    # Verify it's parseable as ISO datetime
    try:
        parsed_dt = datetime.fromisoformat(raw_datetime_string.replace('Z', '+00:00'))
        if parsed_dt.tzinfo is None:
            raise AssertionError(f"Parsed datetime is naive: {parsed_dt} from '{raw_datetime_string}'")
        
        # Verify it's actually UTC
        assert_utc_timezone(parsed_dt)
        
    except ValueError as e:
        raise AssertionError(f"Could not parse datetime string '{raw_datetime_string}' as ISO format: {e}")


def get_timezone_name(tz: Union[str, timezone, ZoneInfo]) -> str:
    """
    Get a consistent timezone name for comparison purposes.
    
    Args:
        tz: Timezone object or string
        
    Returns:
        Normalized timezone name
    """
    if isinstance(tz, str):
        return tz
    elif hasattr(tz, 'key'):
        return tz.key
    elif tz == timezone.utc:
        return 'UTC'
    else:
        return str(tz)


def _normalize_timezone(tz: Union[str, timezone, ZoneInfo]) -> Union[timezone, ZoneInfo]:
    """Normalize timezone input to a timezone object."""
    if isinstance(tz, str):
        if tz.upper() == 'UTC':
            return timezone.utc
        else:
            try:
                return ZoneInfo(tz)
            except Exception:
                raise ValueError(f"Invalid timezone string: '{tz}'")
    elif isinstance(tz, (timezone, ZoneInfo)):
        return tz
    else:
        raise ValueError(f"Invalid timezone type: {type(tz)}")


def _is_utc_timezone(tz) -> bool:
    """Check if a timezone represents UTC."""
    if tz == timezone.utc:
        return True
    
    if hasattr(tz, 'key') and tz.key == 'UTC':
        return True
        
    if str(tz).upper() in ['UTC', 'UTC+00:00', '+00:00']:
        return True
        
    # Check offset for UTC (should be 0)
    try:
        test_dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=tz)
        return test_dt.utcoffset().total_seconds() == 0
    except Exception:
        return False


def create_timezone_test_context(user_timezone: str = "America/New_York"):
    """
    Create a context for timezone testing with known timezone.
    
    Args:
        user_timezone: The user's timezone for testing
        
    Returns:
        Dictionary with timezone test utilities
    """
    return {
        'user_timezone': user_timezone,
        'user_tz_obj': ZoneInfo(user_timezone),
        'utc_tz_obj': timezone.utc,
        'assert_utc': assert_utc_timezone,
        'assert_user_tz': lambda dt: assert_timezone_equals(dt, user_timezone),
        'assert_equivalent': assert_timezones_equivalent,
        'assert_aware': assert_timezone_aware
    }