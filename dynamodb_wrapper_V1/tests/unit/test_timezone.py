import os
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from dynamodb_wrapper_V1.dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper_V1.dynamodb_wrapper.utils.timezone import (
    TimezoneManager,
    ensure_timezone_aware,
    get_timezone_manager,
    now_in_tz,
    set_global_timezone,
    to_user_timezone,
    to_utc,
    utcnow,
)


class TestTimezoneManager:
    """Test cases for TimezoneManager."""

    def test_default_timezone_utc(self):
        """Test default timezone is UTC."""
        tm = TimezoneManager()
        assert tm.default_timezone == "UTC"

    def test_custom_timezone(self):
        """Test custom timezone initialization."""
        tm = TimezoneManager("America/New_York")
        assert tm.default_timezone == "America/New_York"

    @patch.dict(os.environ, {"DYNAMODB_TIMEZONE": "Europe/London"})
    def test_timezone_from_environment(self):
        """Test timezone from environment variable."""
        tm = TimezoneManager()
        assert tm.default_timezone == "Europe/London"

    def test_now_utc(self):
        """Test getting current time in UTC."""
        tm = TimezoneManager()
        now = tm.now()
        assert now.tzinfo is not None
        assert str(now.tzinfo) == "UTC"

    def test_now_custom_timezone(self):
        """Test getting current time in custom timezone."""
        tm = TimezoneManager("America/New_York")
        now = tm.now()
        assert now.tzinfo is not None
        # The exact timezone name may vary, but should not be UTC
        assert str(now.tzinfo) != "UTC"

    def test_utcnow(self):
        """Test getting current UTC time."""
        tm = TimezoneManager()
        now = tm.utcnow()
        assert now.tzinfo == timezone.utc

    def test_to_timezone_naive(self):
        """Test converting naive datetime to timezone."""
        tm = TimezoneManager("America/New_York")
        naive_dt = datetime(2024, 1, 15, 12, 0, 0)

        converted = tm.to_timezone(naive_dt, "UTC")
        assert converted.tzinfo is not None

    def test_to_timezone_aware(self):
        """Test converting timezone-aware datetime."""
        tm = TimezoneManager()
        utc_dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Should work without error
        converted = tm.to_timezone(utc_dt, "America/New_York")
        assert converted.tzinfo is not None

    def test_to_utc(self):
        """Test converting datetime to UTC."""
        tm = TimezoneManager()
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        utc_dt = tm.to_utc(dt)
        assert str(utc_dt.tzinfo) == "UTC"

    def test_ensure_timezone_naive(self):
        """Test ensuring timezone on naive datetime."""
        tm = TimezoneManager("America/New_York")
        naive_dt = datetime(2024, 1, 15, 12, 0, 0)

        aware_dt = tm.ensure_timezone(naive_dt)
        assert aware_dt.tzinfo is not None

    def test_ensure_timezone_aware(self):
        """Test ensuring timezone on already aware datetime."""
        tm = TimezoneManager()
        aware_dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        result = tm.ensure_timezone(aware_dt)
        assert result == aware_dt

    def test_format_iso(self):
        """Test formatting datetime as ISO string."""
        tm = TimezoneManager()
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        iso_str = tm.format_iso(dt)
        assert "2024-01-15T12:00:00+00:00" in iso_str

    def test_parse_iso(self):
        """Test parsing ISO datetime string."""
        tm = TimezoneManager()
        iso_str = "2024-01-15T12:00:00+00:00"

        dt = tm.parse_iso(iso_str)
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.tzinfo is not None

    def test_parse_iso_z_suffix(self):
        """Test parsing ISO string with Z suffix."""
        tm = TimezoneManager()
        iso_str = "2024-01-15T12:00:00Z"

        dt = tm.parse_iso(iso_str)
        assert dt.tzinfo is not None

    def test_get_user_timezone_datetime(self):
        """Test getting datetime in user's timezone."""
        tm = TimezoneManager()
        utc_dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        user_dt = tm.get_user_timezone_datetime(utc_dt, "America/New_York")
        assert user_dt.tzinfo is not None
        # Should be different time due to timezone conversion
        assert user_dt != utc_dt or str(user_dt.tzinfo) != "UTC"


class TestGlobalTimezoneManager:
    """Test cases for global timezone manager functions."""

    def test_global_manager_singleton(self):
        """Test global manager is singleton."""
        tm1 = get_timezone_manager()
        tm2 = get_timezone_manager()
        assert tm1 is tm2

    def test_set_global_timezone(self):
        """Test setting global timezone."""
        set_global_timezone("Europe/London")
        tm = get_timezone_manager()
        assert tm.default_timezone == "Europe/London"

        # Reset to UTC for other tests
        set_global_timezone("UTC")

    def test_now_in_tz(self):
        """Test convenience function for now in timezone."""
        now = now_in_tz("UTC")
        assert now.tzinfo is not None

    def test_utcnow_convenience(self):
        """Test convenience function for UTC now."""
        now = utcnow()
        assert now.tzinfo == timezone.utc

    def test_to_user_timezone_convenience(self):
        """Test convenience function for user timezone conversion."""
        utc_dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        user_dt = to_user_timezone(utc_dt, "America/New_York")
        assert user_dt.tzinfo is not None

    def test_to_utc_convenience(self):
        """Test convenience function for UTC conversion."""
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        utc_dt = to_utc(dt)
        assert str(utc_dt.tzinfo) == "UTC"

    def test_ensure_timezone_aware_convenience(self):
        """Test convenience function for ensuring timezone awareness."""
        naive_dt = datetime(2024, 1, 15, 12, 0, 0)
        aware_dt = ensure_timezone_aware(naive_dt)
        assert aware_dt.tzinfo is not None


class TestDynamoDBConfigTimezone:
    """Test cases for DynamoDB config timezone support."""

    def test_default_timezone_config(self):
        """Test default timezone in config."""
        config = DynamoDBConfig()
        assert config.default_timezone == "UTC"
        assert config.store_timestamps_in_utc is True

    @patch.dict(os.environ, {"DYNAMODB_TIMEZONE": "Asia/Tokyo"})
    def test_timezone_from_env(self):
        """Test timezone from environment."""
        config = DynamoDBConfig()
        assert config.default_timezone == "Asia/Tokyo"

    def test_with_timezone_factory(self):
        """Test creating config with specific timezone."""
        config = DynamoDBConfig.with_timezone("Europe/Paris")
        assert config.default_timezone == "Europe/Paris"

    def test_get_timezone_manager(self):
        """Test getting timezone manager from config."""
        config = DynamoDBConfig.with_timezone("Australia/Sydney")
        tm = config.get_timezone_manager()
        assert tm.default_timezone == "Australia/Sydney"

    def test_timezone_validation_valid(self):
        """Test valid timezone validation."""
        # Should not raise
        config = DynamoDBConfig(default_timezone="UTC")
        assert config.default_timezone == "UTC"

    def test_timezone_validation_invalid(self):
        """Test invalid timezone validation."""
        with pytest.raises(ValueError, match="Invalid timezone"):
            DynamoDBConfig(default_timezone="NotATimezone")
