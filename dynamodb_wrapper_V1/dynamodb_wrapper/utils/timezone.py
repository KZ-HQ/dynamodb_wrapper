"""Timezone utilities for the DynamoDB wrapper library."""

import os
import sys
from datetime import datetime, timezone
from typing import Optional

# Import zoneinfo with fallback for older Python versions
if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except ImportError:
        try:
            from zoneinfo import ZoneInfo  # type: ignore
        except ImportError:
            # Fallback to pytz if available
            try:
                import pytz

                class ZoneInfo:  # type: ignore
                    """Fallback ZoneInfo implementation using pytz."""

                    def __init__(self, key: str):
                        self.key = key
                        self._tz = pytz.timezone(key)

                    def __str__(self) -> str:
                        return self.key

                    def localize(self, dt: datetime) -> datetime:
                        return self._tz.localize(dt)

                    def normalize(self, dt: datetime) -> datetime:
                        return self._tz.normalize(dt)

            except ImportError as e:
                raise ImportError(
                    "No timezone library available. Please install 'zoneinfo' or 'pytz' "
                    "for timezone support."
                ) from e


class TimezoneManager:
    """Manages timezone configuration and conversions for the DynamoDB wrapper."""

    def __init__(self, default_timezone: Optional[str] = None):
        """Initialize timezone manager.

        Args:
            default_timezone: Default timezone string (e.g., 'UTC', 'America/New_York')
                            If None, uses environment variable or UTC
        """
        self.default_timezone = self._resolve_timezone(default_timezone)

    def _resolve_timezone(self, tz_string: Optional[str]) -> str:
        """Resolve timezone string from parameter, environment, or default."""
        if tz_string:
            return tz_string

        # Check environment variables
        env_tz = (
            os.getenv("DYNAMODB_TIMEZONE") or
            os.getenv("TZ") or
            os.getenv("TIMEZONE")
        )

        if env_tz:
            return env_tz

        return "UTC"

    def get_timezone(self, tz_override: Optional[str] = None) -> ZoneInfo:
        """Get ZoneInfo object for timezone.

        Args:
            tz_override: Override timezone for this operation

        Returns:
            ZoneInfo object for the timezone
        """
        tz_string = tz_override or self.default_timezone
        return ZoneInfo(tz_string)

    def now(self, tz_override: Optional[str] = None) -> datetime:
        """Get current datetime in specified timezone.

        Args:
            tz_override: Override timezone for this operation

        Returns:
            Current datetime in the specified timezone
        """
        tz = self.get_timezone(tz_override)
        return datetime.now(tz)

    def utcnow(self) -> datetime:
        """Get current UTC datetime."""
        return datetime.now(timezone.utc)

    def to_timezone(
        self,
        dt: datetime,
        target_tz: Optional[str] = None
    ) -> datetime:
        """Convert datetime to specified timezone.

        Args:
            dt: Datetime to convert
            target_tz: Target timezone (uses default if None)

        Returns:
            Datetime converted to target timezone
        """
        if dt is None:
            return None

        target_zone = self.get_timezone(target_tz)

        # If datetime is naive, assume it's in the default timezone
        if dt.tzinfo is None:
            source_zone = self.get_timezone()
            dt = dt.replace(tzinfo=source_zone)

        return dt.astimezone(target_zone)

    def to_utc(self, dt: datetime) -> datetime:
        """Convert datetime to UTC.

        Args:
            dt: Datetime to convert

        Returns:
            Datetime in UTC
        """
        if dt is None:
            return None

        return self.to_timezone(dt, "UTC")

    def ensure_timezone(
        self,
        dt: datetime,
        assumed_tz: Optional[str] = None
    ) -> datetime:
        """Ensure datetime has timezone information.

        Args:
            dt: Datetime to check
            assumed_tz: Timezone to assume if datetime is naive

        Returns:
            Timezone-aware datetime
        """
        if dt is None:
            return None

        if dt.tzinfo is None:
            # Naive datetime - add timezone
            tz = self.get_timezone(assumed_tz)
            return dt.replace(tzinfo=tz)

        return dt

    def format_iso(self, dt: datetime) -> str:
        """Format datetime as ISO string with timezone.

        Args:
            dt: Datetime to format

        Returns:
            ISO formatted string with timezone
        """
        if dt is None:
            return None

        # Ensure timezone-aware
        dt = self.ensure_timezone(dt)
        return dt.isoformat()

    def parse_iso(
        self,
        iso_string: str,
        target_tz: Optional[str] = None
    ) -> datetime:
        """Parse ISO datetime string and convert to specified timezone.

        Args:
            iso_string: ISO formatted datetime string
            target_tz: Target timezone for result

        Returns:
            Parsed datetime in target timezone
        """
        if not iso_string:
            return None

        # Parse ISO string
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))

        # Convert to target timezone if specified
        if target_tz:
            dt = self.to_timezone(dt, target_tz)

        return dt

    def get_user_timezone_datetime(
        self,
        utc_dt: Optional[datetime] = None,
        user_tz: Optional[str] = None
    ) -> datetime:
        """Get datetime in user's timezone.

        Args:
            utc_dt: UTC datetime (uses current time if None)
            user_tz: User's timezone (uses default if None)

        Returns:
            Datetime in user's timezone
        """
        if utc_dt is None:
            utc_dt = self.utcnow()

        return self.to_timezone(utc_dt, user_tz)


# Global timezone manager instance
_global_tz_manager: Optional[TimezoneManager] = None


def get_timezone_manager() -> TimezoneManager:
    """Get the global timezone manager instance."""
    global _global_tz_manager
    if _global_tz_manager is None:
        _global_tz_manager = TimezoneManager()
    return _global_tz_manager


def set_global_timezone(timezone_str: str) -> None:
    """Set the global default timezone.

    Args:
        timezone_str: Timezone string (e.g., 'UTC', 'America/New_York')
    """
    global _global_tz_manager
    _global_tz_manager = TimezoneManager(timezone_str)


def configure_timezone_from_config(config) -> TimezoneManager:
    """Configure timezone manager from DynamoDB config.

    Args:
        config: DynamoDB configuration object

    Returns:
        Configured TimezoneManager instance
    """
    return TimezoneManager(getattr(config, 'default_timezone', None))


# Convenience functions
def now_in_tz(tz: Optional[str] = None) -> datetime:
    """Get current datetime in specified timezone."""
    return get_timezone_manager().now(tz)


def utcnow() -> datetime:
    """Get current UTC datetime."""
    return get_timezone_manager().utcnow()


def to_user_timezone(dt: datetime, user_tz: Optional[str] = None) -> datetime:
    """Convert datetime to user's timezone."""
    return get_timezone_manager().to_timezone(dt, user_tz)


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC."""
    return get_timezone_manager().to_utc(dt)


def ensure_timezone_aware(dt: datetime, assumed_tz: Optional[str] = None) -> datetime:
    """Ensure datetime is timezone-aware."""
    return get_timezone_manager().ensure_timezone(dt, assumed_tz)
