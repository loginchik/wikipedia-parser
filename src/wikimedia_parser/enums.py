from enum import StrEnum


class AccessType(StrEnum):
    """
    Which device the user used to access the page
    """
    Any = 'all-access'
    Desktop = 'desktop'
    MobileWeb = 'mobile-web'
    MobileApp = 'mobile-app'


class UserAgent(StrEnum):
    """
    Who accessed the page
    """
    Any = 'all-agents'
    User = 'user'
    Spider = 'spider'
    Automated = 'automated'


class DateGranularity(StrEnum):
    """
    The time range to aggregate views by
    """
    Daily = 'daily'
    Monthly = 'Monthly'
