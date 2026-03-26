"""Exception hierarchy for the Baran SDK."""


class BaranError(Exception):
    """Base class for all Baran SDK errors."""


class BaranConnectionError(BaranError):
    """Raised when the sidecar cannot be reached."""


class BaranAuthError(BaranError):
    """Raised when the PSK is invalid or missing."""


class BaranRegistrationError(BaranError):
    """Raised when agent registration fails (e.g. conflict, limit reached)."""


class BaranPublishError(BaranError):
    """Raised when an event publish request fails."""
