"""
WaveQL Authentication Package

Provides unified authentication management for all adapters:
- Basic Authentication (username/password)
- API Key Authentication (header or query param)
- OAuth2 with multiple grant types and auto-refresh
- JWT/Bearer token authentication

Security:
    All credential classes use SecretStr internally to prevent
    accidental exposure in logs, repr(), or exception messages.
"""

from waveql.auth.manager import (
    AuthManager,
    BaseAuthManager,
    BasicAuthManager,
    APIKeyAuthManager,
    OAuth2Manager,
    JWTAuthManager,
    TokenInfo,
    SecretStr,
    AuthenticationError,
    create_auth_manager,
)

__all__ = [
    # Base/Abstract
    "BaseAuthManager",
    "TokenInfo",
    "SecretStr",
    "AuthenticationError",
    # Concrete Managers
    "AuthManager",
    "BasicAuthManager", 
    "APIKeyAuthManager",
    "OAuth2Manager",
    "JWTAuthManager",
    # Factory
    "create_auth_manager",
]
