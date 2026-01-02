"""
WaveQL Authentication Package

Provides unified authentication management for all adapters:
- Basic Authentication (username/password)
- API Key Authentication (header or query param)
- OAuth2 with multiple grant types and auto-refresh
- JWT/Bearer token authentication
"""

from waveql.auth.manager import (
    AuthManager,
    BaseAuthManager,
    BasicAuthManager,
    APIKeyAuthManager,
    OAuth2Manager,
    JWTAuthManager,
    TokenInfo,
    AuthenticationError,
    create_auth_manager,
)

__all__ = [
    # Base/Abstract
    "BaseAuthManager",
    "TokenInfo",
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
