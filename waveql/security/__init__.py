"""
WaveQL Security Module - Row-Level Security (RLS)

Provides transparent data filtering based on policies attached to connections.
"""

from waveql.security.policy import SecurityPolicy, PolicyManager, PolicyViolationError

__all__ = ["SecurityPolicy", "PolicyManager", "PolicyViolationError"]
