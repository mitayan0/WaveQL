"""
WASM/Pyodide Compatibility Utilities.

This module provides helpers for detecting and handling WebAssembly environments
(specifically Pyodide) to ensure code portability.
"""
import sys
import os
import logging

logger = logging.getLogger(__name__)

def is_wasm() -> bool:
    """
    Check if the code is running in a WebAssembly environment (Pyodide).
    """
    # Check sys.platform
    if sys.platform in ("emscripten", "wasi", "js"):
        return True
    
    # Check for pyodide module
    try:
        import pyodide # type: ignore
        return True
    except ImportError:
        pass
        
    return False

def ensure_wasm_compatibility():
    """
    Apply necessary patches or configuration for Wasm environments.
    """
    if not is_wasm():
        return

    # In Wasm, standard requests won't work. We might want to patch it
    # if pyodide-http is available, or rely on async paths.
    try:
        import pyodide_http
        pyodide_http.patch_all()
        logger.info("Applied pyodide-http patches for Wasm")
    except ImportError:
        logger.debug("pyodide-http not found; sync network calls will fail")

def get_wasm_http_client():
    """
    Return a Wasm-compatible HTTP client adapter if needed.
    """
    pass
