#!/usr/bin/env python3
"""Quick network connectivity test for WaveQL adapters."""

import os
import sys
import socket

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()


def test_dns():
    """Test DNS resolution."""
    print("=" * 60)
    print("  DNS Resolution Tests")
    print("=" * 60)
    
    hosts = [
        "api.stripe.com",
        "api.hubapi.com",
        "api.shopify.com",
    ]
    
    for host in hosts:
        try:
            ip = socket.gethostbyname(host)
            print(f"  [OK] {host} -> {ip}")
        except socket.gaierror as e:
            print(f"  [FAIL] {host} -> {e}")


def test_httpx_sync():
    """Test httpx synchronous requests."""
    print("\n" + "=" * 60)
    print("  HTTPX Sync Tests")
    print("=" * 60)
    
    try:
        import httpx
        
        urls = [
            ("Google", "https://www.google.com"),
            ("Stripe", "https://api.stripe.com"),
            ("HubSpot", "https://api.hubapi.com"),
        ]
        
        for name, url in urls:
            try:
                with httpx.Client(timeout=10.0) as client:
                    response = client.head(url)
                    print(f"  [OK] {name}: HTTP {response.status_code}")
            except httpx.ConnectError as e:
                print(f"  [FAIL] {name}: ConnectError - {e}")
            except httpx.TimeoutException as e:
                print(f"  [FAIL] {name}: Timeout - {e}")
            except Exception as e:
                print(f"  [FAIL] {name}: {type(e).__name__} - {e}")
    except ImportError:
        print("  [SKIP] httpx not installed")


def test_httpx_async():
    """Test httpx async requests."""
    print("\n" + "=" * 60)
    print("  HTTPX Async Tests")
    print("=" * 60)
    
    try:
        import httpx
        import asyncio
        
        async def fetch(name: str, url: str):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.head(url)
                    print(f"  [OK] {name}: HTTP {response.status_code}")
            except httpx.ConnectError as e:
                print(f"  [FAIL] {name}: ConnectError - {e}")
            except httpx.TimeoutException as e:
                print(f"  [FAIL] {name}: Timeout - {e}")
            except Exception as e:
                print(f"  [FAIL] {name}: {type(e).__name__} - {e}")
        
        async def main():
            await fetch("Google", "https://www.google.com")
            await fetch("Stripe", "https://api.stripe.com")
            await fetch("HubSpot", "https://api.hubapi.com")
        
        asyncio.run(main())
    except ImportError:
        print("  [SKIP] httpx not installed")


def test_httpx_anyio():
    """Test httpx async requests via anyio.run (like adapters do)."""
    print("\n" + "=" * 60)
    print("  HTTPX via anyio.run() Tests")
    print("=" * 60)
    
    try:
        import httpx
        import anyio
        
        async def fetch(name: str, url: str):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.head(url)
                    print(f"  [OK] {name}: HTTP {response.status_code}")
            except httpx.ConnectError as e:
                print(f"  [FAIL] {name}: ConnectError - {e}")
            except httpx.TimeoutException as e:
                print(f"  [FAIL] {name}: Timeout - {e}")
            except Exception as e:
                print(f"  [FAIL] {name}: {type(e).__name__} - {e}")
        
        # Test each URL using anyio.run (just like adapters do)
        anyio.run(lambda: fetch("Google (anyio)", "https://www.google.com"))
        anyio.run(lambda: fetch("Stripe (anyio)", "https://api.stripe.com"))
        anyio.run(lambda: fetch("HubSpot (anyio)", "https://api.hubapi.com"))
        
    except ImportError as e:
        print(f"  [SKIP] {e}")


def test_stripe_adapter():
    """Test Stripe adapter directly."""
    print("\n" + "=" * 60)
    print("  Stripe Adapter Test")
    print("=" * 60)
    
    api_key = os.getenv("STRIPE_API_KEY")
    if not api_key:
        print("  [SKIP] No STRIPE_API_KEY in environment")
        return
    
    try:
        from waveql.adapters.stripe import StripeAdapter
        
        adapter = StripeAdapter(api_key=api_key)
        print(f"  Adapter created: {adapter}")
        
        # Try a simple list operation
        result = adapter.fetch(
            table="customers",
            predicates=[],
            limit=1
        )
        print(f"  [OK] Fetched {len(result)} rows")
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")


if __name__ == "__main__":
    print("\nWaveQL Network Connectivity Test")
    print("=" * 60)
    
    test_dns()
    test_httpx_sync()
    test_httpx_async()
    test_httpx_anyio()
    test_stripe_adapter()
    
    print("\n" + "=" * 60)
    print("  Test Complete")
    print("=" * 60)
