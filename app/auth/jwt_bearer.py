"""
ZITADEL JWT Bearer authentication dependency for FastAPI.

Validates JWT tokens issued by ZITADEL by:
  - Fetching JWKS public keys from the OIDC discovery endpoint
  - Verifying signature, issuer, audience, and expiration
"""

import time
import logging
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

# ── JWKS cache ───────────────────────────────────────────────────────────
_jwks_cache: Optional[dict] = None
_jwks_cache_expiry: float = 0.0
_JWKS_CACHE_TTL_SECONDS: int = 3600  # 1 hour

security_scheme = HTTPBearer(auto_error=True)


async def _fetch_openid_configuration(issuer_uri: str) -> dict:
    """Fetch the OpenID Connect discovery document."""
    url = f"{issuer_uri.rstrip('/')}/.well-known/openid-configuration"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def _fetch_jwks(jwks_uri: str) -> dict:
    """Fetch the JSON Web Key Set from the JWKS URI."""
    async with httpx.AsyncClient() as client:
        response = await client.get(jwks_uri, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def _get_jwks(issuer_uri: str) -> dict:
    """Return cached JWKS keys, refreshing if expired."""
    global _jwks_cache, _jwks_cache_expiry

    if _jwks_cache and time.time() < _jwks_cache_expiry:
        return _jwks_cache

    logger.info("Refreshing JWKS keys from %s", issuer_uri)
    oidc_config = await _fetch_openid_configuration(issuer_uri)
    jwks_uri = oidc_config["jwks_uri"]
    _jwks_cache = await _fetch_jwks(jwks_uri)
    _jwks_cache_expiry = time.time() + _JWKS_CACHE_TTL_SECONDS
    return _jwks_cache


async def verify_jwt(
    credentials: HTTPAuthorizationCredentials = Depends(security_scheme),
    settings: Settings = Depends(get_settings),
) -> dict:
    """
    FastAPI dependency that validates the JWT Bearer token.

    Returns the decoded token payload on success.
    Raises HTTPException 401 on any validation failure.
    """
    token = credentials.credentials

    if not settings.ZITADEL_ISSUER_URI:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ZITADEL_ISSUER_URI is not configured",
        )

    try:
        jwks = await _get_jwks(settings.ZITADEL_ISSUER_URI)

        # Decode the token header to find the key ID
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        # Find the matching public key
        rsa_key = {}
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                rsa_key = key
                break

        if not rsa_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unable to find matching signing key",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Build audience list — ZITADEL project ID
        audience = settings.ZITADEL_PROJECT_ID

        # Decode and validate the token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            issuer=settings.ZITADEL_ISSUER_URI,
            audience=audience,
            options={
                "verify_aud": bool(audience),
                "verify_iss": True,
                "verify_exp": True,
            },
        )

        logger.debug("JWT validated for subject: %s", payload.get("sub"))
        return payload

    except JWTError as e:
        logger.warning("JWT validation failed: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except httpx.HTTPError as e:
        logger.error("Failed to fetch JWKS: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to validate token: identity provider unreachable",
        )
