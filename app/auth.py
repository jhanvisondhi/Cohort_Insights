import hmac
import json
import os
from typing import Annotated

from fastapi import Header, HTTPException, status


def current_user(
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str:
    try:
        keys = json.loads(os.environ.get("API_KEYS", "{}"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail="API keys are not configured.") from exc

    if not isinstance(keys, dict) or not keys:
        raise HTTPException(status_code=503, detail="API keys are not configured.")

    for user, key in keys.items():
        if not isinstance(user, str) or not user or not isinstance(key, str) or not key:
            raise HTTPException(status_code=503, detail="API keys are not configured.")

    if len(set(keys.values())) != len(keys):
        raise HTTPException(status_code=503, detail="API keys are not configured.")

    if api_key:
        for user, key in keys.items():
            if hmac.compare_digest(api_key.encode(), key.encode()):
                return user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key."
    )
