import logging
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.database import get_mongo_client, get_redis_client, lifespan
from app.routers.documents import router as documents_router


logger = logging.getLogger(__name__)

app = FastAPI(title="Cohort Insights API", version="0.1.0", lifespan=lifespan)
app.include_router(documents_router)


@app.exception_handler(PyMongoError)
async def mongodb_error(request: Request, exc: PyMongoError) -> JSONResponse:
    logger.error("MongoDB request failed (%s)", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "MongoDB is unavailable."})


@app.exception_handler(RedisError)
async def redis_error(request: Request, exc: RedisError) -> JSONResponse:
    logger.error("Redis request failed (%s)", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "Redis is unavailable."})


@app.get("/health", tags=["Health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get(
    "/health/mongodb",
    tags=["Health"],
    responses={503: {"description": "MongoDB is unavailable."}},
)
async def mongodb_health(
    client: Annotated[AsyncMongoClient[dict[str, Any]], Depends(get_mongo_client)],
) -> dict[str, str]:
    try:
        await client.admin.command("ping")
    except PyMongoError as exc:
        logger.warning("MongoDB health check failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MongoDB is unavailable.",
        ) from exc
    return {"status": "ok"}


@app.get(
    "/health/redis",
    tags=["Health"],
    responses={503: {"description": "Redis is unavailable."}},
)
async def redis_health(
    client: Annotated[Redis, Depends(get_redis_client)],
) -> dict[str, str]:
    try:
        await client.ping()
    except RedisError as exc:
        logger.warning("Redis health check failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis is unavailable.",
        ) from exc
    return {"status": "ok"}