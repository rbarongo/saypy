from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import seed
from app.database import SessionLocal
from app.routers import (
    auth,
    churches,
    collection_audit,
    collection_groups,
    collection_types,
    collections,
    families,
    feedback,
    lookup,
    member_lookups,
    members,
    permissions,
    receipt_series,
    receipts,
    reports,
    roles,
    stats,
    users,
)
from app.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = SessionLocal()
    try:
        seed.run(db)
    finally:
        db.close()
    yield


app = FastAPI(title="Saypy Core API — Members Management", version="0.1.0", lifespan=lifespan)

cors_origins = settings.cors_origin_list

# This module is reached through whatever host the frontend happens to be served
# from (localhost, a LAN IP, a forwarded/remote dev-environment hostname, ...),
# and auth is a Bearer JWT header rather than cookies, so there's no CORS
# credentials mode to protect — matching backend_old's own CORS policy
# (`allow_origins=["*"]`), we allow any origin by default rather than requiring
# every possible hostname to be enumerated in CORS_ORIGINS.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if "*" in cors_origins else cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(churches.router)
app.include_router(users.router)
app.include_router(families.router)
app.include_router(members.router)
app.include_router(stats.router)
app.include_router(roles.router)
app.include_router(permissions.router)
app.include_router(collection_types.router)
app.include_router(collections.router)
app.include_router(collection_groups.router)
app.include_router(collection_audit.router)
app.include_router(receipt_series.router)
app.include_router(receipts.router)
app.include_router(feedback.router)
app.include_router(reports.router)
for lookup_router in lookup.all_lookup_routers:
    app.include_router(lookup_router)
for member_lookup_router in member_lookups.all_member_lookup_routers:
    app.include_router(member_lookup_router)


@app.get("/health")
def health():
    return {"status": "ok"}
