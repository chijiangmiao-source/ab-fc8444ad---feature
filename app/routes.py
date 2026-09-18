from __future__ import annotations

from fastapi import APIRouter, Header, Request
from fastapi.responses import FileResponse, JSONResponse

from .schemas import CreateSessionRequest, ReclaimRequest, ReuseRequest
from .service import UploadService


def build_router(service: UploadService) -> APIRouter:
    router = APIRouter()

    @router.post("/sessions", status_code=201)
    def create_session(payload: CreateSessionRequest) -> dict:
        return service.create_session(payload)

    @router.get("/sessions/{session_id}")
    def session_status(session_id: str) -> dict:
        return service.status(session_id)

    @router.put("/sessions/{session_id}/chunks/{chunk_index}")
    async def upload_chunk(
        session_id: str,
        chunk_index: str,
        request: Request,
        x_chunk_sha256: str = Header(...),
    ) -> JSONResponse:
        body, status_code = await service.upload_chunk(
            session_id, chunk_index, x_chunk_sha256, request.stream()
        )
        return JSONResponse(status_code=status_code, content=body)

    @router.post(
        "/sessions/{session_id}/chunks/{chunk_index}/reuse", status_code=201
    )
    def reuse_chunk(
        session_id: str, chunk_index: str, payload: ReuseRequest
    ) -> JSONResponse:
        body, status_code = service.reuse_chunk(
            session_id, chunk_index, payload.sha256
        )
        return JSONResponse(status_code=status_code, content=body)

    @router.post("/sessions/{session_id}/finalize")
    def finalize(session_id: str) -> dict:
        return service.finalize(session_id)

    @router.get("/sessions/{session_id}/artifact")
    def download_artifact(session_id: str) -> FileResponse:
        path, sha256 = service.artifact_file(session_id)
        return FileResponse(
            path,
            media_type="application/octet-stream",
            filename=f"{session_id}.bin",
            headers={"X-File-SHA256": sha256},
        )

    @router.post("/maintenance/chunk-pool/reclaim")
    def reclaim_chunk_pool(payload: ReclaimRequest) -> dict:
        return service.reclaim_pool(payload.max_blobs)

    return router
