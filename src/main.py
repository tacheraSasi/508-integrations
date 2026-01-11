from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import BackgroundTasks, Body, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from .crm import EspoCRMClient
from .crm.document_processor import DocumentProcessor
from .crm.processor import ContactSkillsProcessor
from .crm.skills_extractor import SkillsExtractor
from .models import EspoCRMWebhookPayload
from .settings import settings

VERSION = "0.1.0"

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Starting 508 Integrations Service")

    espocrm_client = EspoCRMClient()
    if espocrm_client.health_check():
        logger.info("EspoCRM connection established")
    else:
        logger.warning("EspoCRM connection failed")

    yield

    logger.info("Shutting down 508 Integrations Service")


app = FastAPI(
    title="508 Integrations",
    description="Integration service for EspoCRM webhooks with resume skills extraction",
    version=VERSION,
    lifespan=lifespan,
)


def process_contact_skills_background(contact_id: str) -> None:
    try:
        processor = ContactSkillsProcessor()
        result = processor.process_contact_skills(contact_id)

        if result.success:
            logger.info(
                "Contact skills processed successfully",
                contact_id=contact_id,
                new_skills_count=len(result.new_skills),
                total_skills_count=len(result.updated_skills),
            )
        else:
            logger.error(
                "Failed to process contact skills",
                contact_id=contact_id,
                error=result.error,
            )

    except Exception as e:
        logger.error(
            "Unexpected error processing contact skills",
            contact_id=contact_id,
            error=str(e),
            exc_info=True,
        )


@app.post("/webhooks/espocrm")
async def espocrm_webhook(
    request: Request, background_tasks: BackgroundTasks
) -> JSONResponse:
    try:
        payload_data = await request.json()

        if not isinstance(payload_data, list):
            raise HTTPException(
                status_code=400, detail="Payload must be an array of webhook events"
            )

        payload = EspoCRMWebhookPayload.from_list(payload_data)

        for event in payload.events:
            logger.info(
                "Processing webhook event",
                event_id=event.id,
                event_name=event.name,
            )

            background_tasks.add_task(process_contact_skills_background, event.id)

        return JSONResponse(
            content={
                "status": "success",
                "message": f"Processing {len(payload.events)} webhook events",
                "events_processed": len(payload.events),
            }
        )

    except Exception as e:
        logger.error("Error processing webhook", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/process-contact/{contact_id}")
async def process_contact_manual(
    contact_id: str, background_tasks: BackgroundTasks
) -> JSONResponse:
    try:
        background_tasks.add_task(process_contact_skills_background, contact_id)

        return JSONResponse(
            content={
                "status": "success",
                "message": f"Contact {contact_id} queued for processing",
                "contact_id": contact_id,
            }
        )

    except Exception as e:
        logger.error(
            "Error queuing contact for processing",
            contact_id=contact_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check() -> dict[str, Any]:
    espocrm_client = EspoCRMClient()
    espocrm_status = espocrm_client.health_check()

    return {
        "status": "healthy" if espocrm_status else "degraded",
        "espocrm": "connected" if espocrm_status else "disconnected",
        "version": VERSION,
    }


@app.post("/extract/dry-run")
async def extract_dry_run(
    text: str | None = Body(None, embed=True),
    file: UploadFile | None = File(None),
) -> JSONResponse:
    if not text and not file:
        raise HTTPException(
            status_code=400,
            detail="Provide resume text in 'text' or upload a supported file",
        )

    try:
        resume_text = ""
        source = "text"

        if text:
            resume_text = text.strip()
            if not resume_text:
                raise ValueError("Provided text is empty")
        elif file:
            if not file.filename:
                raise ValueError("Uploaded file is missing a filename")
            content = await file.read()
            processor = DocumentProcessor()
            resume_text = processor.extract_text(content, file.filename)
            source = file.filename

        extractor = SkillsExtractor()
        extracted = extractor.extract_skills(resume_text)

        return JSONResponse(
            content={
                "status": "success",
                "source": source,
                "skills": extracted.skills,
                "confidence": extracted.confidence,
                "model": extracted.source,
            }
        )

    except ValueError as e:
        logger.warning("Dry-run extraction input error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Dry-run extraction failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Extraction failed")


@app.get("/ping")
async def ping() -> dict[str, Any]:
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": VERSION,
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "508 Integrations Service",
        "version": VERSION,
        "docs": "/docs",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
