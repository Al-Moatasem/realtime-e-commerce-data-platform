import asyncio
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from core.job_manager import simulation_manager
from db.database import AsyncSessionLocal
from db.models import Merchant, Store
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from generators.gen_session import session_simulator
from schemas.control import FlashPromoRuleEvent
from services.fulfillment_service import order_fulfillment_loop
from services.kafka_service import kafka_producer_service
from services.sim_service import (
    SIM_HISTORY_FILE,
    onboard_merchant_task,
    run_bulk_init_task,
    run_merchant_lifecycle_task,
    update_products_task,
)
from sqlalchemy import select

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="E-Commerce Stateful Data Generator")

# Setup templates
current_dir = os.path.dirname(os.path.realpath(__file__))
templates = Jinja2Templates(directory=os.path.join(current_dir, "templates"))


# Global state for Kafka connectivity
KAFKA_CONNECTED = False


# Lifecycle events
@app.on_event("startup")
async def startup_event():
    global KAFKA_CONNECTED
    # Try to start Kafka Producer
    try:
        await kafka_producer_service.start()
        KAFKA_CONNECTED = True
        logger.info("Kafka producer successfully connected.")
    except Exception as e:
        KAFKA_CONNECTED = False
        logger.warning(
            f"Could NOT connect to Kafka on startup. Some simulation features will be disabled. Error: {e}"
        )

    # Register Background Simulation Tasks
    simulation_manager.register_job(
        "onboard-merchants", onboard_merchant_task, interval=10
    )
    simulation_manager.register_job(
        "update-products", update_products_task, interval=30
    )
    simulation_manager.register_job(
        "session-simulator", session_simulator.simulate_sessions_task, interval=5
    )
    simulation_manager.register_job(
        "order-fulfillment", order_fulfillment_loop, interval=15
    )
    logger.info("Simulation manager and events initialized.")


@app.on_event("shutdown")
async def shutdown_event():
    await kafka_producer_service.stop()


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.post("/api/v1/simulations/session-simulator/configure")
async def configure_session_simulator(config: dict):
    """Updates the session simulator parameters from the UI."""
    session_simulator.configure(
        clickstream_topic=config.get("clickstream_topic"),
        base_sessions_per_minute=config.get("base_sessions_per_minute"),
        allowed_event_types=config.get("allowed_event_types"),
    )
    return {"message": "Session simulator configuration updated"}


@app.get("/api/v1/simulations/status")
async def get_simulation_status():
    status = simulation_manager.get_status()
    status["kafka_connected"] = KAFKA_CONNECTED
    return status


# Job Controls (Generic approach for starting/stopping)


@app.post("/api/v1/simulations/{job_name}/start")
async def start_simulation(job_name: str):
    if job_name not in simulation_manager.jobs:
        raise HTTPException(
            status_code=404, detail=f"Simulation job '{job_name}' not found."
        )
    await simulation_manager.start_job(job_name)
    return {"message": f"Started simulation job '{job_name}'"}


@app.post("/api/v1/simulations/{job_name}/pause")
async def pause_simulation(job_name: str):
    if job_name not in simulation_manager.jobs:
        raise HTTPException(
            status_code=404, detail=f"Simulation job '{job_name}' not found."
        )
    await simulation_manager.pause_job(job_name)
    return {"message": f"Paused simulation job '{job_name}'"}


@app.post("/api/v1/simulations/{job_name}/resume")
async def resume_simulation(job_name: str):
    if job_name not in simulation_manager.jobs:
        raise HTTPException(
            status_code=404, detail=f"Simulation job '{job_name}' not found."
        )
    await simulation_manager.resume_job(job_name)
    return {"message": f"Resumed simulation job '{job_name}'"}


@app.post("/api/v1/simulations/{job_name}/stop")
async def stop_simulation(job_name: str):
    if job_name not in simulation_manager.jobs:
        raise HTTPException(
            status_code=404, detail=f"Simulation job '{job_name}' not found."
        )
    await simulation_manager.stop_job(job_name)
    return {"message": f"Stopped simulation job '{job_name}'"}


@app.post("/api/v1/simulations/broadcast-promo")
async def broadcast_flash_promo(config: dict = None):
    """Emits events to the `control.promo_rules` Kafka topic for multiple stores."""
    # Settings for this broadcast
    num_stores = config.get("num_stores", 1) if config else 1
    min_discount = config.get("min_discount", 10.0) if config else 10.0
    max_discount = config.get("max_discount", 30.0) if config else 30.0

    async with AsyncSessionLocal() as session:
        res = await session.execute(select(Store).where(Store.status == "active"))
        all_stores = res.scalars().all()
        if not all_stores:
            raise HTTPException(status_code=400, detail="No active stores available.")

        target_count = min(len(all_stores), num_stores)
        selected_stores = random.sample(all_stores, target_count)

        now = datetime.now(timezone.utc)
        broadcast_results = []

        for store in selected_stores:
            discount = round(random.uniform(min_discount, max_discount), 1)
            rule = FlashPromoRuleEvent(
                rule_id=uuid.uuid4(),
                store_id=store.store_id,
                discount_percentage=discount,
                active_from=now,
                active_until=now + timedelta(hours=2),
            )

            success = await kafka_producer_service.produce_message(
                "control.promo_rules", rule, store.store_id
            )
            if success:
                entry = {
                    "timestamp": now.isoformat(),
                    "store_id": str(store.store_id),
                    "discount": discount,
                    "status": "Success",
                }
                # 1. Update/Inject historical record
                session_simulator.promo_history.insert(0, entry)

                # 2. Update UNIQUE state (one row per store)
                session_simulator.active_promos[str(store.store_id)] = entry

                broadcast_results.append(entry)

        return {
            "message": f"Broadcasted promos to {len(broadcast_results)} stores",
            "results": broadcast_results,
        }


@app.get("/api/v1/simulations/promo-summary")
async def get_promo_summary():
    """Returns the UNIQUE list of stores currently under a promo (latest rule only)."""
    return list(session_simulator.active_promos.values())


@app.get("/api/v1/simulations/promo-history")
async def get_promo_history():
    """Returns the raw historical log of all broadcasts (duplicates allowed)."""
    return session_simulator.promo_history[:30]


@app.get("/api/v1/simulations/segments")
async def get_segments():
    """Returns the list of product segments and their current settings."""
    return [
        {
            "id": k,
            "name": s.name,
            "description": s.description,
            "traffic_weight": s.traffic_weight,
            "conversion_rate": s.conversion_rate,
            "is_active": s.is_active,
        }
        for k, s in session_simulator.SEGMENTS.items()
    ]


@app.post("/api/v1/simulations/segments/configure")
async def configure_segments(config: dict):
    """Updates segment parameters (weight, rate, visibility)."""
    segment_id = config.get("id")
    if segment_id not in session_simulator.SEGMENTS:
        raise HTTPException(status_code=404, detail="Segment not found")

    segment = session_simulator.SEGMENTS[segment_id]
    if "traffic_weight" in config:
        segment.traffic_weight = config["traffic_weight"]
    if "conversion_rate" in config:
        segment.conversion_rate = config["conversion_rate"]
    if "is_active" in config:
        segment.is_active = config["is_active"]

    return {"message": f"Segment '{segment_id}' updated"}


@app.post("/api/v1/simulations/bulk-init")
async def bulk_init_endpoint(config: dict):
    try:
        await run_bulk_init_task(
            merchants_count=config.get("merchants", 500),
            stores_count=config.get("stores", 750),
            products_count=config.get("products", 2500),
            customers_count=config.get("customers", 5000),
        )
        return {"message": "Bulk initialization completed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/merchant/{merchant_id}", response_class=HTMLResponse)
async def merchant_dashboard_page(request: Request, merchant_id: uuid.UUID):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Merchant).where(Merchant.merchant_id == merchant_id)
        )
        merchant = result.scalar_one_or_none()
        merchant_name = merchant.company_name if merchant else "Unknown Merchant"

    return templates.TemplateResponse(
        request=request,
        name="merchant_dashboard.html",
        context={
            "merchant_id": str(merchant_id),
            "merchant_name": merchant_name,
        },
    )


@app.get("/store/{store_id}", response_class=HTMLResponse)
async def store_dashboard_page(request: Request, store_id: uuid.UUID):
    async with AsyncSessionLocal() as session:
        # Join Store with Merchant to get both names
        result = await session.execute(
            select(Store, Merchant)
            .join(Merchant, Store.merchant_id == Merchant.merchant_id)
            .where(Store.store_id == store_id)
        )
        row = result.first()

        if row:
            store, merchant = row
            store_name = store.name
            merchant_name = merchant.company_name
            m_id = str(merchant.merchant_id)
        else:
            store_name = "Unknown Store"
            merchant_name = "Unknown Merchant"
            m_id = ""

    return templates.TemplateResponse(
        request=request,
        name="store_dashboard.html",
        context={
            "merchant_id": m_id,
            "merchant_name": merchant_name,
            "store_id": str(store_id),
            "store_name": store_name,
        },
    )


@app.get("/api/v1/merchants/simulated-history")
async def get_simulated_merchants():
    """Reads simulated merchant history from JSON file."""
    if os.path.exists(SIM_HISTORY_FILE):
        try:
            with open(SIM_HISTORY_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read simulation history: {e}")
    return []


@app.post("/api/v1/simulations/merchant-lifecycle")
async def start_merchant_lifecycle(config: dict = None):
    """Trigger the background lifecycle simulation (onboard + backfill)."""
    backfill_days = config.get("backfill_days", 33) if config else 33
    conversion_rate = config.get("conversion_rate", 0.15) if config else 0.15

    # Run in background to avoid timeout
    asyncio.create_task(
        run_merchant_lifecycle_task(
            backfill_days=backfill_days, conversion_rate=conversion_rate
        )
    )
    return {
        "message": f"Merchant lifecycle simulation started (Backfill: {backfill_days}d, Conv: {int(conversion_rate * 100)}%)."
    }
