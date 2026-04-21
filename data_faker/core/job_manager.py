import asyncio
from typing import Dict, Optional, Callable
import logging

logger = logging.getLogger(__name__)

class SimulationJob:
    def __init__(self, name: str, task_func: Callable, interval: int = 5):
        self.name = name
        self.task_func = task_func
        self.interval = interval
        self.status = "stopped" # stopped, running, paused
        self._task: Optional[asyncio.Task] = None

    async def _run(self):
        while self.status != "stopped":
            if self.status == "running":
                try:
                    await self.task_func()
                except Exception as e:
                    logger.error(f"Error in simulation job '{self.name}': {e}")
            
            await asyncio.sleep(self.interval)

    async def start(self):
        if self.status == "stopped":
            self.status = "running"
            self._task = asyncio.create_task(self._run())
            logger.info(f"Started simulation job: {self.name}")

    async def pause(self):
        if self.status == "running":
            self.status = "paused"
            logger.info(f"Paused simulation job: {self.name}")

    async def resume(self):
        if self.status == "paused":
            self.status = "running"
            logger.info(f"Resumed simulation job: {self.name}")

    async def stop(self):
        self.status = "stopped"
        if self._task:
            await self._task
            self._task = None
        logger.info(f"Stopped simulation job: {self.name}")

class SimulationManager:
    def __init__(self):
        self.jobs: Dict[str, SimulationJob] = {}

    def register_job(self, name: str, task_func: Callable, interval: int = 5):
        self.jobs[name] = SimulationJob(name, task_func, interval)

    async def start_job(self, name: str):
        if name in self.jobs:
            await self.jobs[name].start()

    async def pause_job(self, name: str):
        if name in self.jobs:
            await self.jobs[name].pause()

    async def resume_job(self, name: str):
        if name in self.jobs:
            await self.jobs[name].resume()

    async def stop_job(self, name: str):
        if name in self.jobs:
            await self.jobs[name].stop()

    def get_status(self):
        return {name: job.status for name, job in self.jobs.items()}

simulation_manager = SimulationManager()
