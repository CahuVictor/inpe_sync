from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import settings
from .logging_config import get_logger
from ..api.v1.routers.ingest import run_incremental

scheduler: AsyncIOScheduler | None = None
log = get_logger()

def start_scheduler(app):
    """
    Inicia o scheduler (APScheduler) uma única vez por processo.

    - Lê a expressão cron de `settings.schedule_cron` (formato 5 campos: m h dom mon dow).
    - Agenda a tarefa de sincronização incremental (`run_incremental`) de tempos em tempos.
    """
    global scheduler
    if scheduler:
        return
    scheduler = AsyncIOScheduler()
    # cron estilo "* */30 * * * *" (minuto, hora, dia...)
    trig = CronTrigger.from_crontab(settings.schedule_cron)
    # 👇 passe a função, não uma lambda que retorna coroutine
    # scheduler.add_job(lambda: run_incremental(), trig, name="incremental-sync")
    scheduler.add_job(run_incremental, trig, name="incremental-sync")
    scheduler.start()
    log.info("scheduler.started", cron=settings.schedule_cron)

def stop_scheduler(app):
    """
    Para o scheduler (se estiver iniciado) de forma segura.
    """
    global scheduler
    if scheduler:
        scheduler.shutdown()
        scheduler = None
        log.info("scheduler.stopped")
