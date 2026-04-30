from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from taskiq_horizon.jobs import RetryFailedJob

# We will attach the repository instances to the app state so routes can access them
router = APIRouter(prefix="/api")


def get_job_repo(request: Request):
    return request.app.state.horizon_job_repo


def get_metrics_repo(request: Request):
    return request.app.state.horizon_metrics_repo


def get_workload_repo(request: Request):
    return request.app.state.horizon_workload_repo


def get_wait_time(request: Request):
    return request.app.state.horizon_wait_time


def get_tag_repo(request: Request):
    return request.app.state.horizon_tag_repo


def get_config(request: Request):
    return request.app.state.horizon_config


def get_master_repo(request: Request):
    return getattr(request.app.state, "horizon_master_repo", None)


def get_super_repo(request: Request):
    return getattr(request.app.state, "horizon_super_repo", None)


def get_command_queue(request: Request):
    return getattr(request.app.state, "horizon_command_queue", None)


def get_batch_repo(request: Request):
    return getattr(request.app.state, "horizon_batch_repo", None)


def get_retry_job_handler(
    request: Request,
    jobs=Depends(get_job_repo),
    tags=Depends(get_tag_repo),
    config=Depends(get_config),
):
    broker = getattr(request.app.state, "broker", None)
    return RetryFailedJob(broker=broker, jobs=jobs, tags=tags, config=config)


class MonitorTagRequest(BaseModel):
    tag: str


class ScaleRequest(BaseModel):
    scale: int


@router.get("/stats")
async def get_stats(
    metrics=Depends(get_metrics_repo),
    jobs=Depends(get_job_repo),
    wait_time=Depends(get_wait_time),
    master_repo=Depends(get_master_repo),
    super_repo=Depends(get_super_repo),
):
    recent = await jobs.count_recent()
    failed = await jobs.count_recently_failed()
    jpm = await metrics.jobs_processed_per_minute()
    max_runtime = await metrics.queue_with_maximum_runtime()
    max_tp = await metrics.queue_with_maximum_throughput()

    waits = await wait_time.calculate()
    wait_val = dict(list(waits.items())[:1]) if waits else {}

    processes = 0
    paused_masters = 0
    status = "inactive"

    if master_repo and super_repo:
        masters = await master_repo.all_masters()
        supps = await super_repo.all_supervisors()

        paused_masters = len([m for m in masters if m.get("status") == "paused"])

        if masters:
            status = "paused" if paused_masters > 0 else "running"

        for supervisor in supps:
            process_dict = supervisor.get("processes", {})
            processes += sum(int(value) for value in process_dict.values())

    return {
        "failedJobs": failed,
        "jobsPerMinute": jpm,
        "pausedMasters": paused_masters,
        "periods": {
            "failedJobs": 10080,
            "recentJobs": 60,
        },
        "processes": processes,
        "queueWithMaxRuntime": max_runtime,
        "queueWithMaxThroughput": max_tp,
        "recentJobs": recent,
        "status": status,
        "wait": wait_val,
    }


@router.get("/masters")
async def get_masters(master_repo=Depends(get_master_repo), super_repo=Depends(get_super_repo)):
    if not master_repo or not super_repo:
        return {}

    masters = await master_repo.all_masters()
    supervisors = await super_repo.all_supervisors()

    supervisors_by_master = {}
    for supervisor in supervisors:
        master_name = supervisor.get("master")
        supervisors_by_master.setdefault(master_name, []).append(supervisor)

    masters_dict = {}
    for master in masters:
        master_name = master.get("name")
        master["supervisors"] = supervisors_by_master.get(master_name, [])
        masters_dict[master_name] = master

    return masters_dict


@router.get("/monitoring")
async def get_monitoring(tags=Depends(get_tag_repo)):
    monitored_tags = await tags.monitors()
    results = []
    for tag in monitored_tags:
        count = await tags.count(tag)
        failed_count = await tags.count(f"failed:{tag}")
        results.append(
            {
                "tag": tag,
                "count": count + failed_count,
            }
        )
    results.sort(key=lambda value: value["tag"])
    return results


@router.post("/monitoring")
async def start_monitoring(tag_req: MonitorTagRequest, tags=Depends(get_tag_repo)):
    await tags.monitor(tag_req.tag)
    return {"status": "success"}


@router.get("/monitoring/{tag}")
async def paginate_monitoring(
    tag: str,
    starting_at: int = -1,
    jobs=Depends(get_job_repo),
    tags=Depends(get_tag_repo),
):
    results = await jobs.get_jobs_by_tag(tag, starting_at if starting_at >= 0 else None)
    return {
        "jobs": results,
        "total": await tags.count(tag),
    }


@router.delete("/monitoring/{tag}")
async def stop_monitoring(tag: str, tags=Depends(get_tag_repo)):
    await tags.stop_monitoring(tag)
    return {"status": "success"}


@router.get("/batches")
async def get_batches(
    starting_at: str | None = None,
    query: str | None = None,
    batches=Depends(get_batch_repo),
):
    if not batches:
        return {"batches": [], "total": 0}
    return await batches.get_batches(limit=50, before_id=starting_at, search_query=query)


@router.get("/batches/{batch_id}")
async def get_batch_detail(batch_id: str, request: Request, batches=Depends(get_batch_repo)):
    if not batches:
        return {"error": "Not Configured"}
    broker = getattr(request.app.state, "broker", None)

    batch = await batches.find_batch(batch_id, broker=broker)
    if not batch:
        return {"error": "Not Found"}
    return batch


@router.post("/batches/retry/{batch_id}")
async def retry_batch(
    batch_id: str,
    batches=Depends(get_batch_repo),
    retry_jobs=Depends(get_retry_job_handler),
):
    if not batches:
        return {"error": "Not Configured"}

    batch = await batches.find_batch(batch_id, broker=None)
    if not batch or "batch" not in batch:
        return {"error": "Not Found"}

    failed_job_ids = batch["batch"].get("failedJobIds", [])
    if not failed_job_ids:
        return {"status": "No failed jobs to retry"}

    retried = []
    missing = []
    errors = {}
    for failed_job_id in failed_job_ids:
        try:
            retry_id = await retry_jobs.handle(failed_job_id)
            if retry_id is None:
                missing.append(failed_job_id)
                continue
            retried.append(retry_id)
        except RuntimeError as exc:
            errors[failed_job_id] = str(exc)
        except Exception as exc:
            missing.append(failed_job_id)
            errors[failed_job_id] = str(exc)

    return {
        "status": "retry_dispatched",
        "retried": len(retried),
        "retryIds": retried,
        "missing": missing,
        "errors": errors,
    }


@router.get("/workload")
async def get_workload(workload=Depends(get_workload_repo)):
    return await workload.get()


@router.get("/jobs/pending")
async def get_pending_jobs(starting_at: int = -1, jobs=Depends(get_job_repo)):
    results = await jobs.get_pending(starting_at if starting_at >= 0 else None)
    total = await jobs.count_pending()
    return {"jobs": results, "total": total}


@router.get("/jobs/recent")
async def get_recent_jobs(starting_at: int = -1, jobs=Depends(get_job_repo)):
    results = await jobs.get_recent(starting_at if starting_at >= 0 else None)
    total = await jobs.count_recent()
    return {"jobs": results, "total": total}


@router.get("/jobs/completed")
async def get_completed_jobs(starting_at: int = -1, jobs=Depends(get_job_repo)):
    results = await jobs.get_completed(starting_at if starting_at >= 0 else None)
    total = await jobs.count_completed()
    return {"jobs": results, "total": total}


@router.get("/jobs/failed")
async def get_failed_jobs(
    starting_at: int = -1,
    tag: str | None = None,
    jobs=Depends(get_job_repo),
    tags=Depends(get_tag_repo),
):
    index = starting_at if starting_at >= 0 else None
    if tag:
        results = await jobs.get_jobs_by_tag(f"failed:{tag}", index)
        total = await tags.count(f"failed:{tag}")
        return {"jobs": results, "total": total}

    results = await jobs.get_failed(index)
    total = await jobs.count_failed()
    return {"jobs": results, "total": total}


@router.get("/jobs/silenced")
async def get_silenced_jobs(starting_at: int = -1, jobs=Depends(get_job_repo)):
    results = await jobs.get_silenced(starting_at if starting_at >= 0 else None)
    total = await jobs.count_silenced()
    return {"jobs": results, "total": total}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, jobs=Depends(get_job_repo)):
    job_results = await jobs.get_jobs([job_id])
    if not job_results:
        return {"error": "Not Found"}
    return job_results[0]


@router.get("/jobs/failed/{job_id}")
async def get_failed_job(job_id: str, jobs=Depends(get_job_repo)):
    job = await jobs.find_failed(job_id)
    if not job:
        return {"error": "Not Found"}
    return job


@router.delete("/jobs/failed/{job_id}")
async def delete_failed_job(job_id: str, jobs=Depends(get_job_repo)):
    deleted = await jobs.delete_failed(job_id)
    if not deleted:
        return {"error": "Not Found"}
    return {"status": "deleted", "id": job_id}


@router.post("/jobs/retry/{job_id}")
async def retry_job(
    job_id: str,
    retry_jobs=Depends(get_retry_job_handler),
):
    try:
        retry_id = await retry_jobs.handle(job_id)
    except RuntimeError as exc:
        return {"error": str(exc)}

    if retry_id is None:
        return {"error": "Not Found"}

    return {"status": "retry_dispatched", "id": retry_id}


@router.get("/metrics/jobs")
async def get_metrics_jobs(metrics=Depends(get_metrics_repo)):
    return await metrics.measured_jobs()


@router.get("/metrics/queues")
async def get_metrics_queues(metrics=Depends(get_metrics_repo)):
    return await metrics.measured_queues()


@router.get("/metrics/jobs/{job_id}")
async def get_metrics_job_detail(job_id: str, metrics=Depends(get_metrics_repo)):
    return await metrics.snapshots_for_job(job_id)


@router.get("/metrics/queues/{queue_id}")
async def get_metrics_queue_detail(queue_id: str, metrics=Depends(get_metrics_repo)):
    return await metrics.snapshots_for_queue(queue_id)


@router.post("/queues/{queue}/purge")
@router.delete("/queues/{queue}/purge")
async def purge_queue(queue: str, jobs=Depends(get_job_repo)):
    purged = await jobs.purge(queue)
    return {"status": "purged", "queue": queue, "count": purged}


@router.post("/masters/pause")
async def pause_masters(
    master_repo=Depends(get_master_repo),
    command_queue=Depends(get_command_queue),
):
    if not master_repo or not command_queue:
        return {"status": "error", "message": "Command queue not available"}

    masters = await master_repo.all_masters()
    for master in masters:
        for supervisor_name in master.get("supervisors", []):
            await command_queue.push(supervisor_name, "pause")

    return {"status": "ok", "message": f"Pause sent to {len(masters)} master(s)"}


@router.post("/masters/continue")
async def continue_masters(
    master_repo=Depends(get_master_repo),
    command_queue=Depends(get_command_queue),
):
    if not master_repo or not command_queue:
        return {"status": "error", "message": "Command queue not available"}

    masters = await master_repo.all_masters()
    for master in masters:
        for supervisor_name in master.get("supervisors", []):
            await command_queue.push(supervisor_name, "continue")

    return {"status": "ok", "message": f"Continue sent to {len(masters)} master(s)"}


@router.post("/masters/terminate")
async def terminate_masters(
    master_repo=Depends(get_master_repo),
    command_queue=Depends(get_command_queue),
):
    if not master_repo or not command_queue:
        return {"status": "error", "message": "Command queue not available"}

    masters = await master_repo.all_masters()
    for master in masters:
        for supervisor_name in master.get("supervisors", []):
            await command_queue.push(supervisor_name, "terminate", {"status": 0})

    return {"status": "ok", "message": f"Terminate sent to {len(masters)} master(s)"}


@router.post("/supervisors/{name}/scale")
async def scale_supervisor(
    name: str,
    body: ScaleRequest,
    command_queue=Depends(get_command_queue),
):
    if not command_queue:
        return {"status": "error", "message": "Command queue not available"}

    await command_queue.push(name, "scale", {"scale": body.scale})
    return {"status": "ok", "message": f"Scale to {body.scale} sent to '{name}'"}
