"""
Mojo Scheduler Library

Pure Mojo job scheduling with cron expression support.

Cron Expression Format:
    "minute hour day month weekday"

    minute:  0-59
    hour:    0-23
    day:     1-31
    month:   1-12
    weekday: 0-6 (0=Sunday)

Cron Special Characters:
    *     Any value
    1-5   Range (1 through 5)
    1,3,5 List (1, 3, and 5)
    */5   Step (every 5 units)

Common Patterns:
    "0 0 * * *"      Daily at midnight
    "*/5 * * * *"    Every 5 minutes
    "0 9-17 * * 1-5" 9am-5pm on weekdays
    "0 0 1 * *"      First of each month
    "0 0 * * 0"      Every Sunday at midnight

Example:
    from mojo_scheduler import Scheduler, Job, CronExpr

    # Create scheduler
    var scheduler = Scheduler()

    # Add jobs
    scheduler.add_job("backup", "Daily Backup", "0 0 * * *")
    scheduler.add_job("cleanup", "Hourly Cleanup", "0 * * * *")

    # Set current time (Unix timestamp)
    scheduler.set_time(1703980800)  # 2023-12-31 00:00:00

    # Check for due jobs
    var due_jobs = scheduler.get_due_jobs()
    for job_id in due_jobs:
        scheduler.mark_job_started(job_id)
        # Execute job...
        scheduler.mark_job_completed(job_id)

    # Parse cron directly
    var cron = CronExpr.parse("*/15 * * * *")
    if cron.matches(15, 10, 25, 12, 3):  # minute, hour, day, month, weekday
        print("Matches!")
"""

from .cron import CronExpr, CronField, parse_cron, cron_matches
from .job import Job, JobStatus, JobResult
from .scheduler import Scheduler, SchedulerStats, get_scheduler_stats
