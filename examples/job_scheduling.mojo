"""
Example: Job Scheduling with Cron

Demonstrates:
- Cron expression parsing
- Job registration and management
- Checking for due jobs
- Job execution tracking
"""

from mojo_scheduler import Scheduler, Job, JobStatus
from mojo_scheduler import CronExpr, parse_cron, cron_matches


fn cron_expression_example():
    """Parse and use cron expressions."""
    print("=== Cron Expressions ===")

    # Common patterns
    var patterns = List[Tuple[String, String]]()
    patterns.append(("Every minute", "* * * * *"))
    patterns.append(("Every 5 minutes", "*/5 * * * *"))
    patterns.append(("Hourly", "0 * * * *"))
    patterns.append(("Daily at midnight", "0 0 * * *"))
    patterns.append(("Weekdays 9am", "0 9 * * 1-5"))
    patterns.append(("First of month", "0 0 1 * *"))

    for pattern in patterns:
        print(pattern[0] + ": " + pattern[1])

    print("\nCron format: minute hour day month weekday")
    print("  *     = any value")
    print("  1-5   = range")
    print("  1,3,5 = list")
    print("  */5   = step")
    print("")


fn scheduler_example():
    """Basic job scheduling."""
    print("=== Job Scheduler ===")

    # Create scheduler
    var scheduler = Scheduler()

    # Add jobs
    scheduler.add_job("backup", "Daily Backup", "0 0 * * *")
    scheduler.add_job("cleanup", "Hourly Cleanup", "0 * * * *")
    scheduler.add_job("report", "Weekly Report", "0 9 * * 1")
    scheduler.add_job("health", "Health Check", "*/5 * * * *")

    print("Registered jobs:")
    print("  backup  - Daily at midnight")
    print("  cleanup - Every hour")
    print("  report  - Monday 9am")
    print("  health  - Every 5 minutes")

    # Set current time (simulated)
    # scheduler.set_time(1703980800)  # 2023-12-31 00:00:00

    print("")


fn job_execution_example():
    """Execute and track jobs."""
    print("=== Job Execution ===")

    var scheduler = Scheduler()
    scheduler.add_job("task1", "Task 1", "*/5 * * * *")

    # Check for due jobs
    var due_jobs = scheduler.get_due_jobs()
    print("Due jobs: " + String(len(due_jobs)))

    for job_id in due_jobs:
        # Mark job as started
        scheduler.mark_job_started(job_id)
        print("Started: " + job_id)

        # Execute job (simulated)
        var success = True

        if success:
            scheduler.mark_job_completed(job_id)
            print("Completed: " + job_id)
        else:
            scheduler.mark_job_failed(job_id, "Task failed")
            print("Failed: " + job_id)

    print("")


fn cron_matching_example():
    """Check if time matches cron expression."""
    print("=== Cron Matching ===")

    var cron = CronExpr.parse("*/15 * * * *")

    # Check various times
    var times = List[Tuple[Int, Int, Int, Int, Int]]()
    times.append((0, 10, 25, 12, 3))   # 10:00
    times.append((15, 10, 25, 12, 3))  # 10:15
    times.append((30, 10, 25, 12, 3))  # 10:30
    times.append((7, 10, 25, 12, 3))   # 10:07

    for t in times:
        var matches = cron.matches(t[0], t[1], t[2], t[3], t[4])
        var time_str = String(t[1]) + ":" + (String(t[0]) if t[0] >= 10 else "0" + String(t[0]))
        print(time_str + " matches '*/15 * * * *': " + String(matches))

    print("")


fn job_status_tracking():
    """Track job status and history."""
    print("=== Job Status Tracking ===")

    var scheduler = Scheduler()
    scheduler.add_job("report", "Generate Report", "0 9 * * *")

    # Get job info
    var job = scheduler.get_job("report")
    if job:
        print("Job: " + job.name)
        print("Cron: " + job.cron_expr)
        print("Status: " + job.status_name())
        print("Last run: " + job.last_run_time)
        print("Next run: " + job.next_run_time)

    # Get scheduler stats
    var stats = scheduler.stats()
    print("\nScheduler stats:")
    print("  Total jobs: " + String(stats.total_jobs))
    print("  Running: " + String(stats.running_jobs))
    print("  Completed: " + String(stats.completed_jobs))
    print("  Failed: " + String(stats.failed_jobs))
    print("")


fn main():
    print("mojo-scheduler: Job Scheduling with Cron\n")

    cron_expression_example()
    scheduler_example()
    job_execution_example()
    cron_matching_example()
    job_status_tracking()

    print("=" * 50)
    print("Use cases:")
    print("  - Background job processing")
    print("  - Periodic data cleanup")
    print("  - Report generation")
    print("  - Health check scheduling")
