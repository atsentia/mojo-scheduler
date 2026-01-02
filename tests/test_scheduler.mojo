"""
Scheduler Tests

Unit tests for cron parsing, job management, and scheduling.
"""

from mojo_scheduler import (
    CronExpr,
    CronField,
    parse_cron,
    cron_matches,
    Job,
    JobStatus,
    JobResult,
    Scheduler,
    SchedulerStats,
    get_scheduler_stats,
)


# =============================================================================
# Cron Field Tests
# =============================================================================

fn test_cron_field_wildcard() raises:
    """Test wildcard field (*)."""
    var field = CronField(0, 59)
    field.set_all()

    if not field.matches(0):
        raise Error("Wildcard should match 0")
    if not field.matches(30):
        raise Error("Wildcard should match 30")
    if not field.matches(59):
        raise Error("Wildcard should match 59")

    print("+ Cron field wildcard works")


fn test_cron_field_single_value() raises:
    """Test single value field."""
    var field = CronField(0, 59)
    field.set_value(15)

    if not field.matches(15):
        raise Error("Should match 15")
    if field.matches(0):
        raise Error("Should not match 0")
    if field.matches(30):
        raise Error("Should not match 30")

    print("+ Cron field single value works")


fn test_cron_field_range() raises:
    """Test range field (1-5)."""
    var field = CronField(0, 59)
    field.set_range(10, 15)

    if field.matches(9):
        raise Error("Should not match 9")
    if not field.matches(10):
        raise Error("Should match 10")
    if not field.matches(12):
        raise Error("Should match 12")
    if not field.matches(15):
        raise Error("Should match 15")
    if field.matches(16):
        raise Error("Should not match 16")

    print("+ Cron field range works")


fn test_cron_field_step() raises:
    """Test step field (*/5)."""
    var field = CronField(0, 59)
    field.set_range(0, 59, 5)

    if not field.matches(0):
        raise Error("Should match 0")
    if not field.matches(5):
        raise Error("Should match 5")
    if not field.matches(10):
        raise Error("Should match 10")
    if not field.matches(55):
        raise Error("Should match 55")
    if field.matches(3):
        raise Error("Should not match 3")
    if field.matches(7):
        raise Error("Should not match 7")

    print("+ Cron field step works")


fn test_cron_field_next_match() raises:
    """Test next match finding."""
    var field = CronField(0, 59)
    field.set_range(0, 59, 15)  # 0, 15, 30, 45

    var next = field.next_match(0)
    if next != 0:
        raise Error("Next from 0 should be 0, got " + str(next))

    next = field.next_match(1)
    if next != 15:
        raise Error("Next from 1 should be 15, got " + str(next))

    next = field.next_match(16)
    if next != 30:
        raise Error("Next from 16 should be 30, got " + str(next))

    next = field.next_match(50)
    if next != -1:
        raise Error("Next from 50 should be -1 (wrap), got " + str(next))

    print("+ Cron field next match works")


# =============================================================================
# Cron Expression Tests
# =============================================================================

fn test_cron_parse_simple() raises:
    """Test parsing simple expressions."""
    var cron = CronExpr.parse("0 0 * * *")

    if not cron.is_valid:
        raise Error("Should be valid: " + cron.error_msg)

    # Midnight
    if not cron.matches(0, 0, 15, 6, 3):
        raise Error("Should match midnight")

    # Not midnight
    if cron.matches(30, 0, 15, 6, 3):
        raise Error("Should not match 00:30")
    if cron.matches(0, 12, 15, 6, 3):
        raise Error("Should not match 12:00")

    print("+ Cron parse simple works")


fn test_cron_parse_every_5_minutes() raises:
    """Test parsing */5 expression."""
    var cron = CronExpr.parse("*/5 * * * *")

    if not cron.is_valid:
        raise Error("Should be valid: " + cron.error_msg)

    if not cron.matches(0, 12, 15, 6, 3):
        raise Error("Should match :00")
    if not cron.matches(5, 12, 15, 6, 3):
        raise Error("Should match :05")
    if not cron.matches(10, 12, 15, 6, 3):
        raise Error("Should match :10")
    if not cron.matches(55, 12, 15, 6, 3):
        raise Error("Should match :55")

    if cron.matches(3, 12, 15, 6, 3):
        raise Error("Should not match :03")
    if cron.matches(7, 12, 15, 6, 3):
        raise Error("Should not match :07")

    print("+ Cron parse every 5 minutes works")


fn test_cron_parse_range() raises:
    """Test parsing range expression."""
    var cron = CronExpr.parse("0 9-17 * * *")

    if not cron.is_valid:
        raise Error("Should be valid: " + cron.error_msg)

    if not cron.matches(0, 9, 15, 6, 3):
        raise Error("Should match 9:00")
    if not cron.matches(0, 12, 15, 6, 3):
        raise Error("Should match 12:00")
    if not cron.matches(0, 17, 15, 6, 3):
        raise Error("Should match 17:00")

    if cron.matches(0, 8, 15, 6, 3):
        raise Error("Should not match 8:00")
    if cron.matches(0, 18, 15, 6, 3):
        raise Error("Should not match 18:00")

    print("+ Cron parse range works")


fn test_cron_parse_list() raises:
    """Test parsing list expression."""
    var cron = CronExpr.parse("0 0 1,15 * *")

    if not cron.is_valid:
        raise Error("Should be valid: " + cron.error_msg)

    if not cron.matches(0, 0, 1, 6, 3):
        raise Error("Should match 1st")
    if not cron.matches(0, 0, 15, 6, 3):
        raise Error("Should match 15th")

    if cron.matches(0, 0, 2, 6, 3):
        raise Error("Should not match 2nd")
    if cron.matches(0, 0, 14, 6, 3):
        raise Error("Should not match 14th")

    print("+ Cron parse list works")


fn test_cron_parse_weekday() raises:
    """Test parsing weekday expression."""
    # Weekdays only (1-5, Monday-Friday)
    var cron = CronExpr.parse("0 9 * * 1-5")

    if not cron.is_valid:
        raise Error("Should be valid: " + cron.error_msg)

    # Monday (1)
    if not cron.matches(0, 9, 15, 6, 1):
        raise Error("Should match Monday")

    # Friday (5)
    if not cron.matches(0, 9, 15, 6, 5):
        raise Error("Should match Friday")

    # Sunday (0)
    if cron.matches(0, 9, 15, 6, 0):
        raise Error("Should not match Sunday")

    # Saturday (6)
    if cron.matches(0, 9, 15, 6, 6):
        raise Error("Should not match Saturday")

    print("+ Cron parse weekday works")


fn test_cron_parse_invalid() raises:
    """Test parsing invalid expressions."""
    # Too few fields
    var cron1 = CronExpr.parse("0 0 * *")
    if cron1.is_valid:
        raise Error("Should be invalid (4 fields)")

    # Too many fields
    var cron2 = CronExpr.parse("0 0 * * * *")
    if cron2.is_valid:
        raise Error("Should be invalid (6 fields)")

    # Invalid character
    var cron3 = CronExpr.parse("x 0 * * *")
    if cron3.is_valid:
        raise Error("Should be invalid (non-numeric)")

    print("+ Cron parse invalid works")


fn test_cron_convenience() raises:
    """Test convenience functions."""
    var cron = parse_cron("*/10 * * * *")
    if not cron.is_valid:
        raise Error("parse_cron should work")

    if not cron_matches("0 12 * * *", 0, 12, 15, 6, 3):
        raise Error("cron_matches should match noon")

    if cron_matches("0 12 * * *", 0, 13, 15, 6, 3):
        raise Error("cron_matches should not match 1pm")

    print("+ Cron convenience functions work")


# =============================================================================
# Job Tests
# =============================================================================

fn test_job_creation() raises:
    """Test job creation."""
    var job = Job("backup", "Daily Backup", "0 0 * * *")

    if job.id != "backup":
        raise Error("ID should be 'backup'")
    if job.name != "Daily Backup":
        raise Error("Name should be 'Daily Backup'")
    if not job.is_valid():
        raise Error("Job should be valid")
    if not job.enabled:
        raise Error("Job should be enabled by default")
    if job.status != JobStatus.PENDING:
        raise Error("Status should be PENDING")

    print("+ Job creation works")


fn test_job_invalid_schedule() raises:
    """Test job with invalid schedule."""
    var job = Job("bad", "Bad Job", "invalid cron")

    if job.is_valid():
        raise Error("Job with invalid schedule should not be valid")

    print("+ Job invalid schedule detection works")


fn test_job_next_run() raises:
    """Test job next run calculation."""
    var job = Job("test", "Test Job", "0 0 * * *")  # Daily at midnight

    # Set time to 2023-12-30 12:00:00 (timestamp: 1703937600)
    job.set_current_time(1703937600)

    # Next run should be midnight of the next day
    # 2023-12-31 00:00:00 = 1703980800
    if job.next_run_timestamp != 1703980800:
        raise Error(
            "Next run should be 1703980800, got " + str(job.next_run_timestamp)
        )

    print("+ Job next run calculation works")


fn test_job_is_due() raises:
    """Test job due detection."""
    var job = Job("test", "Test Job", "0 0 * * *")
    job.set_current_time(1703937600)  # 2023-12-30 12:00:00

    # Before next run time
    if job.is_due(1703937600):
        raise Error("Should not be due before next run time")

    # At next run time
    if not job.is_due(1703980800):
        raise Error("Should be due at next run time")

    # After next run time
    if not job.is_due(1703980900):
        raise Error("Should be due after next run time")

    print("+ Job due detection works")


fn test_job_status_transitions() raises:
    """Test job status transitions."""
    var job = Job("test", "Test Job", "0 0 * * *")
    job.set_current_time(1703937600)

    # Initial state
    if job.status != JobStatus.PENDING:
        raise Error("Initial status should be PENDING")

    # Mark started
    job.mark_started(1703980800)
    if job.status != JobStatus.RUNNING:
        raise Error("Status should be RUNNING after mark_started")
    if job.last_run_timestamp != 1703980800:
        raise Error("last_run_timestamp should be updated")

    # Mark completed
    job.mark_completed(1703980860)
    if job.status != JobStatus.COMPLETED:
        raise Error("Status should be COMPLETED after mark_completed")
    if job.run_count != 1:
        raise Error("run_count should be 1")

    print("+ Job status transitions work")


fn test_job_disable_enable() raises:
    """Test job disable/enable."""
    var job = Job("test", "Test Job", "0 0 * * *")
    job.set_current_time(1703937600)

    # Disable
    job.disable()
    if job.enabled:
        raise Error("Job should be disabled")
    if job.status != JobStatus.DISABLED:
        raise Error("Status should be DISABLED")

    # Should not be due even at next run time
    if job.is_due(1703980800):
        raise Error("Disabled job should not be due")

    # Enable
    job.enable(1703980800)
    if not job.enabled:
        raise Error("Job should be enabled")
    if job.status != JobStatus.PENDING:
        raise Error("Status should be PENDING after enable")

    print("+ Job disable/enable works")


# =============================================================================
# Scheduler Tests
# =============================================================================

fn test_scheduler_add_remove() raises:
    """Test adding and removing jobs."""
    var scheduler = Scheduler()

    # Add job
    if not scheduler.add_job("job1", "Job 1", "0 0 * * *"):
        raise Error("Should add job1")

    if scheduler.job_count() != 1:
        raise Error("Should have 1 job")

    if not scheduler.has_job("job1"):
        raise Error("Should have job1")

    # Add duplicate (should fail)
    if scheduler.add_job("job1", "Job 1 Dup", "0 0 * * *"):
        raise Error("Should not add duplicate ID")

    # Add another job
    if not scheduler.add_job("job2", "Job 2", "*/15 * * * *"):
        raise Error("Should add job2")

    if scheduler.job_count() != 2:
        raise Error("Should have 2 jobs")

    # Remove job
    if not scheduler.remove_job("job1"):
        raise Error("Should remove job1")

    if scheduler.job_count() != 1:
        raise Error("Should have 1 job after removal")

    if scheduler.has_job("job1"):
        raise Error("Should not have job1 after removal")

    # Remove non-existent
    if scheduler.remove_job("nonexistent"):
        raise Error("Should not remove non-existent job")

    print("+ Scheduler add/remove works")


fn test_scheduler_invalid_job() raises:
    """Test adding job with invalid schedule."""
    var scheduler = Scheduler()

    if scheduler.add_job("bad", "Bad Job", "invalid"):
        raise Error("Should not add job with invalid schedule")

    if scheduler.job_count() != 0:
        raise Error("Should have 0 jobs")

    print("+ Scheduler invalid job rejection works")


fn test_scheduler_due_jobs() raises:
    """Test getting due jobs."""
    var scheduler = Scheduler()

    # Set initial time: 2023-12-30 23:50:00
    scheduler.set_time(1703980200)

    # Add jobs with different schedules
    _ = scheduler.add_job("midnight", "Midnight Job", "0 0 * * *")  # Due at 00:00
    _ = scheduler.add_job("hourly", "Hourly Job", "0 * * * *")  # Due at next :00

    # No jobs due yet
    var due = scheduler.get_due_jobs()
    if len(due) != 0:
        raise Error("No jobs should be due yet")

    # Advance to midnight: 2023-12-31 00:00:00
    scheduler.set_time(1703980800)

    # Both jobs should be due
    due = scheduler.get_due_jobs()
    if len(due) != 2:
        raise Error("Should have 2 due jobs, got " + str(len(due)))

    print("+ Scheduler due jobs detection works")


fn test_scheduler_mark_jobs() raises:
    """Test marking jobs started/completed/failed."""
    var scheduler = Scheduler()
    scheduler.set_time(1703980800)  # Midnight

    _ = scheduler.add_job("test", "Test Job", "0 0 * * *")

    # Mark started
    if not scheduler.mark_job_started("test"):
        raise Error("Should mark test started")

    var job = scheduler.get_job("test")
    if not job:
        raise Error("Should get test job")
    if job.value().status != JobStatus.RUNNING:
        raise Error("Status should be RUNNING")

    # Running job should not be due
    var due = scheduler.get_due_jobs()
    if len(due) != 0:
        raise Error("Running job should not be in due list")

    # Mark completed
    if not scheduler.mark_job_completed("test"):
        raise Error("Should mark test completed")

    job = scheduler.get_job("test")
    if job.value().status != JobStatus.COMPLETED:
        raise Error("Status should be COMPLETED")
    if job.value().run_count != 1:
        raise Error("run_count should be 1")

    print("+ Scheduler mark jobs works")


fn test_scheduler_time_advance() raises:
    """Test time advancement."""
    var scheduler = Scheduler()
    scheduler.set_time(0)

    scheduler.advance_time(60)
    if scheduler.current_time != 60:
        raise Error("advance_time should add 60 seconds")

    scheduler.advance_minutes(5)
    if scheduler.current_time != 360:
        raise Error("advance_minutes should add 5 minutes")

    scheduler.advance_hours(1)
    if scheduler.current_time != 3960:
        raise Error("advance_hours should add 1 hour")

    scheduler.advance_days(1)
    if scheduler.current_time != 90360:
        raise Error("advance_days should add 1 day")

    print("+ Scheduler time advancement works")


fn test_scheduler_disable_enable() raises:
    """Test disabling and enabling jobs via scheduler."""
    var scheduler = Scheduler()
    scheduler.set_time(1703980800)

    _ = scheduler.add_job("test", "Test Job", "0 0 * * *")

    # Disable
    if not scheduler.disable_job("test"):
        raise Error("Should disable test")

    var due = scheduler.get_due_jobs()
    if len(due) != 0:
        raise Error("Disabled job should not be due")

    # Enable
    if not scheduler.enable_job("test"):
        raise Error("Should enable test")

    due = scheduler.get_due_jobs()
    # After enabling, job recalculates next run, so may not be immediately due
    # Just verify it's enabled
    var job = scheduler.get_job("test")
    if not job.value().enabled:
        raise Error("Job should be enabled")

    print("+ Scheduler disable/enable works")


fn test_scheduler_clear() raises:
    """Test clearing all jobs."""
    var scheduler = Scheduler()

    _ = scheduler.add_job("job1", "Job 1", "0 0 * * *")
    _ = scheduler.add_job("job2", "Job 2", "0 0 * * *")
    _ = scheduler.add_job("job3", "Job 3", "0 0 * * *")

    if scheduler.job_count() != 3:
        raise Error("Should have 3 jobs")

    scheduler.clear()

    if scheduler.job_count() != 0:
        raise Error("Should have 0 jobs after clear")

    print("+ Scheduler clear works")


fn test_scheduler_stats() raises:
    """Test scheduler statistics."""
    var scheduler = Scheduler()
    scheduler.set_time(1703980800)

    _ = scheduler.add_job("job1", "Job 1", "0 0 * * *")
    _ = scheduler.add_job("job2", "Job 2", "0 0 * * *")

    # Mark one job completed
    _ = scheduler.mark_job_started("job1")
    _ = scheduler.mark_job_completed("job1")

    # Mark one job failed
    _ = scheduler.mark_job_started("job2")
    _ = scheduler.mark_job_failed("job2")

    var stats = get_scheduler_stats(scheduler)

    if stats.total_jobs != 2:
        raise Error("total_jobs should be 2")
    if stats.total_runs != 2:
        raise Error("total_runs should be 2")
    if stats.total_failures != 1:
        raise Error("total_failures should be 1")

    var rate = stats.success_rate()
    if rate < 0.45 or rate > 0.55:
        raise Error("success_rate should be ~0.5, got " + str(rate))

    print("+ Scheduler stats works")


fn test_scheduler_next_due_time() raises:
    """Test next due time calculation."""
    var scheduler = Scheduler()
    scheduler.set_time(1703980800)  # 2023-12-31 00:00:00

    _ = scheduler.add_job("hourly", "Hourly", "0 * * * *")  # Every hour

    var next_due = scheduler.next_due_time()
    # Next hour is 01:00:00 = 1703984400
    if next_due != 1703984400:
        raise Error("next_due_time should be 1703984400, got " + str(next_due))

    var time_until = scheduler.time_until_next_due()
    # 1 hour = 3600 seconds
    if time_until != 3600:
        raise Error("time_until_next_due should be 3600, got " + str(time_until))

    print("+ Scheduler next due time works")


fn test_scheduler_all_job_ids() raises:
    """Test getting all job IDs."""
    var scheduler = Scheduler()

    _ = scheduler.add_job("alpha", "Alpha", "0 0 * * *")
    _ = scheduler.add_job("beta", "Beta", "0 0 * * *")
    _ = scheduler.add_job("gamma", "Gamma", "0 0 * * *")

    var ids = scheduler.all_job_ids()

    if len(ids) != 3:
        raise Error("Should have 3 job IDs")

    # Check all IDs present (order may vary based on implementation)
    var found_alpha = False
    var found_beta = False
    var found_gamma = False

    for i in range(len(ids)):
        if ids[i] == "alpha":
            found_alpha = True
        elif ids[i] == "beta":
            found_beta = True
        elif ids[i] == "gamma":
            found_gamma = True

    if not found_alpha or not found_beta or not found_gamma:
        raise Error("Missing some job IDs")

    print("+ Scheduler all_job_ids works")


# =============================================================================
# Integration Tests
# =============================================================================

fn test_integration_daily_job_cycle() raises:
    """Test a full daily job cycle."""
    var scheduler = Scheduler()

    # Start at 2023-12-30 23:55:00
    scheduler.set_time(1703980500)

    # Add daily midnight job
    _ = scheduler.add_job("daily", "Daily Report", "0 0 * * *")

    # Not due yet
    if scheduler.get_due_job_count() != 0:
        raise Error("Job should not be due at 23:55")

    # Advance to midnight (5 minutes)
    scheduler.advance_minutes(5)

    # Now due
    if scheduler.get_due_job_count() != 1:
        raise Error("Job should be due at 00:00")

    # Execute job
    var due = scheduler.get_due_jobs()
    _ = scheduler.mark_job_started(due[0])
    _ = scheduler.mark_job_completed(due[0])

    # Should not be due anymore
    if scheduler.get_due_job_count() != 0:
        raise Error("Job should not be due after completion")

    # Advance 23 hours
    scheduler.advance_hours(23)

    # Still not due
    if scheduler.get_due_job_count() != 0:
        raise Error("Job should not be due at 23:00 next day")

    # Advance 1 more hour (next midnight)
    scheduler.advance_hours(1)

    # Due again
    if scheduler.get_due_job_count() != 1:
        raise Error("Job should be due at next midnight")

    print("+ Integration: Daily job cycle works")


fn test_integration_every_5_minutes() raises:
    """Test a job that runs every 5 minutes."""
    var scheduler = Scheduler()
    scheduler.set_time(0)  # 1970-01-01 00:00:00

    _ = scheduler.add_job("frequent", "Every 5 Min", "*/5 * * * *")

    # At :00, job should be due
    if scheduler.get_due_job_count() != 1:
        raise Error("Job should be due at :00")

    # Complete it
    var due = scheduler.get_due_jobs()
    _ = scheduler.mark_job_started(due[0])
    _ = scheduler.mark_job_completed(due[0])

    # Advance 3 minutes (:03)
    scheduler.advance_minutes(3)
    if scheduler.get_due_job_count() != 0:
        raise Error("Job should not be due at :03")

    # Advance 2 more minutes (:05)
    scheduler.advance_minutes(2)
    if scheduler.get_due_job_count() != 1:
        raise Error("Job should be due at :05")

    print("+ Integration: Every 5 minutes works")


fn test_integration_multiple_jobs() raises:
    """Test multiple jobs with different schedules."""
    var scheduler = Scheduler()
    scheduler.set_time(1703980800)  # 2023-12-31 00:00:00 (Monday)

    # Different schedules
    _ = scheduler.add_job("hourly", "Hourly", "0 * * * *")
    _ = scheduler.add_job("daily", "Daily", "0 0 * * *")
    _ = scheduler.add_job("weekly", "Weekly", "0 0 * * 1")  # Monday

    # All due at midnight Monday
    if scheduler.get_due_job_count() != 3:
        raise Error("All 3 jobs should be due at Monday midnight")

    # Complete all
    var due = scheduler.get_due_jobs()
    for i in range(len(due)):
        _ = scheduler.mark_job_started(due[i])
        _ = scheduler.mark_job_completed(due[i])

    # Advance 1 hour
    scheduler.advance_hours(1)

    # Only hourly due
    if scheduler.get_due_job_count() != 1:
        raise Error("Only hourly should be due at 01:00")

    print("+ Integration: Multiple jobs works")


# =============================================================================
# Main
# =============================================================================

fn main() raises:
    print("Running Scheduler tests...\n")

    # Cron field tests
    test_cron_field_wildcard()
    test_cron_field_single_value()
    test_cron_field_range()
    test_cron_field_step()
    test_cron_field_next_match()

    # Cron expression tests
    test_cron_parse_simple()
    test_cron_parse_every_5_minutes()
    test_cron_parse_range()
    test_cron_parse_list()
    test_cron_parse_weekday()
    test_cron_parse_invalid()
    test_cron_convenience()

    # Job tests
    test_job_creation()
    test_job_invalid_schedule()
    test_job_next_run()
    test_job_is_due()
    test_job_status_transitions()
    test_job_disable_enable()

    # Scheduler tests
    test_scheduler_add_remove()
    test_scheduler_invalid_job()
    test_scheduler_due_jobs()
    test_scheduler_mark_jobs()
    test_scheduler_time_advance()
    test_scheduler_disable_enable()
    test_scheduler_clear()
    test_scheduler_stats()
    test_scheduler_next_due_time()
    test_scheduler_all_job_ids()

    # Integration tests
    test_integration_daily_job_cycle()
    test_integration_every_5_minutes()
    test_integration_multiple_jobs()

    print("\n[OK] All Scheduler tests passed!")
