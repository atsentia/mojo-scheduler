"""
Job Scheduler

Manages scheduled jobs with:
- Job registration (add/remove)
- Due job detection
- Time management (simulated or real)
- Job execution tracking

Example:
    var scheduler = Scheduler()
    scheduler.add_job("cleanup", "Daily Cleanup", "0 0 * * *")
    scheduler.set_time(1703980800)

    var due_jobs = scheduler.get_due_jobs()
    for job_id in due_jobs:
        # Execute job
        scheduler.mark_job_completed(job_id)
"""

from .job import Job, JobStatus, JobResult
from .cron import CronExpr


# =============================================================================
# Scheduler
# =============================================================================

struct Scheduler(Stringable):
    """
    Job scheduler with cron-based scheduling.

    Manages a collection of jobs and determines which are due to run.

    Example:
        var scheduler = Scheduler()
        scheduler.add_job("backup", "Daily Backup", "0 0 * * *")
        scheduler.set_time(1703980800)

        var due = scheduler.get_due_jobs()
    """
    var jobs: List[Job]
    """List of registered jobs."""

    var current_time: Int64
    """Current simulated time (Unix timestamp)."""

    var auto_time: Bool
    """Whether to use auto-advancing time."""

    fn __init__(out self):
        """Create empty scheduler."""
        self.jobs = List[Job]()
        self.current_time = 0
        self.auto_time = False

    fn __init__(out self, start_time: Int64):
        """Create scheduler with initial time."""
        self.jobs = List[Job]()
        self.current_time = start_time
        self.auto_time = False

    fn set_time(inout self, timestamp: Int64):
        """
        Set current time and update all job schedules.

        Args:
            timestamp: Unix timestamp.
        """
        self.current_time = timestamp
        self._update_all_schedules()

    fn advance_time(inout self, seconds: Int64):
        """
        Advance current time by given seconds.

        Args:
            seconds: Number of seconds to advance.
        """
        self.current_time += seconds

    fn advance_minutes(inout self, minutes: Int):
        """Advance time by minutes."""
        self.advance_time(Int64(minutes) * 60)

    fn advance_hours(inout self, hours: Int):
        """Advance time by hours."""
        self.advance_time(Int64(hours) * 3600)

    fn advance_days(inout self, days: Int):
        """Advance time by days."""
        self.advance_time(Int64(days) * 86400)

    fn add_job(inout self, id: String, name: String, schedule: String) -> Bool:
        """
        Add a new job to the scheduler.

        Args:
            id: Unique job identifier.
            name: Human-readable job name.
            schedule: Cron expression string.

        Returns:
            True if job was added successfully.
        """
        # Check if ID already exists
        for i in range(len(self.jobs)):
            if self.jobs[i].id == id:
                return False

        var job = Job(id, name, schedule)
        if not job.is_valid():
            return False

        job.set_current_time(self.current_time)
        self.jobs.append(job)
        return True

    fn add_job(
        inout self,
        id: String,
        name: String,
        schedule: String,
        metadata: String,
    ) -> Bool:
        """Add job with metadata."""
        if self.has_job(id):
            return False

        var job = Job(id, name, schedule, metadata)
        if not job.is_valid():
            return False

        job.set_current_time(self.current_time)
        self.jobs.append(job)
        return True

    fn remove_job(inout self, id: String) -> Bool:
        """
        Remove a job from the scheduler.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and removed.
        """
        var new_jobs = List[Job]()
        var found = False

        for i in range(len(self.jobs)):
            if self.jobs[i].id != id:
                new_jobs.append(self.jobs[i])
            else:
                found = True

        self.jobs = new_jobs
        return found

    fn has_job(self, id: String) -> Bool:
        """Check if a job exists."""
        for i in range(len(self.jobs)):
            if self.jobs[i].id == id:
                return True
        return False

    fn get_job(self, id: String) -> Optional[Job]:
        """
        Get a job by ID.

        Args:
            id: Job identifier.

        Returns:
            The job if found, None otherwise.
        """
        for i in range(len(self.jobs)):
            if self.jobs[i].id == id:
                return self.jobs[i]
        return None

    fn get_job_index(self, id: String) -> Int:
        """Get index of job, or -1 if not found."""
        for i in range(len(self.jobs)):
            if self.jobs[i].id == id:
                return i
        return -1

    fn get_due_jobs(self) -> List[String]:
        """
        Get list of job IDs that are due to run.

        Returns:
            List of job IDs that should run now.
        """
        var due = List[String]()

        for i in range(len(self.jobs)):
            if self.jobs[i].is_due(self.current_time):
                due.append(self.jobs[i].id)

        return due

    fn get_due_job_count(self) -> Int:
        """Get count of jobs that are due to run."""
        var count = 0
        for i in range(len(self.jobs)):
            if self.jobs[i].is_due(self.current_time):
                count += 1
        return count

    fn mark_job_started(inout self, id: String) -> Bool:
        """
        Mark a job as started.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and updated.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].mark_started(self.current_time)
        return True

    fn mark_job_completed(inout self, id: String) -> Bool:
        """
        Mark a job as completed and calculate next run.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and updated.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].mark_completed(self.current_time)
        return True

    fn mark_job_failed(inout self, id: String) -> Bool:
        """
        Mark a job as failed and calculate next run.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and updated.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].mark_failed(self.current_time)
        return True

    fn disable_job(inout self, id: String) -> Bool:
        """
        Disable a job.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and disabled.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].disable()
        return True

    fn enable_job(inout self, id: String) -> Bool:
        """
        Enable a job.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and enabled.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].enable(self.current_time)
        return True

    fn reset_job(inout self, id: String) -> Bool:
        """
        Reset a job's status.

        Args:
            id: Job identifier.

        Returns:
            True if job was found and reset.
        """
        var idx = self.get_job_index(id)
        if idx < 0:
            return False

        self.jobs[idx].reset(self.current_time)
        return True

    fn job_count(self) -> Int:
        """Get total number of jobs."""
        return len(self.jobs)

    fn enabled_job_count(self) -> Int:
        """Get count of enabled jobs."""
        var count = 0
        for i in range(len(self.jobs)):
            if self.jobs[i].enabled:
                count += 1
        return count

    fn pending_job_count(self) -> Int:
        """Get count of jobs in pending status."""
        var count = 0
        for i in range(len(self.jobs)):
            if self.jobs[i].status == JobStatus.PENDING:
                count += 1
        return count

    fn running_job_count(self) -> Int:
        """Get count of currently running jobs."""
        var count = 0
        for i in range(len(self.jobs)):
            if self.jobs[i].status == JobStatus.RUNNING:
                count += 1
        return count

    fn all_job_ids(self) -> List[String]:
        """Get list of all job IDs."""
        var ids = List[String]()
        for i in range(len(self.jobs)):
            ids.append(self.jobs[i].id)
        return ids

    fn clear(inout self):
        """Remove all jobs."""
        self.jobs = List[Job]()

    fn next_due_time(self) -> Int64:
        """
        Get timestamp of next job that will be due.

        Returns:
            Unix timestamp of next scheduled job, or -1 if no jobs.
        """
        if len(self.jobs) == 0:
            return -1

        var earliest: Int64 = -1

        for i in range(len(self.jobs)):
            if self.jobs[i].enabled:
                var next_run = self.jobs[i].next_run_timestamp
                if earliest < 0 or next_run < earliest:
                    earliest = next_run

        return earliest

    fn time_until_next_due(self) -> Int64:
        """
        Get seconds until next job is due.

        Returns:
            Seconds until next due job, or -1 if no jobs.
        """
        var next_due = self.next_due_time()
        if next_due < 0:
            return -1

        var diff = next_due - self.current_time
        if diff < 0:
            return 0
        return diff

    fn _update_all_schedules(inout self):
        """Update next run time for all jobs."""
        for i in range(len(self.jobs)):
            if self.jobs[i].enabled and self.jobs[i].status != JobStatus.RUNNING:
                self.jobs[i].set_current_time(self.current_time)

    fn __str__(self) -> String:
        """Convert to string representation."""
        return (
            "Scheduler(jobs=" + str(len(self.jobs)) +
            ", current_time=" + str(self.current_time) +
            ", due=" + str(self.get_due_job_count()) + ")"
        )


# =============================================================================
# Scheduler Statistics
# =============================================================================

@value
struct SchedulerStats(Stringable):
    """Statistics about scheduler state."""
    var total_jobs: Int
    var enabled_jobs: Int
    var pending_jobs: Int
    var running_jobs: Int
    var due_jobs: Int
    var total_runs: Int
    var total_failures: Int

    fn __init__(out self):
        self.total_jobs = 0
        self.enabled_jobs = 0
        self.pending_jobs = 0
        self.running_jobs = 0
        self.due_jobs = 0
        self.total_runs = 0
        self.total_failures = 0

    fn success_rate(self) -> Float64:
        """Calculate overall success rate."""
        if self.total_runs == 0:
            return 1.0
        return Float64(self.total_runs - self.total_failures) / Float64(self.total_runs)

    fn __str__(self) -> String:
        return (
            "SchedulerStats(total=" + str(self.total_jobs) +
            ", enabled=" + str(self.enabled_jobs) +
            ", pending=" + str(self.pending_jobs) +
            ", running=" + str(self.running_jobs) +
            ", due=" + str(self.due_jobs) +
            ", runs=" + str(self.total_runs) +
            ", failures=" + str(self.total_failures) +
            ", success_rate=" + str(self.success_rate()) + ")"
        )


fn get_scheduler_stats(scheduler: Scheduler) -> SchedulerStats:
    """
    Get statistics from a scheduler.

    Args:
        scheduler: The scheduler to analyze.

    Returns:
        SchedulerStats with current state.
    """
    var stats = SchedulerStats()
    stats.total_jobs = scheduler.job_count()
    stats.enabled_jobs = scheduler.enabled_job_count()
    stats.pending_jobs = scheduler.pending_job_count()
    stats.running_jobs = scheduler.running_job_count()
    stats.due_jobs = scheduler.get_due_job_count()

    for i in range(len(scheduler.jobs)):
        stats.total_runs += scheduler.jobs[i].run_count
        stats.total_failures += scheduler.jobs[i].fail_count

    return stats
