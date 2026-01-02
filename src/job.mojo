"""
Job Definition

Represents a scheduled job with:
- Unique ID and name
- Cron schedule expression
- Next and last run timestamps
- Status tracking

Example:
    var job = Job("backup", "Daily Backup", "0 0 * * *")
    job.set_current_time(1703980800)  # 2023-12-31 00:00:00
    var next = job.next_run_timestamp
"""

from .cron import CronExpr


# =============================================================================
# Job Status
# =============================================================================

@value
struct JobStatus(Stringable, EqualityComparable):
    """Status of a scheduled job."""
    var value: Int

    alias PENDING = JobStatus(0)
    """Job is waiting to run."""

    alias RUNNING = JobStatus(1)
    """Job is currently running."""

    alias COMPLETED = JobStatus(2)
    """Job completed successfully."""

    alias FAILED = JobStatus(3)
    """Job failed to complete."""

    alias DISABLED = JobStatus(4)
    """Job is disabled and won't run."""

    fn __init__(out self, value: Int):
        self.value = value

    fn __eq__(self, other: JobStatus) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: JobStatus) -> Bool:
        return self.value != other.value

    fn __str__(self) -> String:
        if self.value == 0:
            return "PENDING"
        elif self.value == 1:
            return "RUNNING"
        elif self.value == 2:
            return "COMPLETED"
        elif self.value == 3:
            return "FAILED"
        elif self.value == 4:
            return "DISABLED"
        else:
            return "UNKNOWN"


# =============================================================================
# Job
# =============================================================================

struct Job(Stringable):
    """
    A scheduled job with cron-based scheduling.

    Example:
        var job = Job("backup", "Daily Backup", "0 0 * * *")
        print("Next run:", job.next_run_timestamp)
    """
    var id: String
    """Unique job identifier."""

    var name: String
    """Human-readable job name."""

    var schedule: CronExpr
    """Cron schedule expression."""

    var next_run_timestamp: Int64
    """Unix timestamp of next scheduled run."""

    var last_run_timestamp: Int64
    """Unix timestamp of last run (0 if never run)."""

    var status: JobStatus
    """Current job status."""

    var enabled: Bool
    """Whether the job is enabled."""

    var run_count: Int
    """Number of times job has run."""

    var fail_count: Int
    """Number of times job has failed."""

    var metadata: String
    """Optional metadata (JSON or other format)."""

    fn __init__(out self, id: String, name: String, schedule: String):
        """
        Create a new job.

        Args:
            id: Unique job identifier.
            name: Human-readable job name.
            schedule: Cron expression string.
        """
        self.id = id
        self.name = name
        self.schedule = CronExpr.parse(schedule)
        self.next_run_timestamp = 0
        self.last_run_timestamp = 0
        self.status = JobStatus.PENDING
        self.enabled = True
        self.run_count = 0
        self.fail_count = 0
        self.metadata = ""

    fn __init__(
        out self,
        id: String,
        name: String,
        schedule: String,
        metadata: String,
    ):
        """Create job with metadata."""
        self.id = id
        self.name = name
        self.schedule = CronExpr.parse(schedule)
        self.next_run_timestamp = 0
        self.last_run_timestamp = 0
        self.status = JobStatus.PENDING
        self.enabled = True
        self.run_count = 0
        self.fail_count = 0
        self.metadata = metadata

    fn is_valid(self) -> Bool:
        """Check if job has valid schedule."""
        return self.schedule.is_valid

    fn is_due(self, current_timestamp: Int64) -> Bool:
        """
        Check if job is due to run.

        Args:
            current_timestamp: Current Unix timestamp.

        Returns:
            True if job should run now.
        """
        if not self.enabled:
            return False
        if self.status == JobStatus.DISABLED:
            return False
        if self.status == JobStatus.RUNNING:
            return False

        return self.next_run_timestamp <= current_timestamp

    fn calculate_next_run(
        inout self,
        minute: Int,
        hour: Int,
        day: Int,
        month: Int,
        year: Int,
        weekday: Int,
    ):
        """
        Calculate and set the next run time from given time components.

        Args:
            minute: Current minute (0-59).
            hour: Current hour (0-23).
            day: Current day (1-31).
            month: Current month (1-12).
            year: Current year.
            weekday: Current weekday (0-6, 0=Sunday).
        """
        var result = self.schedule.next_run_after(
            minute, hour, day, month, year, weekday
        )
        var m = result[0]
        var h = result[1]
        var d = result[2]
        var mo = result[3]
        var y = result[4]

        # Convert to timestamp (simplified calculation)
        self.next_run_timestamp = self._to_timestamp(y, mo, d, h, m, 0)

    fn set_current_time(inout self, timestamp: Int64):
        """
        Set current time and calculate next run.

        Args:
            timestamp: Current Unix timestamp.
        """
        # Convert timestamp to components (simplified)
        var components = self._from_timestamp(timestamp)
        var year = components[0]
        var month = components[1]
        var day = components[2]
        var hour = components[3]
        var minute = components[4]
        var weekday = self._calculate_weekday(year, month, day)

        self.calculate_next_run(minute, hour, day, month, year, weekday)

    fn mark_started(inout self, timestamp: Int64):
        """Mark job as started."""
        self.status = JobStatus.RUNNING
        self.last_run_timestamp = timestamp

    fn mark_completed(inout self, timestamp: Int64):
        """Mark job as completed and calculate next run."""
        self.status = JobStatus.COMPLETED
        self.run_count += 1
        self.set_current_time(timestamp)

    fn mark_failed(inout self, timestamp: Int64):
        """Mark job as failed and calculate next run."""
        self.status = JobStatus.FAILED
        self.fail_count += 1
        self.run_count += 1
        self.set_current_time(timestamp)

    fn disable(inout self):
        """Disable the job."""
        self.enabled = False
        self.status = JobStatus.DISABLED

    fn enable(inout self, timestamp: Int64):
        """Enable the job and recalculate next run."""
        self.enabled = True
        self.status = JobStatus.PENDING
        self.set_current_time(timestamp)

    fn reset(inout self, timestamp: Int64):
        """Reset job status and recalculate next run."""
        self.status = JobStatus.PENDING
        self.set_current_time(timestamp)

    fn _to_timestamp(
        self,
        year: Int,
        month: Int,
        day: Int,
        hour: Int,
        minute: Int,
        second: Int,
    ) -> Int64:
        """Convert date components to Unix timestamp (UTC)."""
        # Days from year 1970
        var days = 0

        # Add days for complete years
        for y in range(1970, year):
            if self._is_leap_year(y):
                days += 366
            else:
                days += 365

        # Add days for complete months
        var days_in_months = List[Int](
            0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
        )
        for m in range(1, month):
            days += days_in_months[m]
            if m == 2 and self._is_leap_year(year):
                days += 1

        # Add remaining days
        days += day - 1

        # Convert to seconds
        var total_seconds = Int64(days) * 86400
        total_seconds += Int64(hour) * 3600
        total_seconds += Int64(minute) * 60
        total_seconds += Int64(second)

        return total_seconds

    fn _from_timestamp(self, timestamp: Int64) -> Tuple[Int, Int, Int, Int, Int, Int]:
        """
        Convert Unix timestamp to date components.
        Returns (year, month, day, hour, minute, second).
        """
        var remaining = timestamp
        var year = 1970

        # Find year
        while True:
            var days_in_year = 365
            if self._is_leap_year(year):
                days_in_year = 366
            var seconds_in_year = Int64(days_in_year) * 86400
            if remaining < seconds_in_year:
                break
            remaining -= seconds_in_year
            year += 1

        # Find month
        var days_in_months = List[Int](
            0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
        )
        if self._is_leap_year(year):
            days_in_months[2] = 29

        var month = 1
        while month <= 12:
            var seconds_in_month = Int64(days_in_months[month]) * 86400
            if remaining < seconds_in_month:
                break
            remaining -= seconds_in_month
            month += 1

        # Find day
        var day = int(remaining // 86400) + 1
        remaining = remaining % 86400

        # Find hour, minute, second
        var hour = int(remaining // 3600)
        remaining = remaining % 3600
        var minute = int(remaining // 60)
        var second = int(remaining % 60)

        return (year, month, day, hour, minute, second)

    fn _is_leap_year(self, year: Int) -> Bool:
        """Check if year is a leap year."""
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

    fn _calculate_weekday(self, year: Int, month: Int, day: Int) -> Int:
        """Calculate day of week (0=Sunday, 6=Saturday)."""
        var m = month
        var y = year

        if m < 3:
            m += 12
            y -= 1

        var k = y % 100
        var j = y // 100

        var h = (day + (13 * (m + 1)) // 5 + k + k // 4 + j // 4 - 2 * j) % 7

        return (h + 6) % 7

    fn __str__(self) -> String:
        """Convert to string representation."""
        return (
            "Job(id=" + self.id +
            ", name=" + self.name +
            ", schedule=" + self.schedule.expression +
            ", status=" + str(self.status) +
            ", enabled=" + str(self.enabled) +
            ", next_run=" + str(self.next_run_timestamp) +
            ", last_run=" + str(self.last_run_timestamp) +
            ", run_count=" + str(self.run_count) +
            ", fail_count=" + str(self.fail_count) + ")"
        )


# =============================================================================
# Job Result
# =============================================================================

@value
struct JobResult(Stringable):
    """Result of a job execution."""
    var job_id: String
    var success: Bool
    var start_time: Int64
    var end_time: Int64
    var error_message: String
    var output: String

    fn __init__(out self, job_id: String):
        """Create pending job result."""
        self.job_id = job_id
        self.success = False
        self.start_time = 0
        self.end_time = 0
        self.error_message = ""
        self.output = ""

    fn duration_ms(self) -> Int64:
        """Get execution duration in milliseconds."""
        return (self.end_time - self.start_time) * 1000

    fn __str__(self) -> String:
        return (
            "JobResult(job_id=" + self.job_id +
            ", success=" + str(self.success) +
            ", duration_ms=" + str(self.duration_ms()) + ")"
        )
