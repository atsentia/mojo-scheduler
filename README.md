# mojo-scheduler

Pure Mojo job scheduling with cron expression support.

## Features

- **Cron Expressions** - Standard cron syntax
- **Job Management** - Add, remove, track jobs
- **Status Tracking** - Pending, running, completed states
- **Flexible Scheduling** - Minute to monthly intervals

## Installation

```bash
pixi add mojo-scheduler
```

## Quick Start

### Basic Usage

```mojo
from mojo_scheduler import Scheduler, Job, CronExpr

# Create scheduler
var scheduler = Scheduler()

# Add jobs
scheduler.add_job("backup", "Daily Backup", "0 0 * * *")
scheduler.add_job("cleanup", "Hourly Cleanup", "0 * * * *")

# Set current time (Unix timestamp)
scheduler.set_time(1703980800)

# Check for due jobs
var due_jobs = scheduler.get_due_jobs()
for job_id in due_jobs:
    scheduler.mark_job_started(job_id)
    # Execute job...
    scheduler.mark_job_completed(job_id)
```

### Cron Expressions

```mojo
from mojo_scheduler import CronExpr

# Parse cron expression
var cron = CronExpr.parse("*/15 * * * *")

# Check if matches (minute, hour, day, month, weekday)
if cron.matches(15, 10, 25, 12, 3):
    print("Matches!")
```

## Cron Expression Format

```
minute hour day month weekday
  │      │    │    │     │
  │      │    │    │     └─ 0-6 (0=Sunday)
  │      │    │    └─────── 1-12
  │      │    └──────────── 1-31
  │      └───────────────── 0-23
  └──────────────────────── 0-59
```

### Special Characters

| Char | Meaning | Example |
|------|---------|---------|
| `*` | Any value | `* * * * *` |
| `1-5` | Range | `0 9-17 * * *` |
| `1,3,5` | List | `0 0 1,15 * *` |
| `*/5` | Step | `*/5 * * * *` |

### Common Patterns

| Pattern | Description |
|---------|-------------|
| `0 0 * * *` | Daily at midnight |
| `*/5 * * * *` | Every 5 minutes |
| `0 9-17 * * 1-5` | 9am-5pm weekdays |
| `0 0 1 * *` | First of each month |
| `0 0 * * 0` | Every Sunday |

## Testing

```bash
mojo run tests/test_scheduler.mojo
```

## License

MIT

## Part of mojo-contrib

This library is part of [mojo-contrib](https://github.com/atsentia/mojo-contrib), a collection of pure Mojo libraries.
