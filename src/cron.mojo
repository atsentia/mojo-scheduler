"""
Cron Expression Parser

Parses cron expressions with support for:
- Minute (0-59)
- Hour (0-23)
- Day of month (1-31)
- Month (1-12)
- Day of week (0-6, 0=Sunday)

Supports:
- Wildcards: *
- Ranges: 1-5
- Lists: 1,3,5
- Steps: */5, 1-10/2

Example:
    var cron = CronExpr.parse("0 0 * * *")  # Daily at midnight
    var matches = cron.matches(0, 0, 15, 6, 3)  # Check if matches
"""


# =============================================================================
# Cron Field
# =============================================================================

struct CronField(Stringable):
    """
    A single cron field (minute, hour, day, month, or weekday).

    Stores which values are allowed as a bitmask.
    """
    var allowed: List[Bool]  # Which values are allowed
    var min_val: Int
    var max_val: Int

    fn __init__(out self, min_val: Int, max_val: Int):
        """Create field with range, initially all values allowed."""
        self.min_val = min_val
        self.max_val = max_val
        self.allowed = List[Bool]()
        # Initialize all as False
        for _ in range(max_val - min_val + 1):
            self.allowed.append(False)

    fn set_all(inout self):
        """Set all values as allowed (wildcard)."""
        for i in range(len(self.allowed)):
            self.allowed[i] = True

    fn set_value(inout self, value: Int):
        """Set a specific value as allowed."""
        if value >= self.min_val and value <= self.max_val:
            self.allowed[value - self.min_val] = True

    fn set_range(inout self, start: Int, end: Int, step: Int = 1):
        """Set a range of values as allowed with optional step."""
        var actual_step = step if step > 0 else 1
        var i = start
        while i <= end:
            self.set_value(i)
            i += actual_step

    fn matches(self, value: Int) -> Bool:
        """Check if value matches this field."""
        if value < self.min_val or value > self.max_val:
            return False
        return self.allowed[value - self.min_val]

    fn next_match(self, from_value: Int) -> Int:
        """
        Find next matching value >= from_value.
        Returns -1 if no match found (should wrap to next period).
        """
        var start = from_value
        if start < self.min_val:
            start = self.min_val

        for v in range(start, self.max_val + 1):
            if self.matches(v):
                return v

        return -1  # No match, need to increment parent field

    fn first_match(self) -> Int:
        """Get first matching value."""
        for v in range(self.min_val, self.max_val + 1):
            if self.matches(v):
                return v
        return self.min_val  # Fallback

    fn __str__(self) -> String:
        """Convert to string representation."""
        var parts = List[String]()
        var in_range = False
        var range_start = 0

        for i in range(len(self.allowed)):
            var val = i + self.min_val
            if self.allowed[i]:
                if not in_range:
                    in_range = True
                    range_start = val
            else:
                if in_range:
                    if val - 1 == range_start:
                        parts.append(String(range_start))
                    else:
                        parts.append(String(range_start) + "-" + String(val - 1))
                    in_range = False

        # Handle trailing range
        if in_range:
            var end_val = self.max_val
            if end_val == range_start:
                parts.append(String(range_start))
            else:
                parts.append(String(range_start) + "-" + String(end_val))

        if len(parts) == 0:
            return "*"

        # Check if all values
        var all_set = True
        for i in range(len(self.allowed)):
            if not self.allowed[i]:
                all_set = False
                break
        if all_set:
            return "*"

        # Join parts
        var result = String("")
        for i in range(len(parts)):
            if i > 0:
                result += ","
            result += parts[i]
        return result


# =============================================================================
# Cron Expression
# =============================================================================

@value
struct CronExpr(Stringable):
    """
    A parsed cron expression.

    Standard 5-field format: minute hour day month weekday

    Example:
        var cron = CronExpr.parse("0 0 * * *")  # Midnight daily
        var cron2 = CronExpr.parse("*/15 * * * *")  # Every 15 minutes
    """
    var minute: CronField
    var hour: CronField
    var day: CronField
    var month: CronField
    var weekday: CronField
    var expression: String
    var is_valid: Bool
    var error_msg: String

    fn __init__(out self):
        """Create empty expression."""
        self.minute = CronField(0, 59)
        self.hour = CronField(0, 23)
        self.day = CronField(1, 31)
        self.month = CronField(1, 12)
        self.weekday = CronField(0, 6)
        self.expression = ""
        self.is_valid = False
        self.error_msg = ""

    @staticmethod
    fn parse(expression: String) -> CronExpr:
        """
        Parse a cron expression string.

        Format: "minute hour day month weekday"

        Examples:
            "0 0 * * *"     - Daily at midnight
            "*/5 * * * *"   - Every 5 minutes
            "0 9-17 * * 1-5" - 9am-5pm on weekdays
        """
        var result = CronExpr()
        result.expression = expression

        # Split by whitespace
        var parts = CronExpr._split_whitespace(expression)

        if len(parts) != 5:
            result.error_msg = "Expected 5 fields, got " + String(len(parts))
            return result

        # Parse each field
        if not CronExpr._parse_field(parts[0], result.minute):
            result.error_msg = "Invalid minute field: " + parts[0]
            return result

        if not CronExpr._parse_field(parts[1], result.hour):
            result.error_msg = "Invalid hour field: " + parts[1]
            return result

        if not CronExpr._parse_field(parts[2], result.day):
            result.error_msg = "Invalid day field: " + parts[2]
            return result

        if not CronExpr._parse_field(parts[3], result.month):
            result.error_msg = "Invalid month field: " + parts[3]
            return result

        if not CronExpr._parse_field(parts[4], result.weekday):
            result.error_msg = "Invalid weekday field: " + parts[4]
            return result

        result.is_valid = True
        return result

    @staticmethod
    fn _split_whitespace(s: String) -> List[String]:
        """Split string by whitespace."""
        var parts = List[String]()
        var current = String("")

        for i in range(len(s)):
            var c = s[i]
            if c == ' ' or c == '\t':
                if len(current) > 0:
                    parts.append(current)
                    current = String("")
            else:
                current += c

        if len(current) > 0:
            parts.append(current)

        return parts

    @staticmethod
    fn _parse_field(field_str: String, inout field: CronField) -> Bool:
        """Parse a single cron field."""
        # Handle wildcard
        if field_str == "*":
            field.set_all()
            return True

        # Split by comma for lists
        var list_parts = CronExpr._split_comma(field_str)

        for i in range(len(list_parts)):
            if not CronExpr._parse_list_item(list_parts[i], field):
                return False

        return True

    @staticmethod
    fn _split_comma(s: String) -> List[String]:
        """Split string by comma."""
        var parts = List[String]()
        var current = String("")

        for i in range(len(s)):
            var c = s[i]
            if c == ',':
                if len(current) > 0:
                    parts.append(current)
                    current = String("")
            else:
                current += c

        if len(current) > 0:
            parts.append(current)

        return parts

    @staticmethod
    fn _parse_list_item(item: String, inout field: CronField) -> Bool:
        """Parse a single list item (value, range, or step)."""
        # Check for step
        var step_pos = CronExpr._find_char(item, '/')
        var step = 1
        var base_item = item

        if step_pos >= 0:
            base_item = CronExpr._substring(item, 0, step_pos)
            var step_str = CronExpr._substring(item, step_pos + 1, len(item))
            step = CronExpr._parse_int(step_str)
            if step <= 0:
                return False

        # Check for range
        var range_pos = CronExpr._find_char(base_item, '-')

        if base_item == "*":
            # */5 style
            field.set_range(field.min_val, field.max_val, step)
            return True
        elif range_pos >= 0:
            # Range like 1-5
            var start_str = CronExpr._substring(base_item, 0, range_pos)
            var end_str = CronExpr._substring(base_item, range_pos + 1, len(base_item))
            var start = CronExpr._parse_int(start_str)
            var end = CronExpr._parse_int(end_str)

            if start < 0 or end < 0 or start > end:
                return False

            field.set_range(start, end, step)
            return True
        else:
            # Single value
            var value = CronExpr._parse_int(base_item)
            if value < 0:
                return False

            if step > 1:
                # Single value with step (unusual but valid)
                field.set_range(value, field.max_val, step)
            else:
                field.set_value(value)
            return True

    @staticmethod
    fn _find_char(s: String, c: String) -> Int:
        """Find first occurrence of character."""
        for i in range(len(s)):
            if s[i] == c:
                return i
        return -1

    @staticmethod
    fn _substring(s: String, start: Int, end: Int) -> String:
        """Extract substring."""
        var result = String("")
        for i in range(start, end):
            if i < len(s):
                result += s[i]
        return result

    @staticmethod
    fn _parse_int(s: String) -> Int:
        """Parse integer from string. Returns -1 on error."""
        if len(s) == 0:
            return -1

        var result = 0
        for i in range(len(s)):
            var c = s[i]
            if c >= '0' and c <= '9':
                var digit_val = ord(c) - ord('0')
                result = result * 10 + digit_val
            else:
                return -1

        return result

    fn matches(self, minute: Int, hour: Int, day: Int, month: Int, weekday: Int) -> Bool:
        """
        Check if the given time matches this cron expression.

        Args:
            minute: Minute (0-59)
            hour: Hour (0-23)
            day: Day of month (1-31)
            month: Month (1-12)
            weekday: Day of week (0-6, 0=Sunday)

        Returns:
            True if the time matches the cron expression.
        """
        if not self.is_valid:
            return False

        return (
            self.minute.matches(minute) and
            self.hour.matches(hour) and
            self.day.matches(day) and
            self.month.matches(month) and
            self.weekday.matches(weekday)
        )

    fn next_run_after(
        self,
        minute: Int,
        hour: Int,
        day: Int,
        month: Int,
        year: Int,
        weekday: Int,
    ) -> Tuple[Int, Int, Int, Int, Int]:
        """
        Calculate next run time after the given time.

        Returns (minute, hour, day, month, year) tuple.
        Note: This is a simplified calculation that doesn't handle
        all edge cases perfectly but works for common schedules.
        """
        if not self.is_valid:
            return (0, 0, 1, 1, year)

        var m = minute + 1  # Start from next minute
        var h = hour
        var d = day
        var mo = month
        var y = year
        var wd = weekday

        # Normalize if minute overflowed
        if m > 59:
            m = 0
            h += 1

        # Maximum iterations to prevent infinite loop
        var max_iter = 366 * 24 * 60  # One year of minutes
        var iter = 0

        while iter < max_iter:
            iter += 1

            # Check month
            var next_mo = self.month.next_match(mo)
            if next_mo < 0 or next_mo > mo:
                # Move to next year
                if next_mo < 0:
                    y += 1
                    mo = self.month.first_match()
                else:
                    mo = next_mo
                d = self.day.first_match()
                h = self.hour.first_match()
                m = self.minute.first_match()
                wd = self._calculate_weekday(y, mo, d)
                continue

            # Check day
            var next_d = self.day.next_match(d)
            var days_in_month = self._days_in_month(y, mo)
            if next_d < 0 or next_d > days_in_month:
                # Move to next month
                mo += 1
                if mo > 12:
                    mo = 1
                    y += 1
                d = self.day.first_match()
                h = self.hour.first_match()
                m = self.minute.first_match()
                wd = self._calculate_weekday(y, mo, d)
                continue
            elif next_d > d:
                d = next_d
                h = self.hour.first_match()
                m = self.minute.first_match()
                wd = self._calculate_weekday(y, mo, d)

            # Check weekday
            wd = self._calculate_weekday(y, mo, d)
            if not self.weekday.matches(wd):
                d += 1
                if d > days_in_month:
                    mo += 1
                    if mo > 12:
                        mo = 1
                        y += 1
                    d = 1
                h = self.hour.first_match()
                m = self.minute.first_match()
                continue

            # Check hour
            var next_h = self.hour.next_match(h)
            if next_h < 0:
                # Move to next day
                d += 1
                if d > days_in_month:
                    mo += 1
                    if mo > 12:
                        mo = 1
                        y += 1
                    d = 1
                h = self.hour.first_match()
                m = self.minute.first_match()
                continue
            elif next_h > h:
                h = next_h
                m = self.minute.first_match()

            # Check minute
            var next_m = self.minute.next_match(m)
            if next_m < 0:
                # Move to next hour
                h += 1
                if h > 23:
                    h = 0
                    d += 1
                    if d > days_in_month:
                        mo += 1
                        if mo > 12:
                            mo = 1
                            y += 1
                        d = 1
                m = self.minute.first_match()
                continue
            else:
                m = next_m

            # Found matching time
            return (m, h, d, mo, y)

        # Fallback if no match found
        return (0, 0, 1, 1, y + 1)

    fn _days_in_month(self, year: Int, month: Int) -> Int:
        """Get days in a month."""
        if month == 2:
            # Leap year check
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                return 29
            return 28
        elif month in (4, 6, 9, 11):
            return 30
        else:
            return 31

    fn _calculate_weekday(self, year: Int, month: Int, day: Int) -> Int:
        """
        Calculate day of week (0=Sunday, 6=Saturday).
        Uses Zeller's congruence.
        """
        var m = month
        var y = year

        if m < 3:
            m += 12
            y -= 1

        var k = y % 100
        var j = y // 100

        var h = (day + (13 * (m + 1)) // 5 + k + k // 4 + j // 4 - 2 * j) % 7

        # Convert to 0=Sunday format
        return (h + 6) % 7

    fn __str__(self) -> String:
        """Convert to string representation."""
        if not self.is_valid:
            return "CronExpr(invalid: " + self.error_msg + ")"
        return "CronExpr(" + self.expression + ")"


# =============================================================================
# Helper Functions
# =============================================================================

fn parse_cron(expression: String) -> CronExpr:
    """
    Parse a cron expression.

    Convenience function that wraps CronExpr.parse().

    Args:
        expression: Cron expression string (5 fields).

    Returns:
        Parsed CronExpr.
    """
    return CronExpr.parse(expression)


fn cron_matches(
    expression: String,
    minute: Int,
    hour: Int,
    day: Int,
    month: Int,
    weekday: Int,
) -> Bool:
    """
    Check if a time matches a cron expression.

    Args:
        expression: Cron expression string.
        minute: Minute (0-59).
        hour: Hour (0-23).
        day: Day of month (1-31).
        month: Month (1-12).
        weekday: Day of week (0-6, 0=Sunday).

    Returns:
        True if the time matches.
    """
    var cron = CronExpr.parse(expression)
    return cron.matches(minute, hour, day, month, weekday)
