-- Strict timestamp parsing for rules.md "Timestamps".
--
--   ts_parse(s) -> BIGINT epoch milliseconds (UTC), or NULL when s is a bad timestamp.
--
-- Forms accepted (ASCII digits only, nothing else):
--   F1  YYYY-MM-DDTHH:MM:SSZ                   (20 characters)
--   F2  YYYY-MM-DDTHH:MM:SS+HH:MM / -HH:MM     (25 characters; offset hour 00-23, minute 00-59)
--   F3  digits only, epoch milliseconds, leading zeros allowed
--
-- No regular expressions: translate() maps every ASCII digit to '0', and the result must equal a fixed
-- template, so a non-ASCII digit, a missing or extra character or a wrong separator fails the shape test.
-- Fields are then read at fixed positions. The calendar check is done by hand (no lenient date casts):
-- month 1-12, day within the month (Gregorian leap years), hour <= 23, minute <= 59, second <= 59, year >= 1.
--
-- F3 values with more than 18 significant digits return NULL. rules.md calls these "valid but outside the
-- window"; every caller drops them either way, because they fail the window test, so NULL is safe.

CREATE OR REPLACE MACRO ts_leap(y) AS (y % 4 = 0 AND (y % 100 <> 0 OR y % 400 = 0));

CREATE OR REPLACE MACRO ts_dim(y, m) AS (
  CASE m
    WHEN 2 THEN (CASE WHEN ts_leap(y) THEN 29 ELSE 28 END)
    WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30
    ELSE 31
  END);

-- Days since 1970-01-01 for a proleptic-Gregorian date (Hinnant's days_from_civil); y >= 1.
-- yy is the year with January and February counted in the previous year.
CREATE OR REPLACE MACRO ts_days_yy(yy, m, d) AS (
  (yy // 400) * 146097
  + (yy - (yy // 400) * 400) * 365 + (yy - (yy // 400) * 400) // 4 - (yy - (yy // 400) * 400) // 100
  + (153 * (m + CASE WHEN m > 2 THEN -3 ELSE 9 END) + 2) // 5 + d - 1
  - 719468);
CREATE OR REPLACE MACRO ts_days(y, m, d) AS ts_days_yy(y - CASE WHEN m <= 2 THEN 1 ELSE 0 END, m, d);

-- Digit-shape of a string: every ASCII digit becomes '0'.
CREATE OR REPLACE MACRO ts_shape(s) AS translate(s, '0123456789', '0000000000');

-- A fixed-position number; the shape test has already proved these characters are ASCII digits.
CREATE OR REPLACE MACRO ts_num(s, pos, len) AS TRY_CAST(substr(s, pos, len) AS BIGINT);

-- y, mo, d, h, mi, se: date and time fields; offset_min: signed offset in minutes (0 for Z).
CREATE OR REPLACE MACRO ts_f12_ms(y, mo, d, h, mi, se, oh, om, sign) AS (
  CASE WHEN y >= 1 AND mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND ts_dim(y, mo)
            AND h <= 23 AND mi <= 59 AND se <= 59 AND oh <= 23 AND om <= 59
       THEN (ts_days(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 - sign * (oh * 60 + om) * 60000
  END);

CREATE OR REPLACE MACRO ts_f12(s, has_offset) AS ts_f12_ms(
  ts_num(s, 1, 4), ts_num(s, 6, 2), ts_num(s, 9, 2), ts_num(s, 12, 2), ts_num(s, 15, 2), ts_num(s, 18, 2),
  CASE WHEN has_offset THEN ts_num(s, 21, 2) ELSE 0 END,
  CASE WHEN has_offset THEN ts_num(s, 24, 2) ELSE 0 END,
  CASE WHEN NOT has_offset THEN 0 WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END);

-- F3: leading zeros stripped; the all-zero string is 0.
CREATE OR REPLACE MACRO ts_f3(s) AS (
  CASE WHEN ltrim(s, '0') = '' THEN 0
       WHEN length(ltrim(s, '0')) <= 18 THEN TRY_CAST(ltrim(s, '0') AS BIGINT)
  END);

CREATE OR REPLACE MACRO ts_parse(s) AS (
  CASE
    WHEN s IS NULL OR s = '' THEN NULL
    WHEN translate(s, '0123456789', '') = '' THEN ts_f3(s)
    WHEN ts_shape(s) = '0000-00-00T00:00:00Z' THEN ts_f12(s, false)
    WHEN ts_shape(s) = '0000-00-00T00:00:00+00:00' OR ts_shape(s) = '0000-00-00T00:00:00-00:00' THEN ts_f12(s, true)
    ELSE NULL
  END);
