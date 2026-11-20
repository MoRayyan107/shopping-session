-- Strict timestamp parsing for rules.md "Timestamps".
--
--   ts_parse(s) -> BIGINT epoch milliseconds (UTC), or NULL when s is a bad timestamp.
--
-- Forms accepted (ASCII digits only, nothing else):
--   F1  YYYY-MM-DDTHH:MM:SSZ
--   F2  YYYY-MM-DDTHH:MM:SS+HH:MM / -HH:MM   (offset hour 00-23, minute 00-59, colon required)
--   F3  digits only, epoch milliseconds, leading zeros allowed
--
-- A real calendar check is done by hand (no lenient date casts): month 1-12, day within the month
-- (leap years by the Gregorian rule), hour <= 23, minute <= 59, second <= 59.
--
-- F3 values with more than 18 significant digits (> 1e18 ms, i.e. beyond any window and beyond
-- what fits comfortably in a BIGINT) return NULL. rules.md calls these "valid but outside the
-- window"; every caller drops them either way, because they fail the window test, so NULL is safe.
-- A parsed value that is merely very small or very large (e.g. 86400000, year 0001) is returned as is
-- and is rejected later by the window check.

CREATE OR REPLACE MACRO ts_pat() AS
  '^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:Z|([+-])([0-9]{2}):([0-9]{2}))$';

CREATE OR REPLACE MACRO ts_g(s, i) AS regexp_extract(s, ts_pat(), i);
CREATE OR REPLACE MACRO ts_n(s, i) AS TRY_CAST(ts_g(s, i) AS BIGINT);

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

-- Is s a well-formed, real F1/F2 value?
CREATE OR REPLACE MACRO ts_f12_ok(s) AS (
  regexp_full_match(s, ts_pat())
  AND ts_n(s, 1) >= 1
  AND ts_n(s, 2) BETWEEN 1 AND 12
  AND ts_n(s, 3) BETWEEN 1 AND ts_dim(ts_n(s, 1), ts_n(s, 2))
  AND ts_n(s, 4) <= 23 AND ts_n(s, 5) <= 59 AND ts_n(s, 6) <= 59
  AND (ts_g(s, 7) = '' OR (ts_n(s, 8) <= 23 AND ts_n(s, 9) <= 59))
);

-- Epoch ms of an F1/F2 value; only meaningful when ts_f12_ok(s).
CREATE OR REPLACE MACRO ts_f12_ms(s) AS (
  (ts_days(ts_n(s, 1), ts_n(s, 2), ts_n(s, 3)) * 86400 + ts_n(s, 4) * 3600 + ts_n(s, 5) * 60 + ts_n(s, 6)) * 1000
  - CASE ts_g(s, 7)
      WHEN '+' THEN  (ts_n(s, 8) * 60 + ts_n(s, 9)) * 60000
      WHEN '-' THEN -(ts_n(s, 8) * 60 + ts_n(s, 9)) * 60000
      ELSE 0
    END);

-- F3: digits only, leading zeros stripped; the all-zero string is 0.
CREATE OR REPLACE MACRO ts_f3_digits(s) AS (CASE WHEN regexp_replace(s, '^0+', '') = '' THEN '0' ELSE regexp_replace(s, '^0+', '') END);

CREATE OR REPLACE MACRO ts_parse(s) AS (
  CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+')
      THEN (CASE WHEN length(ts_f3_digits(s)) <= 18 THEN TRY_CAST(ts_f3_digits(s) AS BIGINT) ELSE NULL END)
    WHEN ts_f12_ok(s) THEN ts_f12_ms(s)
    ELSE NULL
  END);
