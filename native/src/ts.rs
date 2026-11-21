//! Strict timestamps (rules.md "Timestamps"), byte for byte the same rules as `pipeline/ts_macros.sql`.
//!
//! F1 `YYYY-MM-DDTHH:MM:SSZ` (20 bytes), F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` / `-HH:MM` (25 bytes; offset hour
//! 0-23, minute 0-59), F3 ASCII digits only (epoch ms, leading zeros allowed). Calendar checked by hand.
//! F3 values with more than 18 significant digits are `None` (outside every window), like the oracle.

#[inline(always)]
fn num(s: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as i64;
    }
    Some(v)
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic-Gregorian date with y >= 1 (Hinnant's days_from_civil).
#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy / 400; // yy >= 0 because y >= 1
    let yoe = yy - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Epoch milliseconds (UTC) of a timestamp, or `None` when it is a bad timestamp.
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(u8::is_ascii_digit) {
        let first = s.iter().position(|&b| b != b'0').unwrap_or(s.len());
        let t = &s[first..];
        if t.len() > 18 {
            return None;
        }
        return Some(if t.is_empty() { 0 } else { num(t)? });
    }
    let offset_min = match s.len() {
        20 => {
            if s[19] != b'Z' {
                return None;
            }
            0
        }
        25 => {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let (oh, om) = (num(&s[20..22])?, num(&s[23..25])?);
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 60 + om)
        }
        _ => return None,
    };
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let (y, mo, d) = (num(&s[0..4])?, num(&s[5..7])?, num(&s[8..10])?);
    let (h, mi, se) = (num(&s[11..13])?, num(&s[14..16])?, num(&s[17..19])?);
    if y < 1 || !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 - offset_min * 60_000)
}

/// A purchase price: ASCII digits only, value 1..=10,000,000 (leading zeros allowed), else `None`.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let first = s.iter().position(|&b| b != b'0').unwrap_or(s.len());
    let t = &s[first..];
    if t.is_empty() || t.len() > 9 {
        return None;
    }
    let v = num(t)?;
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn examples() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-02-29T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2028-02-29T00:00:00Z").is_some(), true);
        assert_eq!(parse_ts(b"2026-10-03T10:00:00+0200"), None);
        assert_eq!(parse_ts(b"1791071890000\r"), None);
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000001"), None);
    }
}
