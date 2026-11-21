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
    // F1 (all ingest_ts, most ts) at fixed positions; a 20-byte value ending in `Z` is never all digits, so
    // this is the same answer as the general path below
    if s.len() == 20 && s[19] == b'Z' {
        return parse_f1(s.try_into().unwrap());
    }
    parse_ts_general(s)
}

/// `YYYY-MM-DDTHH:MM:SSZ` with every check of the general path, at fixed positions.
#[inline(always)]
fn parse_f1(s: &[u8; 20]) -> Option<i64> {
    let d = |i: usize| s[i].wrapping_sub(b'0');
    let mut bad = 0u8;
    for i in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        bad |= (d(i) > 9) as u8;
    }
    if bad != 0 || s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let two = |i: usize| (d(i) as i64) * 10 + d(i + 1) as i64;
    let (y, mo, dd) = (two(0) * 100 + two(2), two(5), two(8));
    let (h, mi, se) = (two(11), two(14), two(17));
    if y < 1 || !(1..=12).contains(&mo) || dd < 1 || dd > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some((days_from_civil(y, mo, dd) * 86400 + h * 3600 + mi * 60 + se) * 1000)
}

/// Every form, by the rules (the reference path; `parse_ts` takes F1 aside first).
#[inline]
pub fn parse_ts_general(s: &[u8]) -> Option<i64> {
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

    /// The F1 fast path against the general path: every position mutated to digits, separators and others.
    #[test]
    fn f1_fast_equals_general() {
        let bases: [&[u8; 20]; 4] = [b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"0001-01-01T00:00:00Z", b"9999-12-31T23:59:59Z"];
        let subs = b"0123456789-T:Z +/a\x00\xff";
        for base in bases {
            assert_eq!(parse_ts(base), parse_ts_general(base));
            for i in 0..20 {
                for &c in subs {
                    let mut v = *base;
                    v[i] = c;
                    assert_eq!(parse_ts(&v), parse_ts_general(&v), "{:?}", String::from_utf8_lossy(&v));
                    for j in 0..20 {
                        for &c2 in b"019-:" {
                            let mut w = v;
                            w[j] = c2;
                            assert_eq!(parse_ts(&w), parse_ts_general(&w), "{:?}", String::from_utf8_lossy(&w));
                        }
                    }
                }
            }
        }
        // every day of several years, including leap rules
        for y in [1900i64, 2000, 2024, 2025, 2026, 2027, 2028, 2100] {
            for mo in 1..=12 {
                for dd in 0..=32 {
                    let t = format!("{y:04}-{mo:02}-{dd:02}T12:34:56Z");
                    assert_eq!(parse_ts(t.as_bytes()), parse_ts_general(t.as_bytes()), "{t}");
                }
            }
        }
    }
}
