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
    if s.len() == 25 && (s[19] == b'+' || s[19] == b'-') {
        return parse_f2(s.try_into().unwrap());
    }
    if s.len() == 13 {
        return parse_f3_13(s.try_into().unwrap());
    }
    parse_ts_general(s)
}

/// The value of eight ASCII digits (byte 0 most significant), given as the word XOR `0` bytes (each byte
/// 0..=9): pairs, then fours, then the eight.
#[inline(always)]
fn eight_digits(d: u64) -> u64 {
    // each step: lane j becomes 100 * lane j + lane j + 1 (or 10_000 *), every lane stays below its width
    let d = pairs(d) & 0x00FF_00FF_00FF_00FF;
    let d = (d.wrapping_mul(1 + (100 << 16)) >> 16) & 0x0000_FFFF_0000_FFFF;
    d.wrapping_mul(1 + (10_000 << 32)) >> 32
}

/// A 13-byte value: F3 when all digits (13 digits never exceed 18 significant ones), else no form of 13 bytes
/// exists, so `None`, exactly as `parse_ts_general`.
#[inline(always)]
fn parse_f3_13(s: &[u8; 13]) -> Option<i64> {
    const ZEROS: u64 = 0x3030_3030_3030_3030;
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap()) ^ ZEROS;
    let b = u64::from_le_bytes(s[5..13].try_into().unwrap()) ^ ZEROS;
    if bad_bytes(a, !0) | bad_bytes(b, !0) != 0 {
        return None;
    }
    // digits 0-7, then digits 5-12 of which the last five are digits 8-12
    Some((eight_digits(a) * 100_000 + eight_digits(b) % 100_000) as i64)
}

/// F2 (`YYYY-MM-DDTHH:MM:SS+HH:MM` or `-HH:MM`) at fixed positions: the F1 checks on the first 19 bytes, then
/// the offset (hours <= 23, minutes <= 59), exactly as `parse_ts_general`.
#[inline(always)]
fn parse_f2(s: &[u8; 25]) -> Option<i64> {
    let d = |i: usize| s[i].wrapping_sub(b'0');
    if d(20) > 9 || d(21) > 9 || s[22] != b':' || d(23) > 9 || d(24) > 9 {
        return None;
    }
    let (oh, om) = ((d(20) * 10 + d(21)) as i64, (d(23) * 10 + d(24)) as i64);
    if oh > 23 || om > 59 {
        return None;
    }
    let sign = if s[19] == b'+' { 1 } else { -1 };
    let mut f1 = [0u8; 20];
    f1[..19].copy_from_slice(&s[..19]);
    f1[19] = b'Z';
    Some(parse_f1(&f1)? - sign * (oh * 60 + om) * 60_000)
}

/// The bad-byte test of one word: `e` is the word XOR its expected bytes (`0` in digit places, the separator
/// itself elsewhere), `digits` has 0xFF in the digit places. A digit byte is good when its `e` is at most 9
/// (exactly `0`-`9`), a separator byte when its `e` is 0. Nonzero iff some byte is bad: a byte's carry only
/// reaches higher bytes, and only out of a bad byte, so the lowest bad byte is always seen and good bytes
/// never set a flag. The add wraps: a carry out of the top byte comes only from a bad top byte (e >= 0x8A),
/// which its own bit 7 already flags.
#[inline(always)]
fn bad_bytes(e: u64, digits: u64) -> u64 {
    let hi = digits & 0x8080_8080_8080_8080;
    let k = digits & 0x7676_7676_7676_7676;
    (e & !digits) | ((((e & digits).wrapping_add(k)) | e) & hi)
}

/// Each byte k (k + 1 also a digit place) becomes 10 * digit k + digit k + 1; digits are at most 9, so no
/// byte overflows.
#[inline(always)]
fn pairs(d: u64) -> u64 {
    d * 10 + (d >> 8)
}

/// `YYYY-MM-DDTHH:MM:SSZ` with every check of the general path, at fixed positions, eight bytes at a time.
#[inline(always)]
fn parse_f1(s: &[u8; 20]) -> Option<i64> {
    const EA: u64 = u64::from_le_bytes(*b"0000-00-");
    const EB: u64 = u64::from_le_bytes(*b"00T00:00");
    const EC: u64 = u32::from_le_bytes(*b":00Z") as u64;
    const DA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const DB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const DC: u64 = 0x0000_0000_00FF_FF00;
    let ea = u64::from_le_bytes(s[0..8].try_into().unwrap()) ^ EA;
    let eb = u64::from_le_bytes(s[8..16].try_into().unwrap()) ^ EB;
    let ec = u32::from_le_bytes(s[16..20].try_into().unwrap()) as u64 ^ EC;
    if bad_bytes(ea, DA) | bad_bytes(eb, DB) | bad_bytes(ec, DC) != 0 {
        return None;
    }
    let (pa, pb, pc) = (pairs(ea & DA), pairs(eb & DB), pairs(ec & DC));
    let byte = |w: u64, k: u32| ((w >> (8 * k)) & 0xFF) as i64;
    let (y, mo, dd) = (byte(pa, 0) * 100 + byte(pa, 2), byte(pa, 5), byte(pb, 0));
    let (h, mi, se) = (byte(pb, 3), byte(pb, 6), byte(pc, 1));
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

    #[test]
    fn f3_13_fast_equals_general() {
        let bases: [&[u8; 13]; 4] = [b"1753012345678", b"0000000000000", b"9999999999999", b"0123456789012"];
        for base in bases {
            assert!(parse_ts(base).is_some());
            assert_eq!(parse_ts(base), parse_ts_general(base));
            for i in 0..13 {
                for c in 0..=255u8 {
                    let mut v = *base;
                    v[i] = c;
                    assert_eq!(parse_ts(&v), parse_ts_general(&v), "{:?}", String::from_utf8_lossy(&v));
                    if i > 0 {
                        for low in [0xFFu8, 0x8A, b':', b'/'] {
                            let mut w = v;
                            w[i - 1] = low;
                            assert_eq!(parse_ts(&w), parse_ts_general(&w), "{:?}", String::from_utf8_lossy(&w));
                        }
                    }
                }
            }
        }
        // many values: every digit in every place
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let t = format!("{:013}", x % 10_000_000_000_000);
            assert_eq!(parse_ts(t.as_bytes()), parse_ts_general(t.as_bytes()), "{t}");
        }
    }

    /// The F2 fast path against the general path.
    #[test]
    fn f2_fast_equals_general() {
        let bases: [&[u8; 25]; 5] = [
            b"2026-10-03T19:58:10-04:00",
            b"2028-02-29T00:00:00+23:59",
            b"0001-01-01T00:00:00+00:00",
            b"9999-12-31T23:59:59-23:59",
            b"2025-07-16T00:30:00+05:30",
        ];
        let subs = b"0123456789-T:Z +/a\x00\xff";
        for base in bases {
            assert!(parse_ts(base).is_some());
            assert_eq!(parse_ts(base), parse_ts_general(base));
            for i in 0..25 {
                for &c in subs {
                    let mut v = *base;
                    v[i] = c;
                    assert_eq!(parse_ts(&v), parse_ts_general(&v), "{:?}", String::from_utf8_lossy(&v));
                    for j in 0..25 {
                        for &c2 in b"0129+-:" {
                            let mut w = v;
                            w[j] = c2;
                            assert_eq!(parse_ts(&w), parse_ts_general(&w), "{:?}", String::from_utf8_lossy(&w));
                        }
                    }
                }
            }
        }
        // every offset hour and minute, around the limits
        for oh in 0..=25 {
            for om in [0, 1, 30, 58, 59, 60, 99] {
                for sign in ['+', '-'] {
                    let t = format!("2027-12-20T22:03:47{sign}{oh:02}:{om:02}");
                    assert_eq!(parse_ts(t.as_bytes()), parse_ts_general(t.as_bytes()), "{t}");
                }
            }
        }
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
        // every byte value at every position, alone and next to a bad byte below it (the SWAR carry case)
        for base in bases {
            for i in 0..20 {
                for c in 0..=255u8 {
                    let mut v = *base;
                    v[i] = c;
                    assert_eq!(parse_ts(&v), parse_ts_general(&v), "{:?}", String::from_utf8_lossy(&v));
                    if i > 0 {
                        for low in [0xFFu8, 0x8A, 0x39 + 1, b'/'] {
                            let mut w = v;
                            w[i - 1] = low;
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
