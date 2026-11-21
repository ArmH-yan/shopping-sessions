//! Parsing primitives: ID keys, the CSV dialect, numbers and timestamps (rules.md).
#![allow(dead_code)]
use std::sync::atomic::{AtomicBool, Ordering};

pub const DAY: i64 = 86_400_000;
pub const BAD_TS: i64 = i64::MIN;

pub static BAIL: AtomicBool = AtomicBool::new(false);

pub fn bail() {
    BAIL.store(true, Ordering::Relaxed);
}

// ------------------------------------------------------------------ keys

#[inline]
pub fn key(b: &[u8]) -> u128 {
    if b.len() > 15 {
        bail();
        return 1; // never used: we exit before producing output
    }
    let mut buf = [0u8; 16];
    buf[..b.len()].copy_from_slice(b);
    buf[15] = b.len() as u8;
    u128::from_be_bytes(buf)
}

pub fn key_bytes(k: u128) -> Vec<u8> {
    let b = k.to_be_bytes();
    b[..b[15] as usize].to_vec()
}

#[inline]
pub fn shard_of(k: u128) -> usize {
    let x = (k as u64) ^ ((k >> 64) as u64).rotate_left(17);
    (x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize
}

// ------------------------------------------------------------------ CSV

/// Splits one line into exactly N fields with the rules' dialect. None = malformed.
/// An empty field (or "") is returned as an empty slice, meaning null. One pass over the bytes.
#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut n = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32; // quotes seen in the current field
    let len = line.len();
    let mut i = 0usize;
    while i <= len {
        let c = if i < len { line[i] } else { b',' };
        if c == b',' {
            if n == N {
                return None;
            }
            let f = &line[start..i];
            out[n] = if quotes == 0 {
                f
            } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                &f[1..f.len() - 1]
            } else {
                return None;
            };
            n += 1;
            start = i + 1;
            quotes = 0;
        } else if c == b'"' {
            quotes += 1;
        }
        i += 1;
    }
    if n == N && len > 0 { Some(out) } else { None }
}

// ------------------------------------------------------------------ numbers and time

#[inline]
pub fn all_digits(b: &[u8]) -> bool {
    !b.is_empty() && b.iter().all(|c| c.is_ascii_digit())
}

/// Digits only, leading zeros allowed. Values over 18 significant digits return i64::MAX.
#[inline]
pub fn digits_value(b: &[u8]) -> i64 {
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    if b.len() - i > 18 {
        return i64::MAX;
    }
    let mut v: i64 = 0;
    for &c in &b[i..] {
        v = v * 10 + (c - b'0') as i64;
    }
    v
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 }
        }
    }
}

#[inline]
pub fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i], b[i + 1]);
    if a.is_ascii_digit() && c.is_ascii_digit() {
        Some(((a - b'0') * 10 + (c - b'0')) as i64)
    } else {
        None
    }
}

/// Days from 1970-01-01 to the first of each month, for years 1900..2200 (fast path).
const Y0: i64 = 1900;
const NY: usize = 300;
static MONTH_START: std::sync::LazyLock<Vec<i64>> = std::sync::LazyLock::new(|| {
    let mut v = vec![0i64; NY * 12];
    for y in 0..NY {
        for m in 0..12 {
            v[y * 12 + m] = days_from_civil(Y0 + y as i64, m as i64 + 1, 1);
        }
    }
    v
});

#[inline(always)]
fn dig(c: u8) -> Option<i64> {
    let d = c.wrapping_sub(b'0');
    if d < 10 { Some(d as i64) } else { None }
}

/// F1 / F2 / F3 -> epoch ms; BAD_TS when bad.
#[inline]
pub fn parse_ts(b: &[u8]) -> i64 {
    let n = b.len();
    let zulu = n == 20 && b[19] == b'Z';
    let offs = n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':';
    if zulu || offs {
        return parse_iso(b, offs);
    }
    if all_digits(b) {
        return digits_value(b);
    }
    BAD_TS
}

#[inline(always)]
fn parse_iso(b: &[u8], offs: bool) -> i64 {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return BAD_TS;
    }
    let num = |i: usize, k: usize| -> Option<i64> {
        let mut v = 0;
        for j in i..i + k {
            v = v * 10 + dig(b[j])?;
        }
        Some(v)
    };
    let (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) =
        (num(0, 4), num(5, 2), num(8, 2), num(11, 2), num(14, 2), num(17, 2))
    else {
        return BAD_TS;
    };
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return BAD_TS;
    }
    let days = if (Y0..Y0 + NY as i64).contains(&y) {
        MONTH_START[(y - Y0) as usize * 12 + (mo - 1) as usize] + d - 1
    } else {
        days_from_civil(y, mo, d)
    };
    let mut ms = days * DAY + h * 3_600_000 + mi * 60_000 + s * 1000;
    if offs {
        let (Some(oh), Some(om)) = (num(20, 2), num(23, 2)) else {
            return BAD_TS;
        };
        if oh > 23 || om > 59 {
            return BAD_TS;
        }
        let off = oh * 3_600_000 + om * 60_000;
        ms -= if b[19] == b'-' { -off } else { off };
    }
    ms
}

/// `parse_ts` with a small cache of "YYYY-MM-DD" -> day number (or invalid), and the time of
/// day validated and converted with SWAR. Returns exactly what `parse_ts` returns.
pub struct TsParser {
    keys: [(u64, u16); 64],
    days: [i64; 64], // i64::MIN = invalid date
}

impl Default for TsParser {
    fn default() -> Self {
        TsParser { keys: [(0, 0); 64], days: [0; 64] }
    }
}

const NO_DATE: i64 = i64::MIN;

impl TsParser {
    #[inline(always)]
    pub fn parse(&mut self, b: &[u8]) -> i64 {
        let n = b.len();
        let zulu = n == 20 && b[19] == b'Z';
        let offs = n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':';
        if !(zulu || offs) {
            return if all_digits(b) { digits_value(b) } else { BAD_TS };
        }
        if b[10] != b'T' {
            return BAD_TS;
        }
        let k8 = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let k2 = u16::from_le_bytes([b[8], b[9]]);
        let slot = ((k8 ^ (k2 as u64).rotate_left(29)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
        let days = if self.keys[slot] == (k8, k2) && k8 != 0 {
            self.days[slot]
        } else {
            let d = date_days(&b[0..10]);
            self.keys[slot] = (k8, k2);
            self.days[slot] = d;
            d
        };
        if days == NO_DATE {
            return BAD_TS;
        }
        let Some(secs) = hms(u64::from_le_bytes(b[11..19].try_into().unwrap())) else {
            return BAD_TS;
        };
        let mut ms = days * DAY + secs * 1000;
        if offs {
            let (Some(oh), Some(om)) = (d2(b, 20), d2(b, 23)) else {
                return BAD_TS;
            };
            if oh > 23 || om > 59 {
                return BAD_TS;
            }
            let off = oh * 3_600_000 + om * 60_000;
            ms -= if b[19] == b'-' { -off } else { off };
        }
        ms
    }
}

/// "YYYY-MM-DD" -> days since 1970-01-01, or NO_DATE if malformed or impossible.
fn date_days(b: &[u8]) -> i64 {
    if b[4] != b'-' || b[7] != b'-' {
        return NO_DATE;
    }
    let num = |i: usize, k: usize| -> Option<i64> {
        let mut v = 0;
        for j in i..i + k {
            v = v * 10 + dig(b[j])?;
        }
        Some(v)
    };
    let (Some(y), Some(mo), Some(d)) = (num(0, 4), num(5, 2), num(8, 2)) else {
        return NO_DATE;
    };
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return NO_DATE;
    }
    if (Y0..Y0 + NY as i64).contains(&y) {
        MONTH_START[(y - Y0) as usize * 12 + (mo - 1) as usize] + d - 1
    } else {
        days_from_civil(y, mo, d)
    }
}

/// "HH:MM:SS" (as 8 little-endian bytes) -> seconds of the day, if well-formed and in range.
#[inline(always)]
fn hms(x: u64) -> Option<i64> {
    const DIGITS: u64 = 0x0000_FF00_00FF_0000 ^ u64::MAX; // all bytes except the two colons
    // every byte has high nibble 3 ('0'..'9' and ':' all do)
    if x & 0xF0F0_F0F0_F0F0_F0F0 != 0x3030_3030_3030_3030 {
        return None;
    }
    let lo = x & 0x0F0F_0F0F_0F0F_0F0F;
    // digit bytes: low nibble <= 9; colon bytes: exactly ':' (low nibble 0xA)
    if (lo + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010 & DIGITS != 0 {
        return None;
    }
    if (x >> 16) & 0xFF != b':' as u64 || (x >> 40) & 0xFF != b':' as u64 {
        return None;
    }
    let byte = |i: u32| ((lo >> (8 * i)) & 0xFF) as i64;
    let (h, m, s) = (byte(0) * 10 + byte(1), byte(3) * 10 + byte(4), byte(6) * 10 + byte(7));
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some(h * 3600 + m * 60 + s)
}

#[inline(always)]
pub fn field(d: &[u8], s: usize, e: usize, q: u32) -> Option<&[u8]> {
    if q == 0 {
        Some(&d[s..e])
    } else if q == 2 && e - s >= 2 && d[s] == b'"' && d[e - 1] == b'"' {
        Some(&d[s + 1..e - 1])
    } else {
        None
    }
}

/// (comma mask, newline mask, quote mask) for data[base..base+64]: bit i set when that byte
/// is ',' / '\n' / '"'. Needs base + 64 <= data.len(). SSE2 version (every x86-64 CPU).
#[inline(always)]
fn masks_sse(data: &[u8], base: usize) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let p = data.as_ptr().add(base);
        let (nl, cm, qt) = (_mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'"' as i8));
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
            c |= ((_mm_movemask_epi8(_mm_cmpeq_epi8(v, cm)) as u32) as u64) << (16 * k);
            n |= ((_mm_movemask_epi8(_mm_cmpeq_epi8(v, nl)) as u32) as u64) << (16 * k);
            q |= ((_mm_movemask_epi8(_mm_cmpeq_epi8(v, qt)) as u32) as u64) << (16 * k);
        }
        (c, n, q)
    }
    #[cfg(not(target_arch = "x86_64"))]
    masks_tail(data, base)
}

/// Same masks with one AVX-512 compare per byte class (Cascade Lake and newer Xeons).
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512bw")]
unsafe fn masks_512(data: &[u8], base: usize) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let v = _mm512_loadu_si512(data.as_ptr().add(base) as *const _);
        (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
        )
    }
}

/// Masks for the last, partial block (scalar).
#[inline(never)]
fn masks_tail(data: &[u8], base: usize) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..(data.len() - base).min(64) {
        match data[base + i] {
            b',' => c |= 1 << i,
            b'\n' => n |= 1 << i,
            b'"' => q |= 1 << i,
            _ => {}
        }
    }
    (c, n, q)
}

static HAS_512: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    #[cfg(target_arch = "x86_64")]
    {
        std::is_x86_feature_detected!("avx512bw") && std::env::var_os("NO_AVX512").is_none()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
});

/// Every well-formed record of `data` as N fields, skipping malformed and empty lines.
/// Separators are found 64 bytes at a time with SIMD; quotes per field are counted with a
/// running popcount, so the loop stops only at commas and newlines.
#[inline]
pub fn records<const N: usize>(data: &[u8], f: impl FnMut([&[u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    if *HAS_512 {
        return unsafe { records_512::<N>(data, f) };
    }
    records_impl::<N, false>(data, f)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
unsafe fn records_512<const N: usize>(data: &[u8], f: impl FnMut([&[u8]; N])) {
    records_impl::<N, true>(data, f)
}

#[inline(always)]
fn records_impl<const N: usize, const W: bool>(data: &[u8], mut f: impl FnMut([&[u8]; N])) {
    let empty: &[u8] = &data[..0];
    let mut out: [&[u8]; N] = [empty; N];
    let (mut n, mut start, mut line_start, mut bad) = (0usize, 0usize, 0usize, false);
    let len = data.len();
    let mut base = 0usize;
    let mut q_before_block: u32 = 0; // quotes in data[..base]
    let mut q_at_start: u32 = 0; // quotes in data[..start]
    while base < len {
        let (cm, nm, qm) = if base + 64 <= len {
            #[cfg(target_arch = "x86_64")]
            {
                if W { unsafe { masks_512(data, base) } } else { masks_sse(data, base) }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                masks_sse(data, base)
            }
        } else {
            masks_tail(data, base)
        };
        let mut m = cm | nm;
        while m != 0 {
            let i = m.trailing_zeros();
            m &= m - 1;
            let p = base + i as usize;
            let q_here = if qm == 0 { q_before_block } else { q_before_block + (qm & ((1u64 << i) - 1)).count_ones() };
            let q = q_here - q_at_start;
            // Field data[start..p], branch-free: valid when it has no quotes, or exactly two
            // that wrap the whole field (and it is at least 2 bytes long).
            let flen = p - start;
            let wrapped = (q == 2) & (flen >= 2) & (data[start] == b'"') & (data[p.saturating_sub(1)] == b'"');
            let valid = (q == 0) | wrapped;
            let w = wrapped as usize;
            let slot = if n < N { n } else { N - 1 };
            out[slot] = &data[start + w..p - w];
            if (cm >> i) & 1 == 1 {
                bad |= !valid | (n + 1 >= N);
                n += 1;
            } else {
                if !bad & valid & (p > line_start) & (n + 1 == N) {
                    f(out);
                }
                n = 0;
                line_start = p + 1;
                bad = false;
            }
            start = p + 1;
            q_at_start = q_here; // data[p] is not a quote
        }
        q_before_block += qm.count_ones();
        base += 64;
    }
    let q = q_before_block - q_at_start;
    if !bad && len > line_start && n + 1 == N {
        if let Some(v) = field(data, start, len, q) {
            out[n] = v;
            f(out);
        }
    }
}

/// An exact 64-bit code for an ID of 1 to 10 ASCII letters and digits (6 bits per character,
/// no zero digit, so different lengths never collide). Anything else is interned with bit 63 set.
const fn code_table() -> [u8; 256] {
    let mut t = [0x80u8; 256];
    let mut c = 0;
    while c < 256 {
        let u = c as u8;
        t[c] = if u >= b'0' && u <= b'9' {
            u - b'0' + 1
        } else if u >= b'A' && u <= b'Z' {
            u - b'A' + 11
        } else if u >= b'a' && u <= b'z' {
            u - b'a' + 37
        } else {
            0x80
        };
        c += 1;
    }
    t
}
static CODE: [u8; 256] = code_table();

#[inline]
pub fn code64(b: &[u8]) -> u64 {
    if !b.is_empty() && b.len() <= 10 {
        let mut v: u64 = 0;
        let mut bad: u8 = 0;
        for &c in b {
            let d = CODE[c as usize];
            bad |= d;
            v = (v << 6) | (d & 63) as u64;
        }
        if bad & 0x80 == 0 {
            return v;
        }
    }
    intern(b)
}

#[derive(Default)]
struct Interned {
    code: ahash::AHashMap<Vec<u8>, u64>,
    bytes: Vec<Vec<u8>>,
}

static INTERN: std::sync::LazyLock<std::sync::Mutex<Interned>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Interned::default()));

#[cold]
fn intern(b: &[u8]) -> u64 {
    let mut m = INTERN.lock().unwrap();
    if let Some(&c) = m.code.get(b) {
        return c;
    }
    let c = (1u64 << 63) | m.bytes.len() as u64;
    m.bytes.push(b.to_vec());
    m.code.insert(b.to_vec(), c);
    c
}

/// Appends the bytes of a `code64` code to `out`.
#[inline]
pub fn decode64_into(c: u64, out: &mut Vec<u8>) {
    if c >> 63 == 1 {
        let m = INTERN.lock().unwrap();
        out.extend_from_slice(&m.bytes[(c & !(1u64 << 63)) as usize]);
        return;
    }
    let n = (64 - c.leading_zeros() as usize).div_ceil(6);
    for i in (0..n).rev() {
        let d = ((c >> (6 * i)) & 63) as u8;
        out.push(match d {
            1..=10 => b'0' + d - 1,
            11..=36 => b'A' + d - 11,
            _ => b'a' + d - 37,
        });
    }
}

/// The bytes of a `code64` code.
pub fn decode64(c: u64) -> Vec<u8> {
    if c >> 63 == 1 {
        let m = INTERN.lock().unwrap();
        return m.bytes[(c & !(1u64 << 63)) as usize].clone();
    }
    let mut out = vec![];
    let mut v = c;
    while v != 0 {
        let d = (v & 63) as u8;
        out.push(match d {
            1..=10 => b'0' + d - 1,
            11..=36 => b'A' + d - 11,
            _ => b'a' + d - 37,
        });
        v >>= 6;
    }
    out.reverse();
    out
}

#[inline]
pub fn shard64(x: u64) -> usize {
    (x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64) -> i64 {
        days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + s * 1000
    }

    #[test]
    fn timestamps_valid_forms() {
        let f1 = ms(2026, 10, 3, 23, 58, 10);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), f1);
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), f1); // F2 equals the F1 example
        assert_eq!(parse_ts(b"1791071890000"), 1_791_071_890_000);
        assert_eq!(parse_ts(b"0001791071890000"), 1_791_071_890_000); // leading zeros
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+02:00"), ms(2026, 10, 3, 23, 30, 0));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), ms(2026, 10, 3, 10, 0, 0));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00+23:59"), ms(2026, 10, 3, 10, 0, 0) - (23 * 60 + 59) * 60_000);
        assert_eq!(parse_ts(b"2028-02-29T12:00:00Z"), ms(2028, 2, 29, 12, 0, 0));
        assert_eq!(parse_ts(b"86400000"), DAY); // valid form, just outside any window
    }

    #[test]
    fn timestamps_bad_forms() {
        for bad in [
            &b""[..],
            b"2026-10-03T10:00:00.000Z",
            b"2026-10-03T10:00:00z",
            b"2026-10-03t10:00:00Z",
            b"2026-10-03 10:00:00Z",
            b"2026-10-03T10:00:00",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T10:00:00Z",
            b"2026-02-30T10:00:00Z",
            b"2027-02-29T10:00:00Z",
            b"2026-10-03T24:00:00Z",
            b"2026-10-03T10:00:60Z",
            b"2026-10-03T10:60:00Z",
            b"2026-10-03T10:00:00+24:00",
            b"2026-10-03T10:00:00+05:60",
            b"2026-10-03T10:00:00+0200",
            b"2026-10-03T10:00:00Z\r",
            "\u{662}\u{665}\u{660}".as_bytes(), // Arabic-Indic digits
            "2\u{ff10}26-10-03T10:00:00Z".as_bytes(), // full-width digit
        ] {
            assert_eq!(parse_ts(bad), BAD_TS, "{:?}", String::from_utf8_lossy(bad));
        }
        // too long for 64 bits: a number, but far outside every window
        assert!(parse_ts(b"123456789012345678901") > 10_000_000_000_000);
    }

    fn recs<const N: usize>(data: &[u8]) -> Vec<Vec<String>> {
        let mut v = vec![];
        records::<N>(data, |f| v.push(f.iter().map(|x| String::from_utf8_lossy(x).into_owned()).collect()));
        v
    }

    #[test]
    fn csv_dialect() {
        let r = recs::<2>(b"a,b\n\"u4\",\"u1\"\n\"\",x\nu1,u2,u3\nu1\n\nu1,u5\"\n\"u1,u2\nab\"c,d\n,\n last , b\r\nend,line");
        assert_eq!(
            r,
            vec![
                vec!["a", "b"],
                vec!["u4", "u1"],      // quotes are syntax
                vec!["", "x"],         // "" is an empty field (null)
                vec!["", ""],          // ",": two empty fields, well-formed
                vec![" last ", " b\r"], // spaces and '\r' are data
                vec!["end", "line"],   // last line without '\n'
            ]
        );
    }

    #[test]
    fn csv_long_lines_cross_simd_blocks() {
        let mut data = vec![];
        for i in 0..1000 {
            let pad = "x".repeat(i % 97);
            data.extend_from_slice(format!("\"{pad}\",{i},\"q\"\n").as_bytes());
            if i % 7 == 0 {
                data.extend_from_slice(b"bad\"line,1,2\n");
            }
        }
        let r = recs::<3>(&data);
        assert_eq!(r.len(), 1000);
        for (i, row) in r.iter().enumerate() {
            assert_eq!(row[0].len(), i % 97);
            assert_eq!(row[1], i.to_string());
            assert_eq!(row[2], "q");
        }
    }

    #[test]
    fn codes_are_exact() {
        let ids: Vec<&[u8]> = vec![b"u1", b"U1", b"u10", b"u01", b"e9pr3kfk5", b"0", b"zzzzzzzzzz", b" u1", b"u1 ", b"A\tB", b"zzzzzzzzzzz"];
        let codes: Vec<u64> = ids.iter().map(|b| code64(b)).collect();
        for (i, a) in codes.iter().enumerate() {
            assert_eq!(decode64(*a), ids[i].to_vec());
            let mut v = b"x".to_vec();
            decode64_into(*a, &mut v);
            assert_eq!(&v[1..], ids[i]);
            for b in &codes[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert_eq!(code64(b" u1"), code64(b" u1")); // interned codes are stable
    }

    #[test]
    fn keys_sort_byte_wise() {
        let mut ids: Vec<&[u8]> = vec![b"u2", b"u10", b"U7", b"u1", b"A\tB", b"u1 ", b" u1"];
        let mut by_key = ids.clone();
        by_key.sort_by_key(|b| key(b));
        ids.sort();
        assert_eq!(by_key, ids);
    }

    #[test]
    fn ts_parser_matches_parse_ts() {
        // every valid and bad form, then many random mutations of them
        let mut cases: Vec<Vec<u8>> = vec![];
        let seeds: [&[u8]; 8] = [
            b"2026-10-03T23:58:10Z",
            b"2026-10-03T19:58:10-04:00",
            b"2028-02-29T00:00:00+12:00",
            b"2027-02-28T23:59:59-10:00",
            b"1791071890000",
            b"2026-10-03T24:00:00Z",
            b"2026-02-30T10:00:00Z",
            b"2026-10-03T10:00:00+05:60",
        ];
        let alphabet = b"0123456789:-+TZtz .9/";
        let mut r: u64 = 0x1234_5678_9abc_def1;
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        for s in seeds {
            cases.push(s.to_vec());
            for _ in 0..4000 {
                let mut v = s.to_vec();
                for _ in 0..(next() % 3 + 1) {
                    let i = (next() as usize) % v.len();
                    v[i] = alphabet[(next() as usize) % alphabet.len()];
                }
                cases.push(v);
            }
        }
        let mut p = TsParser::default();
        for c in cases.iter().chain(cases.iter()) {
            assert_eq!(p.parse(c), parse_ts(c), "{:?}", String::from_utf8_lossy(c));
        }
    }

    #[test]
    fn prices_digits_only() {
        assert!(all_digits(b"0250") && digits_value(b"0250") == 250);
        assert!(!all_digits(b"19.99") && !all_digits(b"-5") && !all_digits(b"") && !all_digits("\u{ff12}".as_bytes()));
        assert_eq!(digits_value(b"0000000000000000000000001"), 1);
        assert_eq!(digits_value(b"99999999999999999999"), i64::MAX);
    }
}
