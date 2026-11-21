//! Shopping Sessions: a fast, exact implementation of rules.md.
//!
//! IDs are packed into u128 keys (up to 15 bytes, big-endian, length in the low byte), so key
//! order is byte-wise string order and equality is exact. An ID longer than 15 bytes makes the
//! program exit with code 3, and run.sh falls back to the DuckDB pipeline.

use ahash::AHashMap;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const BAD_TS: i64 = i64::MIN;
const NS: usize = 256; // shards
const CHUNK: usize = 4 << 20;
const UNKNOWN_CAT: u8 = 254;
const NO_CAMP: u16 = u16::MAX;

static BAIL: AtomicBool = AtomicBool::new(false);

fn bail() {
    BAIL.store(true, Ordering::Relaxed);
}

// ------------------------------------------------------------------ keys

#[inline]
fn key(b: &[u8]) -> u128 {
    if b.len() > 15 {
        bail();
        return 1; // never used: we exit before producing output
    }
    let mut buf = [0u8; 16];
    buf[..b.len()].copy_from_slice(b);
    buf[15] = b.len() as u8;
    u128::from_be_bytes(buf)
}

fn key_bytes(k: u128) -> Vec<u8> {
    let b = k.to_be_bytes();
    b[..b[15] as usize].to_vec()
}

#[inline]
fn shard_of(k: u128) -> usize {
    let x = (k as u64) ^ ((k >> 64) as u64).rotate_left(17);
    (x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize
}

// ------------------------------------------------------------------ CSV

/// Splits one line into exactly N fields with the rules' dialect. None = malformed.
/// An empty field (or "") is returned as an empty slice, meaning null. One pass over the bytes.
#[inline]
fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
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
fn all_digits(b: &[u8]) -> bool {
    !b.is_empty() && b.iter().all(|c| c.is_ascii_digit())
}

/// Digits only, leading zeros allowed. Values over 18 significant digits return i64::MAX.
#[inline]
fn digits_value(b: &[u8]) -> i64 {
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

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 }
        }
    }
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i], b[i + 1]);
    if a.is_ascii_digit() && c.is_ascii_digit() {
        Some(((a - b'0') * 10 + (c - b'0')) as i64)
    } else {
        None
    }
}

/// F1 / F2 / F3 -> epoch ms; BAD_TS when bad.
fn parse_ts(b: &[u8]) -> i64 {
    if b.is_empty() {
        return BAD_TS;
    }
    if all_digits(b) {
        return digits_value(b);
    }
    let n = b.len();
    let zulu = n == 20 && b[19] == b'Z';
    let offs = n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':';
    if !(zulu || offs) {
        return BAD_TS;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return BAD_TS;
    }
    let (Some(y1), Some(y2), Some(mo), Some(d), Some(h), Some(mi), Some(s)) =
        (d2(b, 0), d2(b, 2), d2(b, 5), d2(b, 8), d2(b, 11), d2(b, 14), d2(b, 17))
    else {
        return BAD_TS;
    };
    let y = y1 * 100 + y2;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return BAD_TS;
    }
    let mut ms = days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + s * 1000;
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

// ------------------------------------------------------------------ files

struct Chunk {
    map: Arc<Mmap>,
    start: usize,
    end: usize,
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// Data region of each file (header line skipped), cut into chunks at '\n'.
fn chunks(dir: &Path) -> Vec<Chunk> {
    let mut out = vec![];
    for p in csv_files(dir) {
        let f = File::open(&p).expect("open");
        if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            continue;
        }
        let map = Arc::new(unsafe { Mmap::map(&f).expect("mmap") });
        let data: &[u8] = &map;
        let Some(h) = data.iter().position(|&c| c == b'\n') else { continue };
        let mut s = h + 1;
        while s < data.len() {
            let mut e = (s + CHUNK).min(data.len());
            if e < data.len() {
                e = match data[e..].iter().position(|&c| c == b'\n') {
                    Some(p) => e + p + 1,
                    None => data.len(),
                };
            }
            out.push(Chunk { map: map.clone(), start: s, end: e });
            s = e;
        }
    }
    out
}

/// Lines of a chunk; empty lines (malformed) are skipped, as is the empty tail after the last '\n'.
fn for_lines(c: &Chunk, mut f: impl FnMut(&[u8])) {
    let data = &c.map[c.start..c.end];
    let mut s = 0;
    for e in memchr::memchr_iter(b'\n', data) {
        if e > s {
            f(&data[s..e]);
        }
        s = e + 1;
    }
    if s < data.len() {
        f(&data[s..]);
    }
}

fn small_csv<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let data = fs::read(path).unwrap_or_default();
    let mut out = vec![];
    let mut it = data.split(|&b| b == b'\n');
    it.next();
    for line in it {
        if let Some(fs) = fields::<N>(line) {
            out.push(fs.map(|x| x.to_vec()));
        }
    }
    out
}

// ------------------------------------------------------------------ records

#[derive(Clone, Copy)]
struct Ev {
    ek: u128,
    uk: u128,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u8,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Sv {
    pk: u128,
    ts: i64,
    price: u32, // 0 = not a purchase
    cat: u8,
}

#[derive(Clone, Copy)]
struct Cl {
    ck: u128,
    pk: u128, // user key at parse time, person key after mapping
    ts: i64,
    camp: u16,
}

struct UserRow {
    pk: u128,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u8>,
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    if let Ok(n) = std::env::var("NCPU").map(|s| s.parse::<usize>().unwrap_or(0)) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
        }
    }
    let t0 = std::time::Instant::now();
    let verbose = std::env::var("VERBOSE").is_ok();
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:8.3}s {}", t0.elapsed().as_secs_f64(), what);
        }
    };

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).expect("manifest")).expect("json");
    let ws = parse_ts(manifest["window_start"].as_str().unwrap().as_bytes());
    let we = parse_ts(manifest["window_end"].as_str().unwrap().as_bytes());

    // Products and campaigns.
    let mut cat_names: Vec<Vec<u8>> = vec![];
    let mut cat_ix: AHashMap<Vec<u8>, u8> = AHashMap::new();
    let mut products: AHashMap<u128, u8> = AHashMap::new();
    for [p, c] in small_csv::<2>(&data.join("products.csv")) {
        if p.is_empty() || p.len() > 15 {
            if p.len() > 15 {
                bail();
            }
            continue;
        }
        let ix = *cat_ix.entry(c.clone()).or_insert_with(|| {
            cat_names.push(c.clone());
            (cat_names.len() - 1) as u8
        });
        products.insert(key(&p), ix);
    }
    if cat_names.len() >= UNKNOWN_CAT as usize {
        bail();
    }
    // rank[cat] for byte-wise tie-breaking; UNKNOWN_CAT handled via `unknown_rank`.
    let mut all_names: Vec<(Vec<u8>, u8)> = cat_names.iter().cloned().enumerate().map(|(i, n)| (n, i as u8)).collect();
    all_names.push((b"unknown".to_vec(), UNKNOWN_CAT));
    all_names.sort();
    let mut rank = [0u8; 256];
    for (r, (_, c)) in all_names.iter().enumerate() {
        rank[*c as usize] = r as u8;
    }
    let ncat_slots = 256;

    let mut camp_ids: Vec<Vec<u8>> = vec![];
    let mut camp_channel: Vec<Vec<u8>> = vec![];
    let mut campaigns: AHashMap<u128, u16> = AHashMap::new();
    for [c, ch] in small_csv::<2>(&data.join("campaigns.csv")) {
        if c.is_empty() || c.len() > 15 {
            if c.len() > 15 {
                bail();
            }
            continue;
        }
        let k = key(&c);
        if campaigns.contains_key(&k) {
            continue;
        }
        campaigns.insert(k, camp_ids.len() as u16);
        camp_ids.push(c);
        camp_channel.push(ch);
    }
    if camp_ids.len() >= (NO_CAMP - 1) as usize {
        bail();
    }
    let organic = camp_ids.len() as u16;
    lap("small tables");

    // ---------------------------------------------------------- links -> person names
    let link_chunks = chunks(&data.join("links"));
    let link_pairs: Vec<(u128, u128)> = link_chunks
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = vec![];
            for_lines(c, |l| {
                if let Some([a, b]) = fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((key(a), key(b)));
                    }
                }
            });
            v
        })
        .collect();
    // IDs sorted byte-wise: index order = name order, so a root that is always the smaller
    // index is the person's name.
    let mut ids: Vec<u128> = link_pairs.par_iter().flat_map_iter(|&(a, b)| [a, b]).collect();
    ids.par_sort_unstable();
    ids.dedup();
    let pairs: Vec<(u32, u32)> = link_pairs
        .par_iter()
        .map(|&(a, b)| (ids.binary_search(&a).unwrap() as u32, ids.binary_search(&b).unwrap() as u32))
        .collect();
    drop(link_pairs);
    let mut parent: Vec<u32> = (0..ids.len() as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for &(a, b) in &pairs {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra < rb {
            parent[rb as usize] = ra;
        } else if rb < ra {
            parent[ra as usize] = rb;
        }
    }
    for i in 0..ids.len() as u32 {
        let r = find(&mut parent, i);
        parent[i as usize] = r;
    }
    // one map per shard, built in parallel
    let mut by_shard: Vec<Vec<u32>> = (0..NS).map(|_| Vec::new()).collect();
    for (i, &k) in ids.iter().enumerate() {
        by_shard[shard_of(k)].push(i as u32);
    }
    let person: Vec<AHashMap<u128, u128>> = by_shard
        .par_iter()
        .map(|v| {
            let mut m = AHashMap::with_capacity(v.len());
            for &i in v {
                m.insert(ids[i as usize], ids[parent[i as usize] as usize]);
            }
            m
        })
        .collect();
    drop(by_shard);
    let pmap = |k: u128| -> u128 { *person[shard_of(k)].get(&k).unwrap_or(&k) };
    lap("links");

    // ---------------------------------------------------------- events: parse
    let ev_chunks = chunks(&data.join("events"));
    let parsed: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let guess = (c.end - c.start) / 70 / NS * 5 / 4 + 16;
            let mut sh: Vec<Vec<Ev>> = (0..NS).map(|_| Vec::with_capacity(guess)).collect();
            for_lines(c, |l| {
                let Some([eid, uid, ts, ing, et, pid, price]) = fields::<7>(l) else { return };
                if eid.is_empty() {
                    return;
                }
                let ek = key(eid);
                let uk = if uid.is_empty() { 0 } else { key(uid) };
                let t = parse_ts(ts);
                let ingest = parse_ts(ing);
                if ingest == BAD_TS || ingest == i64::MAX || !(ing.len() == 20) {
                    bail(); // ingest_ts is promised to be valid F1
                }
                let etc = match et {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 255,
                };
                let cat = if pid.is_empty() { 255 } else { *products.get(&key_or_miss(pid)).unwrap_or(&UNKNOWN_CAT) };
                let mut pr: u32 = 0;
                if etc == 2 && all_digits(price) {
                    let v = digits_value(price);
                    if (1..=10_000_000).contains(&v) {
                        pr = v as u32;
                    }
                }
                let ok = uk != 0
                    && t != BAD_TS
                    && t >= ws
                    && t < we
                    && etc != 255
                    && cat != 255
                    && (etc != 2 || pr > 0);
                sh[shard_of(ek)].push(Ev { ek, uk, ts: t, ingest, price: if etc == 2 { pr } else { 0 }, cat, ok });
            });
            sh
        })
        .collect();
    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    lap("events parsed");

    // events: dedup per shard, validate, map person, reshard by person
    let parsed = Arc::new(parsed);
    let survivors: Vec<Vec<Vec<Sv>>> = (0..NS)
        .into_par_iter()
        .map(|s| {
            let total: usize = parsed.iter().map(|c| c[s].len()).sum();
            // keep, per event_id, the copy with the smallest ingest_ts (equal ingest => identical)
            let mut best: AHashMap<u128, Ev> = AHashMap::with_capacity(total);
            for c in parsed.iter() {
                for e in &c[s] {
                    match best.entry(e.ek) {
                        std::collections::hash_map::Entry::Vacant(x) => {
                            x.insert(*e);
                        }
                        std::collections::hash_map::Entry::Occupied(mut x) => {
                            if e.ingest < x.get().ingest {
                                x.insert(*e);
                            }
                        }
                    }
                }
            }
            let guess = total / NS * 5 / 4 + 16;
            let mut out: Vec<Vec<Sv>> = (0..NS).map(|_| Vec::with_capacity(guess)).collect();
            for e in best.values() {
                let e = *e;
                if e.ok && e.ingest - e.ts <= DAY {
                    let pk = pmap(e.uk);
                    out[shard_of(pk)].push(Sv { pk, ts: e.ts, price: e.price, cat: e.cat });
                }
            }
            out
        })
        .collect();
    drop(parsed);
    lap("events deduped");

    // ---------------------------------------------------------- clicks
    let cl_chunks = chunks(&data.join("clicks"));
    let cparsed: Vec<Vec<Vec<Cl>>> = cl_chunks
        .par_iter()
        .map(|c| {
            let guess = (c.end - c.start) / 45 / NS * 5 / 4 + 16;
            let mut sh: Vec<Vec<Cl>> = (0..NS).map(|_| Vec::with_capacity(guess)).collect();
            for_lines(c, |l| {
                let Some([cid, uid, camp, ts]) = fields::<4>(l) else { return };
                if cid.is_empty() {
                    return;
                }
                let ck = key(cid);
                let uk = if uid.is_empty() { 0 } else { key(uid) };
                let cp = if camp.is_empty() { NO_CAMP } else { *campaigns.get(&key_or_miss(camp)).unwrap_or(&NO_CAMP) };
                let t = parse_ts(ts);
                sh[shard_of(ck)].push(Cl { ck, pk: uk, ts: t, camp: cp });
            });
            sh
        })
        .collect();
    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    let cparsed = Arc::new(cparsed);
    let cvalid: Vec<Vec<Vec<Cl>>> = (0..NS)
        .into_par_iter()
        .map(|s| {
            let mut v: Vec<Cl> = Vec::with_capacity(cparsed.iter().map(|c| c[s].len()).sum());
            for c in cparsed.iter() {
                v.extend_from_slice(&c[s]);
            }
            v.sort_unstable_by_key(|c| c.ck);
            let mut out: Vec<Vec<Cl>> = (0..NS).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < v.len() {
                let c = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].ck == c.ck {
                    j += 1;
                }
                if c.pk != 0 && c.ts != BAD_TS && c.ts >= ws - WEEK && c.ts < we && c.camp != NO_CAMP {
                    let pk = pmap(c.pk);
                    out[shard_of(pk)].push(Cl { pk, ..c });
                }
                i = j;
            }
            out
        })
        .collect();
    drop(cparsed);
    lap("clicks");

    // ---------------------------------------------------------- sessions, credit, assists
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = camp_ids.len() + 1;
    let survivors = Arc::new(survivors);
    let cvalid = Arc::new(cvalid);
    let results: Vec<(Vec<UserRow>, Vec<[i64; 4]>)> = (0..NS)
        .into_par_iter()
        .map(|s| {
            let mut ev: Vec<Sv> = Vec::with_capacity(survivors.iter().map(|c| c[s].len()).sum());
            for c in survivors.iter() {
                ev.extend_from_slice(&c[s]);
            }
            ev.sort_unstable_by(|a, b| (a.pk, a.ts).cmp(&(b.pk, b.ts)));
            let mut cl: Vec<Cl> = Vec::with_capacity(cvalid.iter().map(|c| c[s].len()).sum());
            for c in cvalid.iter() {
                cl.extend_from_slice(&c[s]);
            }
            cl.sort_unstable_by(|a, b| (a.pk, a.ts, a.ck).cmp(&(b.pk, b.ts, b.ck)));

            let mut rows: Vec<UserRow> = vec![];
            let mut camp = vec![[0i64; 4]; ncamp * ndays];
            let mut stamp = vec![0u64; ncamp];
            let mut stamp_id: u64 = 0;
            let mut cat_rev = vec![0i64; ncat_slots];
            let mut touched: Vec<u8> = vec![];

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let pk = ev[i].pk;
                let mut j = i;
                while j < ev.len() && ev[j].pk == pk {
                    j += 1;
                }
                while ci < cl.len() && cl[ci].pk < pk {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cl.len() && cl[cj].pk == pk {
                    cj += 1;
                }
                let clicks = &cl[ci..cj];
                let (mut lo, mut hi) = (0usize, 0usize);

                // current (person, day) accumulator
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sessions, mut d_events, mut d_rev) = (0i64, 0i64, 0i64);

                let mut k = i;
                while k < j {
                    // one session: [k, m)
                    let start = ev[k].ts;
                    let mut m = k + 1;
                    while m < j && ev[m].ts - ev[m - 1].ts <= GAP {
                        m += 1;
                    }
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            rows.push(finish_row(pk, cur_day, d_sessions, d_events, d_rev, &mut cat_rev, &mut touched, &rank));
                        }
                        cur_day = day;
                        d_sessions = 0;
                        d_events = 0;
                        d_rev = 0;
                    }
                    let mut rev = 0i64;
                    for e in &ev[k..m] {
                        if e.price > 0 {
                            rev += e.price as i64;
                            let c = e.cat as usize;
                            if cat_rev[c] == 0 {
                                touched.push(e.cat);
                            }
                            cat_rev[c] += e.price as i64;
                        }
                    }
                    let purchased = ev[k..m].iter().any(|e| e.price > 0);
                    d_sessions += 1;
                    d_events += (m - k) as i64;
                    d_rev += rev;

                    // click window [start - 7d, start]
                    while hi < clicks.len() && clicks[hi].ts <= start {
                        hi += 1;
                    }
                    while lo < hi && clicks[lo].ts < start - WEEK {
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credited = if hi > lo {
                        let t = clicks[hi - 1].ts;
                        let mut p = hi - 1;
                        while p > lo && clicks[p - 1].ts == t {
                            p -= 1;
                        }
                        stamp_id += 1;
                        for c in &clicks[lo..hi] {
                            let cc = c.camp as usize;
                            if stamp[cc] != stamp_id {
                                stamp[cc] = stamp_id;
                                camp[cc * ndays + di][3] += 1;
                            }
                        }
                        clicks[p].camp
                    } else {
                        organic
                    };
                    let slot = &mut camp[credited as usize * ndays + di];
                    slot[0] += 1;
                    slot[1] += purchased as i64;
                    slot[2] += rev;
                    k = m;
                }
                if cur_day != i64::MIN {
                    rows.push(finish_row(pk, cur_day, d_sessions, d_events, d_rev, &mut cat_rev, &mut touched, &rank));
                }
                ci = cj;
                i = j;
            }
            (rows, camp)
        })
        .collect();
    drop(survivors);
    drop(cvalid);
    lap("sessions");

    let mut camp_tot = vec![[0i64; 4]; ncamp * ndays];
    for (_, c) in &results {
        for (a, b) in camp_tot.iter_mut().zip(c) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }

    // ---------------------------------------------------------- write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    let names: Vec<Vec<u8>> = (0..256).map(|c| {
        if c == UNKNOWN_CAT as usize { b"unknown".to_vec() } else { cat_names.get(c).cloned().unwrap_or_default() }
    }).collect();

    let groups: Vec<&[(Vec<UserRow>, Vec<[i64; 4]>)]> = results.chunks(NS / 8).collect();
    groups.par_iter().enumerate().for_each(|(g, part)| {
        write_users(&ud.join(format!("part-{g}.parquet")), part.iter().flat_map(|(r, _)| r.iter()), &names, day0);
    });
    write_campaigns(&cd.join("part-0.parquet"), &camp_tot, ndays, day0, &camp_ids, &camp_channel, organic);
    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    lap("written");
}

#[inline]
fn key_or_miss(b: &[u8]) -> u128 {
    // a product or campaign ID too long to be a key can't be in the (short-keyed) table
    if b.len() > 15 { 0 } else { key(b) }
}

#[allow(clippy::too_many_arguments)]
fn finish_row(pk: u128, day: i64, s: i64, e: i64, r: i64, cat_rev: &mut [i64], touched: &mut Vec<u8>, rank: &[u8; 256]) -> UserRow {
    let mut top: Option<u8> = None;
    for &c in touched.iter() {
        let v = cat_rev[c as usize];
        top = match top {
            None => Some(c),
            Some(t) => {
                let tv = cat_rev[t as usize];
                if v > tv || (v == tv && rank[c as usize] < rank[t as usize]) { Some(c) } else { Some(t) }
            }
        };
    }
    for &c in touched.iter() {
        cat_rev[c as usize] = 0;
    }
    touched.clear();
    UserRow { pk, day: day as i32, sessions: s, events: e, revenue: r, top }
}

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build()
}

fn utf8(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or_else(|_| {
        bail();
        ""
    })
}

fn write_users<'a>(path: &Path, rows: impl Iterator<Item = &'a UserRow>, names: &[Vec<u8>], _day0: i64) {
    let rows: Vec<&UserRow> = rows.collect();
    let mut uid = StringBuilder::with_capacity(rows.len(), rows.len() * 10);
    let mut top = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    for r in &rows {
        uid.append_value(utf8(&key_bytes(r.pk)));
        match r.top {
            Some(c) => top.append_value(utf8(&names[c as usize])),
            None => top.append_null(),
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(uid.finish()),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.day))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.sessions))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.events))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.revenue))),
        Arc::new(top.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let f = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn write_campaigns(path: &Path, tot: &[[i64; 4]], ndays: usize, day0: i64, ids: &[Vec<u8>], ch: &[Vec<u8>], organic: u16) {
    let mut cid = StringBuilder::new();
    let mut chn = StringBuilder::new();
    let (mut day, mut s, mut p, mut r, mut a) = (vec![], vec![], vec![], vec![], vec![]);
    for c in 0..=organic as usize {
        for d in 0..ndays {
            let v = tot[c * ndays + d];
            if v[0] == 0 && v[3] == 0 {
                continue;
            }
            if c == organic as usize {
                cid.append_value("organic");
                chn.append_value("organic");
            } else {
                cid.append_value(utf8(&ids[c]));
                chn.append_value(utf8(&ch[c]));
            }
            day.push((day0 + d as i64) as i32);
            s.push(v[0]);
            p.push(v[1]);
            r.push(v[2]);
            a.push(v[3]);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(cid.finish()),
        Arc::new(chn.finish()),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(s)),
        Arc::new(Int64Array::from(p)),
        Arc::new(Int64Array::from(r)),
        Arc::new(Int64Array::from(a)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let f = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

