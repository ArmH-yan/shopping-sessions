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
use std::sync::atomic::Ordering;

const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 4 << 20;
const UNKNOWN_CAT: u8 = 254;
const NO_CAMP: u16 = u16::MAX;

/// The system allocator for ordinary allocations; anything of 4 MB or more gets its own mapping with
/// 2 MB transparent huge pages requested (madvise), so big buffers fault in 512x fewer pages.
/// If THP is off on the host the hint is ignored.
struct Alloc;
const BIG: usize = 4 << 20;
const HUGE: usize = 2 << 20;

unsafe impl std::alloc::GlobalAlloc for Alloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            let n = (l.size() + HUGE - 1) & !(HUGE - 1);
            let p = unsafe { libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) };
            if p == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            unsafe { libc::madvise(p, n, libc::MADV_HUGEPAGE) };
            return p as *mut u8;
        }
        unsafe { std::alloc::System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            return unsafe { self.alloc(l) }; // fresh anonymous mappings are zero
        }
        unsafe { std::alloc::System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        if l.size() >= BIG && l.align() <= 4096 {
            let n = (l.size() + HUGE - 1) & !(HUGE - 1);
            unsafe { libc::munmap(p as *mut libc::c_void, n) };
            return;
        }
        unsafe { std::alloc::System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, new: usize) -> *mut u8 {
        let (old_big, new_big) = (l.size() >= BIG && l.align() <= 4096, new >= BIG && l.align() <= 4096);
        if !old_big && !new_big {
            return unsafe { std::alloc::System.realloc(p, l, new) };
        }
        if old_big && new_big {
            let (on, nn) = ((l.size() + HUGE - 1) & !(HUGE - 1), (new + HUGE - 1) & !(HUGE - 1));
            if nn <= on {
                return p;
            }
            let q = unsafe { libc::mremap(p as *mut libc::c_void, on, nn, libc::MREMAP_MAYMOVE) };
            if q == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            unsafe { libc::madvise(q, nn, libc::MADV_HUGEPAGE) };
            return q as *mut u8;
        }
        let nl = unsafe { std::alloc::Layout::from_size_align_unchecked(new, l.align()) };
        let q = unsafe { self.alloc(nl) };
        if !q.is_null() {
            unsafe { std::ptr::copy_nonoverlapping(p, q, l.size().min(new)) };
            unsafe { self.dealloc(p, l) };
        }
        q
    }
}

#[global_allocator]
static GLOBAL: Alloc = Alloc;


mod parse;
use parse::*;

// ------------------------------------------------------------------ files

enum Src {
    M(Mmap),
    V(Vec<u8>),
}

impl std::ops::Deref for Src {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Src::M(m) => m,
            Src::V(v) => v,
        }
    }
}

struct Chunk {
    map: Arc<Src>,
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
/// Files are mapped (and pre-faulted) in parallel.
fn chunks(dir: &Path) -> Vec<Chunk> {
    let files = csv_files(dir);
    let per_file: Vec<Vec<Chunk>> = files
        .par_iter()
        .map(|p| {
            let mut out = vec![];
            let f = File::open(p).expect("open");
            if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return out;
            }
            let map = Arc::new(if std::env::var("READ").is_ok() {
                Src::V(fs::read(p).expect("read"))
            } else {
                // no populate here: each chunk populates its own pages when a worker takes it,
                // so on a cold page cache the disk reads of all workers overlap
                Src::M(unsafe { memmap2::MmapOptions::new().map(&f).expect("mmap") })
            });
            let data: &[u8] = &map;
            let Some(h) = memchr::memchr(b'\n', data) else { return out };
            let mut s = h + 1;
            while s < data.len() {
                let mut e = (s + CHUNK).min(data.len());
                if e < data.len() {
                    e = match memchr::memchr(b'\n', &data[e..]) {
                        Some(p) => e + p + 1,
                        None => data.len(),
                    };
                }
                out.push(Chunk { map: map.clone(), start: s, end: e });
                s = e;
            }
            out
        })
        .collect();
    per_file.into_iter().flatten().collect()
}

/// Every well-formed record of a chunk as N fields (see `fields`), in one SIMD pass that
/// finds all '\n', ',' and '"' bytes. Malformed and empty lines are skipped.
#[inline]
fn for_records<const N: usize>(c: &Chunk, f: impl FnMut([&[u8]; N])) {
    if let Src::M(m) = &*c.map {
        // Map this chunk's pages in one call (Linux 5.14+; ignored where unsupported) instead
        // of one page fault per 4 KB while scanning.
        const MADV_POPULATE_READ: i32 = 22;
        let base = m.as_ptr() as usize;
        let a = (base + c.start) & !4095;
        let b = base + c.end;
        unsafe { libc::madvise(a as *mut libc::c_void, b - a, MADV_POPULATE_READ) };
    }
    records::<N>(&c.map[c.start..c.end], f)
}

/// Two background threads that walk all input files in processing order (links, events,
/// clicks) issuing readahead(2) in 512 KB steps, so a cold page cache is filled from disk
/// ahead of (and alongside) the parsing workers. On a warm cache these calls return at once.
fn start_readahead(data: &Path) {
    let mut files: Vec<PathBuf> = vec![];
    for d in ["links", "events", "clicks"] {
        files.extend(csv_files(&data.join(d)));
    }
    let files = Arc::new(files);
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    const STEP: usize = 512 << 10;
    for _ in 0..2 {
        let (files, next) = (files.clone(), next.clone());
        std::thread::spawn(move || {
            use std::os::unix::io::AsRawFd;
            let opened: Vec<Option<(File, usize)>> = files
                .iter()
                .map(|p| File::open(p).ok().and_then(|f| f.metadata().ok().map(|m| (f, m.len() as usize))))
                .collect();
            // a global sequence of (file, block), claimed one block at a time
            let blocks: Vec<(usize, usize)> = opened
                .iter()
                .enumerate()
                .flat_map(|(i, o)| {
                    let n = o.as_ref().map(|x| x.1).unwrap_or(0);
                    (0..n.div_ceil(STEP)).map(move |b| (i, b * STEP))
                })
                .collect();
            loop {
                let k = next.fetch_add(1, Ordering::Relaxed);
                let Some(&(i, off)) = blocks.get(k) else { break };
                if let Some((f, _)) = &opened[i] {
                    unsafe { libc::readahead(f.as_raw_fd(), off as i64, STEP) };
                }
            }
        });
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

/// One parsed copy of an event (32 bytes). `ok`: this copy is valid and not late.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Ev {
    uk: u64,
    ek: u64,
    ingest: i32, // seconds from window_start
    ts: u32,     // ms from window_start (valid copies only)
    price: u32,
    cat: u8,
    ok: bool,
}

/// A surviving event (20 bytes).
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Sv {
    pk: u64,
    ts: u32,    // ms from window_start
    price: u32, // 0 = not a purchase
    cat: u8,
}

/// A click (28 bytes). IDs are `code64` codes.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Cl {
    ck: u64,
    pk: u64, // user code at parse time, person code after mapping
    ts: i64,
    camp: u16,
}

struct UserRow {
    pk: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u8>,
}


/// Per-thread sets of `NB` buckets: each rayon worker appends to its own buckets, so there is
/// no per-chunk allocation and nothing to concatenate; a bucket is read across all threads.
const NB: usize = 1024;

#[inline]
fn bucket_of(x: u64) -> usize {
    (x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 54) as usize
}

struct Buckets<T> {
    v: Vec<std::sync::Mutex<Vec<Vec<T>>>>,
}

impl<T: Send> Buckets<T> {
    fn new(cap: usize) -> Self {
        let nt = rayon::current_num_threads().max(1);
        Buckets {
            v: (0..nt).map(|_| std::sync::Mutex::new((0..NB).map(|_| Vec::with_capacity(cap)).collect())).collect(),
        }
    }
    fn with<R>(&self, f: impl FnOnce(&mut Vec<Vec<T>>) -> R) -> R {
        let i = rayon::current_thread_index().unwrap_or(0) % self.v.len();
        f(&mut self.v[i].lock().unwrap())
    }
    fn into_inner(self) -> Vec<Vec<Vec<T>>> {
        self.v.into_iter().map(|m| m.into_inner().unwrap()).collect()
    }
}

/// user code -> person code, open addressing with 16-byte slots; IDs not in it are their own
/// person. Lookups come in batches with software prefetch, since the table is far bigger than
/// the caches.
struct PTab {
    slots: Vec<(u64, u64)>,
    mask: usize,
}

impl PTab {
    fn new(pairs: &[(u64, u64)]) -> Self {
        let cap = (pairs.len() * 2).next_power_of_two().max(16);
        // filled by real writes, not a zeroed allocation: probing reads untouched pages, which
        // would map the shared zero page and cost a TLB flush on every CPU at the first write
        let mut slots: Vec<(u64, u64)> = Vec::with_capacity(cap);
        slots.resize(cap, (0, 0));
        let mask = cap - 1;
        for &(k, v) in pairs {
            let mut i = Self::home(k, mask);
            while slots[i].0 != 0 && slots[i].0 != k {
                i = (i + 1) & mask;
            }
            slots[i] = (k, v);
        }
        PTab { slots, mask }
    }
    #[inline(always)]
    fn home(k: u64, mask: usize) -> usize {
        (k.wrapping_mul(0xA24B_AED4_963E_E407) >> 24) as usize & mask
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let p = self.slots.as_ptr().add(Self::home(k, self.mask)) as *const i8;
            std::arch::x86_64::_mm_prefetch(p, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut i = Self::home(k, self.mask);
        loop {
            let (a, b) = self.slots[i];
            if a == k {
                return b;
            }
            if a == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Open-addressing table of (key, ingest, location), reused by each thread across buckets:
/// clearing warm memory is much cheaper than faulting in fresh pages.
#[derive(Clone, Copy, Default)]
struct Slot {
    key: u64,
    ingest: i32,
    loc: u32, // part << 26 | index in that part's bucket
}

thread_local! {
    static TABLE: std::cell::RefCell<Vec<Slot>> = const { std::cell::RefCell::new(Vec::new()) };
    static SCRATCH: std::cell::RefCell<Group> = std::cell::RefCell::new(Group::default());
}

/// Per-thread scratch for grouping one bucket by person.
#[derive(Default)]
struct Group {
    ev: Vec<Sv>,
    cl: Vec<Cl>,
    tab: Vec<(u64, u32)>, // person code -> dense id + 1
    dense: Vec<u32>,      // dense id of each event
    start: Vec<u32>,      // first event of each person, then one past the last
    cstart: Vec<u32>,     // same for clicks
    ev_out: Vec<Sv>,
    cl_out: Vec<Cl>,
    persons: Vec<u64>,
}

impl Group {
    /// Groups the bucket's events and clicks by person (counting passes, no global sort) and
    /// sorts each person's events and clicks by ts.
    fn build(&mut self) {
        let n = self.ev.len();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        self.tab.clear();
        self.tab.resize(cap, (0, 0));
        self.persons.clear();
        self.dense.clear();
        let mut counts: Vec<u32> = std::mem::take(&mut self.start);
        counts.clear();
        for e in &self.ev {
            let k = e.pk;
            let mut i = (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 24) as usize & mask;
            let d = loop {
                let sl = &mut self.tab[i];
                if sl.0 == k {
                    break sl.1 - 1;
                }
                if sl.0 == 0 {
                    let d = self.persons.len() as u32;
                    *sl = (k, d + 1);
                    self.persons.push(k);
                    counts.push(0);
                    break d;
                }
                i = (i + 1) & mask;
            };
            counts[d as usize] += 1;
            self.dense.push(d);
        }
        let np = self.persons.len();
        // counts -> start offsets (np + 1 entries)
        let mut acc = 0u32;
        for c in counts.iter_mut() {
            let x = *c;
            *c = acc;
            acc += x;
        }
        counts.push(acc);
        self.ev_out.clear();
        self.ev_out.reserve(n);
        unsafe { self.ev_out.set_len(n) };
        {
            let mut pos: Vec<u32> = counts[..np].to_vec();
            for (e, &d) in self.ev.iter().zip(&self.dense) {
                let p = &mut pos[d as usize];
                self.ev_out[*p as usize] = *e;
                *p += 1;
            }
        }
        for d in 0..np {
            let g = &mut self.ev_out[counts[d] as usize..counts[d + 1] as usize];
            if g.len() > 1 {
                if g.len() <= 24 {
                    for a in 1..g.len() {
                        let x = g[a];
                        let mut b = a;
                        while b > 0 && { g[b - 1].ts } > { x.ts } {
                            g[b] = g[b - 1];
                            b -= 1;
                        }
                        g[b] = x;
                    }
                } else {
                    g.sort_unstable_by_key(|e| e.ts);
                }
            }
        }
        self.start = counts;
        // clicks: only persons with events matter
        let mut ccount = std::mem::take(&mut self.cstart);
        ccount.clear();
        ccount.resize(np + 1, 0);
        self.dense.clear();
        for c in &self.cl {
            let k = c.pk;
            let mut i = (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 24) as usize & mask;
            let d = loop {
                let sl = self.tab[i];
                if sl.0 == k {
                    break sl.1 - 1;
                }
                if sl.0 == 0 {
                    break u32::MAX;
                }
                i = (i + 1) & mask;
            };
            self.dense.push(d);
            if d != u32::MAX {
                ccount[d as usize] += 1;
            }
        }
        let mut acc = 0u32;
        for c in ccount.iter_mut() {
            let x = *c;
            *c = acc;
            acc += x;
        }
        let total = acc as usize;
        self.cl_out.clear();
        self.cl_out.reserve(total);
        unsafe { self.cl_out.set_len(total) };
        {
            let mut pos: Vec<u32> = ccount[..np].to_vec();
            for (c, &d) in self.cl.iter().zip(&self.dense) {
                if d != u32::MAX {
                    let p = &mut pos[d as usize];
                    self.cl_out[*p as usize] = *c;
                    *p += 1;
                }
            }
        }
        for d in 0..np {
            let g = &mut self.cl_out[ccount[d] as usize..ccount[d + 1] as usize];
            if g.len() > 1 {
                g.sort_unstable_by_key(|c| c.ts);
            }
        }
        self.cstart = ccount;
    }
}

/// Calls `keep(&T)` once per distinct key over all threads' bucket `b`, choosing the element
/// with the smallest `rank` (first seen on ties; callers only tie on identical copies).
fn dedup_bucket<T: Copy>(parts: &[Vec<Vec<T>>], b: usize, key: impl Fn(&T) -> u64, rank: impl Fn(&T) -> i32, mut keep: impl FnMut(&T)) {
    let n: usize = parts.iter().map(|t| t[b].len()).sum();
    if n == 0 {
        return;
    }
    if parts.len() > 63 || parts.iter().any(|t| t[b].len() >= 1 << 26) {
        bail(); // locations would not fit; the fallback pipeline takes over
        return;
    }
    TABLE.with(|t| {
        let mut tab = t.borrow_mut();
        let cap = (n * 3 / 2).next_power_of_two().max(16);
        if tab.len() < cap {
            tab.resize(cap, Slot::default());
        }
        let tab = &mut tab[..cap];
        tab.fill(Slot::default());
        let mask = cap - 1;
        for (pi, part) in parts.iter().enumerate() {
            for (ei, e) in part[b].iter().enumerate() {
                let k = key(e);
                let r = rank(e);
                let loc = ((pi as u32) << 26) | ei as u32;
                let mut i = (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 20) as usize & mask;
                loop {
                    let sl = &mut tab[i];
                    if sl.key == 0 {
                        *sl = Slot { key: k, ingest: r, loc };
                        break;
                    }
                    if sl.key == k {
                        if r < sl.ingest {
                            sl.ingest = r;
                            sl.loc = loc;
                        }
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
        }
        for sl in tab.iter() {
            if sl.key != 0 {
                keep(&parts[(sl.loc >> 26) as usize][b][(sl.loc & ((1 << 26) - 1)) as usize]);
            }
        }
    });
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
    let cpu = || -> f64 {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        tv(ru.ru_utime) + tv(ru.ru_stime)
    };
    let last = std::cell::Cell::new((0.0f64, 0.0f64));
    let lap = |what: &str| {
        if verbose {
            let (w, c) = (t0.elapsed().as_secs_f64(), cpu());
            let (lw, lc) = last.get();
            eprintln!("{:7.3}s wall {:7.3}s cpu  {}", w - lw, c - lc, what);
            last.set((w, c));
        }
    };

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).expect("manifest")).expect("json");
    if std::env::var_os("NO_READAHEAD").is_none() {
        start_readahead(&data);
    }
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
    // Fast path for IDs of the form p<digits> with no leading zero: a direct table.
    let canon = |p: &[u8]| -> Option<usize> {
        if p.len() >= 2 && p.len() <= 8 && p[0] == b'p' && p[1] != b'0' && p[1..].iter().all(|c| c.is_ascii_digit()) {
            Some(p[1..].iter().fold(0usize, |v, &c| v * 10 + (c - b'0') as usize))
        } else {
            None
        }
    };
    let mut pdirect: Vec<u8> = vec![];
    for (&k, &c) in &products {
        if let Some(n) = canon(&key_bytes(k)) {
            if n < 4_000_000 {
                if pdirect.len() <= n {
                    pdirect.resize(n + 1, UNKNOWN_CAT);
                }
                pdirect[n] = c;
            }
        }
    }
    let product_cat = |p: &[u8]| -> u8 {
        if let Some(n) = canon(p) {
            return if n < pdirect.len() { pdirect[n] } else { UNKNOWN_CAT };
        }
        *products.get(&key_or_miss(p)).unwrap_or(&UNKNOWN_CAT)
    };
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

    let links_job = || -> PTab {
    // ---------------------------------------------------------- links -> person names
    let link_chunks = chunks(&data.join("links"));
    let link_pairs: Vec<(u128, u128)> = link_chunks
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = vec![];
            for_records::<2>(c, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    v.push((key(a), key(b)));
                }
            });
            v
        })
        .collect();
    // IDs sorted byte-wise: index order = name order, so a root that is always the smaller
    // index is the person's name.
    let mut slots: Vec<(u128, u32)> = link_pairs
        .par_iter()
        .enumerate()
        .flat_map_iter(|(i, &(a, b))| [(a, 2 * i as u32), (b, 2 * i as u32 + 1)])
        .collect();
    slots.par_sort_unstable();
    let mut ids: Vec<u128> = Vec::with_capacity(slots.len() / 2);
    let mut side: Vec<u32> = vec![0; slots.len()];
    for &(k, slot) in &slots {
        if ids.last() != Some(&k) {
            ids.push(k);
        }
        side[slot as usize] = ids.len() as u32 - 1;
    }
    std::mem::forget(slots);
    let pairs: Vec<(u32, u32)> = side.chunks_exact(2).map(|c| (c[0], c[1])).collect();
    std::mem::forget(side);
    std::mem::forget(link_pairs);
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
    // Names were chosen on byte-ordered u128 keys; the rest of the pipeline uses code64 codes.
    let codes: Vec<u64> = ids.par_iter().map(|&k| code64(&key_bytes(k))).collect();
    let pairs: Vec<(u64, u64)> = (0..codes.len()).map(|i| (codes[i], codes[parent[i] as usize])).collect();
    let person = PTab::new(&pairs);
    std::mem::forget(pairs);
        person
    };
    let events_job = || -> Vec<Vec<Vec<Ev>>> {
    // ---------------------------------------------------------- events: parse
    let ev_chunks = chunks(&data.join("events"));
    let bytes: usize = ev_chunks.iter().map(|c| c.end - c.start).sum();
    let evb: Buckets<Ev> = Buckets::new(bytes / 70 / NB / rayon::current_num_threads().max(1) * 5 / 4 + 16);
    ev_chunks.par_iter().for_each(|c| {
        evb.with(|sh| {
            let mut tsp = TsParser::default();
            for_records::<7>(c, |[eid, uid, ts, ing, et, pid, price]| {
                if eid.is_empty() {
                    return;
                }
                let ek = code64(eid);
                let uk = if uid.is_empty() { 0 } else { code64(uid) };
                let t = tsp.parse(ts);
                let ingest = tsp.parse(ing);
                if ingest == BAD_TS || ingest == i64::MAX || !(ing.len() == 20) {
                    bail(); // ingest_ts is promised to be valid F1
                }
                let etc = match et {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 255,
                };
                let cat = if pid.is_empty() { 255 } else { product_cat(pid) };
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
                    && (etc != 2 || pr > 0)
                    && ingest - t <= DAY;
                let ing_rel = ingest.div_euclid(1000) - ws.div_euclid(1000);
                if ing_rel < i32::MIN as i64 || ing_rel > i32::MAX as i64 {
                    bail();
                }
                sh[bucket_of(ek)].push(Ev {
                    uk,
                    ek,
                    ingest: ing_rel as i32,
                    ts: if ok { (t - ws) as u32 } else { 0 },
                    price: if etc == 2 { pr } else { 0 },
                    cat,
                    ok,
                });
            });
        })
    });
    evb.into_inner()
    };
    // Links do not depend on events, so both run at once.
    let (person, parsed) = rayon::join(links_job, events_job);
    lap("links + events parsed");


    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    lap("events parsed");

    // events: dedup per bucket (earliest ingest_ts wins), keep valid copies, move to person buckets
    let nt = rayon::current_num_threads().max(1);
    let n_ev: usize = parsed.iter().flatten().map(|b| b.len()).sum();
    let sv: Buckets<Sv> = Buckets::new(n_ev / NB / nt * 5 / 4 + 16);
    (0..NB).into_par_iter().for_each(|b| {
        sv.with(|out| {
            let mut kept: Vec<Ev> = Vec::with_capacity(4096);
            dedup_bucket(&parsed, b, |e: &Ev| e.ek, |e: &Ev| e.ingest, |e: &Ev| {
                if e.ok {
                    kept.push(*e);
                }
            });
            const PF: usize = 16;
            for i in 0..kept.len() {
                if i + PF < kept.len() {
                    person.prefetch(kept[i + PF].uk);
                }
                let e = kept[i];
                let pk = person.get(e.uk);
                out[bucket_of(pk)].push(Sv { pk, ts: e.ts, price: e.price, cat: e.cat });
            }
        })
    });
    std::mem::forget(parsed);
    let survivors = sv.into_inner();
    lap("events deduped");

    // ---------------------------------------------------------- clicks
    let cl_chunks = chunks(&data.join("clicks"));
    lap("  clicks mapped");
    let cb: Buckets<Cl> = Buckets::new(cl_chunks.iter().map(|c| c.end - c.start).sum::<usize>() / 45 / NB / nt * 5 / 4 + 16);
    cl_chunks.par_iter().for_each(|c| {
        cb.with(|sh| {
            let mut tsp = TsParser::default();
            for_records::<4>(c, |[cid, uid, camp, ts]| {
                if cid.is_empty() {
                    return;
                }
                let ck = code64(cid);
                let uk = if uid.is_empty() { 0 } else { code64(uid) };
                let cp = if camp.is_empty() { NO_CAMP } else { *campaigns.get(&key_or_miss(camp)).unwrap_or(&NO_CAMP) };
                let t = tsp.parse(ts);
                sh[bucket_of(ck)].push(Cl { ck, pk: uk, ts: t, camp: cp });
            });
        })
    });
    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    let cparsed = cb.into_inner();
    lap("  clicks parsed");
    let n_cl: usize = cparsed.iter().flatten().map(|b| b.len()).sum();
    let cvb: Buckets<Cl> = Buckets::new(n_cl / NB / nt * 5 / 4 + 16);
    (0..NB).into_par_iter().for_each(|b| {
        cvb.with(|out| {
            // copies of a click are identical: keep any one
            let mut kept: Vec<Cl> = Vec::with_capacity(1024);
            dedup_bucket(&cparsed, b, |c: &Cl| c.ck, |_| 0, |&c| {
                if c.pk != 0 && c.ts != BAD_TS && c.ts >= ws - WEEK && c.ts < we && c.camp != NO_CAMP {
                    kept.push(c);
                }
            });
            const PF: usize = 16;
            for i in 0..kept.len() {
                if i + PF < kept.len() {
                    person.prefetch(kept[i + PF].pk);
                }
                let c = kept[i];
                let pk = person.get(c.pk);
                out[bucket_of(pk)].push(Cl { pk, ..c });
            }
        })
    });
    std::mem::forget(cparsed);
    let cvalid = cvb.into_inner();
    lap("clicks");

    // ---------------------------------------------------------- sessions, credit, assists
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = camp_ids.len() + 1;
    struct Acc {
        rows: Vec<UserRow>,
        camp: Vec<[i64; 4]>,
        stamp: Vec<u64>,
        stamp_id: u64,
        cat_rev: Vec<i64>,
        touched: Vec<u8>,
    }
    let accs: Vec<std::sync::Mutex<Acc>> = (0..nt)
        .map(|_| {
            std::sync::Mutex::new(Acc {
                rows: Vec::with_capacity(n_ev / 8 / nt + 16),
                camp: {
                    let mut v = Vec::with_capacity(ncamp * ndays);
                    v.resize(ncamp * ndays, [0i64; 4]); // real writes (see PTab::new)
                    v
                },
                stamp: vec![0u64; ncamp],
                stamp_id: 0,
                cat_rev: vec![0i64; ncat_slots],
                touched: vec![],
            })
        })
        .collect();
    (0..NB).into_par_iter().for_each(|s| {
        let me = rayon::current_thread_index().unwrap_or(0) % nt;
        let mut acc = accs[me].lock().unwrap();
        let Acc { rows, camp, stamp, stamp_id, cat_rev, touched } = &mut *acc;
        SCRATCH.with(|sc| {
            let mut g = sc.borrow_mut();
            g.ev.clear();
            for t in survivors.iter() {
                g.ev.extend_from_slice(&t[s]);
            }
            g.cl.clear();
            for t in cvalid.iter() {
                g.cl.extend_from_slice(&t[s]);
            }
            g.build();
            let g = &*g;
            for d in 0..g.persons.len() {
                let pk = g.persons[d];
                let ev = &g.ev_out[g.start[d] as usize..g.start[d + 1] as usize];
                let clicks = &g.cl_out[g.cstart[d] as usize..g.cstart[d + 1] as usize];
                let (i, j) = (0usize, ev.len());
                let (mut lo, mut hi) = (0usize, 0usize);

                // current (person, day) accumulator
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sessions, mut d_events, mut d_rev) = (0i64, 0i64, 0i64);

                let mut k = i;
                while k < j {
                    // one session: [k, m)
                    let start = ws + ev[k].ts as i64;
                    let mut m = k + 1;
                    while m < j && (ev[m].ts - ev[m - 1].ts) as i64 <= GAP {
                        m += 1;
                    }
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            rows.push(finish_row(pk, cur_day, d_sessions, d_events, d_rev, cat_rev, touched, &rank));
                        }
                        cur_day = day;
                        d_sessions = 0;
                        d_events = 0;
                        d_rev = 0;
                    }
                    let mut rev = 0i64;
                    let mut purchased = false;
                    for e in &ev[k..m] {
                        if e.price > 0 {
                            purchased = true;
                            rev += e.price as i64;
                            let c = e.cat as usize;
                            if cat_rev[c] == 0 {
                                touched.push(e.cat);
                            }
                            cat_rev[c] += e.price as i64;
                        }
                    }
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
                        while p > lo && { clicks[p - 1].ts } == t {
                            p -= 1;
                        }
                        if p + 1 < hi {
                            // several clicks at the latest ts: the byte-wise smallest click_id
                            p = (p..hi).min_by(|&a, &b| decode64(clicks[a].ck).cmp(&decode64(clicks[b].ck))).unwrap();
                        }
                        *stamp_id += 1;
                        for c in &clicks[lo..hi] {
                            let cc = c.camp as usize;
                            if stamp[cc] != *stamp_id {
                                stamp[cc] = *stamp_id;
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
                    rows.push(finish_row(pk, cur_day, d_sessions, d_events, d_rev, cat_rev, touched, &rank));
                }
            }
        });
    });
    std::mem::forget(survivors);
    std::mem::forget(cvalid);
    let accs: Vec<Acc> = accs.into_iter().map(|m| m.into_inner().unwrap()).collect();
    lap("sessions");

    let mut camp_tot = vec![[0i64; 4]; ncamp * ndays];
    for a in &accs {
        for (x, y) in camp_tot.iter_mut().zip(&a.camp) {
            for z in 0..4 {
                x[z] += y[z];
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

    accs.par_iter().enumerate().for_each(|(g, a)| {
        write_users(&ud.join(format!("part-{g}.parquet")), a.rows.iter(), &names, day0);
    });
    write_campaigns(&cd.join("part-0.parquet"), &camp_tot, ndays, day0, &camp_ids, &camp_channel, organic);
    if BAIL.load(Ordering::Relaxed) {
        std::process::exit(3);
    }
    lap("written");
    // Every file is written and closed; skip tearing down the heap and the thread pool.
    unsafe { libc::_exit(0) }
}

#[inline]
fn key_or_miss(b: &[u8]) -> u128 {
    // a product or campaign ID too long to be a key can't be in the (short-keyed) table
    if b.len() > 15 { 0 } else { key(b) }
}

#[allow(clippy::too_many_arguments)]
fn finish_row(pk: u64, day: i64, s: i64, e: i64, r: i64, cat_rev: &mut [i64], touched: &mut Vec<u8>, rank: &[u8; 256]) -> UserRow {
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
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    for r in &rows {
        buf.clear();
        decode64_into(r.pk, &mut buf);
        uid.append_value(utf8(&buf));
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

