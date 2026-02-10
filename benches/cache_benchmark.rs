use arc_swap::ArcSwapOption;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Credentials {
    token: String,
    expiry: Instant,
}

// Arc-swap based cache implementation
#[derive(Debug)]
struct ArcSwapCache {
    cache: ArcSwapOption<Credentials>,
}

impl ArcSwapCache {
    fn new() -> Self {
        Self {
            cache: ArcSwapOption::new(None),
        }
    }

    fn get(&self) -> Option<Credentials> {
        self.cache.load_full().as_ref().map(|c| (**c).clone())
    }

    fn update(&self, creds: Credentials) {
        self.cache.store(Some(Arc::new(creds)));
    }
}

// RwLock based cache implementation
#[derive(Debug)]
struct RwLockCache {
    cache: RwLock<Option<Credentials>>,
}

impl RwLockCache {
    fn new() -> Self {
        Self {
            cache: RwLock::new(None),
        }
    }

    async fn get(&self) -> Option<Credentials> {
        self.cache.read().await.clone()
    }

    async fn update(&self, creds: Credentials) {
        *self.cache.write().await = Some(creds);
    }
}

// Mutex based cache implementation (original problematic approach)
#[derive(Debug)]
struct MutexCache {
    cache: Mutex<Option<Credentials>>,
}

impl MutexCache {
    fn new() -> Self {
        Self {
            cache: Mutex::new(None),
        }
    }

    async fn get(&self) -> Option<Credentials> {
        self.cache.lock().await.clone()
    }

    async fn update(&self, creds: Credentials) {
        *self.cache.lock().await = Some(creds);
    }
}

fn generate_delay(jitter_us: (u64, u64)) -> u64 {
    let mut rng = rand::rng();
    rng.random_range(jitter_us.0..=jitter_us.1)
}

async fn simulate_request(jitter_us: (u64, u64)) {
    let delay = generate_delay(jitter_us);
    tokio::time::sleep(Duration::from_micros(delay)).await;
}

async fn run_arc_swap_benchmark(
    num_requests: usize,
    concurrent_tasks: usize,
    jitter_us: (u64, u64),
) -> (Duration, Vec<Duration>) {
    let cache = Arc::new(ArcSwapCache::new());

    // Initialize cache with a very long TTL (no expiry during test = uncontended)
    cache.update(Credentials {
        token: "initial_token".to_string(),
        expiry: Instant::now() + Duration::from_secs(86400), // 24 hours
    });

    // No background updater - simulating uncontended case where TTL never expires

    // Run benchmark
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(num_requests * concurrent_tasks);
    let requests_per_task = num_requests; // Each task does this many requests

    let mut tasks = Vec::new();
    for _ in 0..concurrent_tasks {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            let mut task_latencies = Vec::with_capacity(requests_per_task);
            for _ in 0..requests_per_task {
                let req_start = Instant::now();

                // Get credentials (lock-free read, no TTL check/refresh needed)
                let _creds = cache.get();

                // Simulate HTTP request work
                simulate_request(jitter_us).await;

                task_latencies.push(req_start.elapsed());
            }
            task_latencies
        }));
    }

    // Collect all latencies
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }

    let total_duration = start.elapsed();

    (total_duration, latencies)
}

async fn run_rwlock_benchmark(
    num_requests: usize,
    concurrent_tasks: usize,
    jitter_us: (u64, u64),
) -> (Duration, Vec<Duration>) {
    let cache = Arc::new(RwLockCache::new());

    // Initialize cache with a very long TTL (no expiry during test = uncontended)
    cache
        .update(Credentials {
            token: "initial_token".to_string(),
            expiry: Instant::now() + Duration::from_secs(86400), // 24 hours
        })
        .await;

    // No background updater - simulating uncontended case where TTL never expires

    // Run benchmark
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(num_requests * concurrent_tasks);
    let requests_per_task = num_requests; // Each task does this many requests

    let mut tasks = Vec::new();
    for _ in 0..concurrent_tasks {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            let mut task_latencies = Vec::with_capacity(requests_per_task);
            for _ in 0..requests_per_task {
                let req_start = Instant::now();

                // Get credentials (requires read lock acquisition even though no writes)
                let _creds = cache.get().await;

                // Simulate HTTP request work
                simulate_request(jitter_us).await;

                task_latencies.push(req_start.elapsed());
            }
            task_latencies
        }));
    }

    // Collect all latencies
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }

    let total_duration = start.elapsed();

    (total_duration, latencies)
}

async fn run_mutex_benchmark(
    num_requests: usize,
    concurrent_tasks: usize,
    jitter_us: (u64, u64),
) -> (Duration, Vec<Duration>) {
    let cache = Arc::new(MutexCache::new());

    // Initialize cache with a very long TTL (no expiry during test = uncontended)
    cache
        .update(Credentials {
            token: "initial_token".to_string(),
            expiry: Instant::now() + Duration::from_secs(86400), // 24 hours
        })
        .await;

    // No background updater - simulating uncontended case where TTL never expires

    // Run benchmark
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(num_requests * concurrent_tasks);
    let requests_per_task = num_requests; // Each task does this many requests

    let mut tasks = Vec::new();
    for _ in 0..concurrent_tasks {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            let mut task_latencies = Vec::with_capacity(requests_per_task);
            for _ in 0..requests_per_task {
                let req_start = Instant::now();

                // Get credentials (requires exclusive mutex lock even for reads)
                let _creds = cache.get().await;

                // Simulate HTTP request work
                simulate_request(jitter_us).await;

                task_latencies.push(req_start.elapsed());
            }
            task_latencies
        }));
    }

    // Collect all latencies
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }

    let total_duration = start.elapsed();

    (total_duration, latencies)
}

fn calculate_percentiles(mut latencies: Vec<Duration>) -> (Duration, Duration, Duration, Duration) {
    if latencies.is_empty() {
        eprintln!("Warning: No latencies collected!");
        return (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
    }

    latencies.sort();
    let len = latencies.len();

    let p50 = latencies[len / 2];
    let p95 = latencies[len * 95 / 100];
    let p99 = latencies[len * 99 / 100];
    let p999 = latencies[len * 999 / 1000];

    (p50, p95, p99, p999)
}

#[derive(Debug)]
struct BenchmarkResult {
    concurrent_tasks: usize,
    mutex_p50: Duration,
    mutex_p99: Duration,
    arc_p50: Duration,
    arc_p99: Duration,
    rwlock_p50: Duration,
    rwlock_p99: Duration,
}

async fn run_benchmark_for_concurrency(
    num_requests: usize,
    concurrent_tasks: usize,
    jitter_us: (u64, u64),
) -> BenchmarkResult {
    println!(
        "\n=== Testing with {} concurrent readers ===",
        concurrent_tasks
    );

    // Mutex
    println!("  Running Mutex...");
    let (_, mutex_latencies) = run_mutex_benchmark(num_requests, concurrent_tasks, jitter_us).await;
    let (mutex_p50, _, mutex_p99, _) = calculate_percentiles(mutex_latencies);

    // Arc-swap
    println!("  Running Arc-swap...");
    let (_, arc_latencies) =
        run_arc_swap_benchmark(num_requests, concurrent_tasks, jitter_us).await;
    let (arc_p50, _, arc_p99, _) = calculate_percentiles(arc_latencies);

    // RwLock
    println!("  Running RwLock...");
    let (_, rwlock_latencies) =
        run_rwlock_benchmark(num_requests, concurrent_tasks, jitter_us).await;
    let (rwlock_p50, _, rwlock_p99, _) = calculate_percentiles(rwlock_latencies);

    BenchmarkResult {
        concurrent_tasks,
        mutex_p50,
        mutex_p99,
        arc_p50,
        arc_p99,
        rwlock_p50,
        rwlock_p99,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("=== Credentials Cache Benchmark ===");
    println!("Simulated HTTP request workload with varying concurrency");
    println!("(Uncontended: TTL never expires, no writes during benchmark)\n");

    let num_requests = 200; // Requests per task
    let jitter_us = (1000, 2000); // 4-12ms jitter for simulated HTTP requests (8ms ± 4ms)
    let concurrency_levels = vec![100, 500, 5_000, 5_000, 10_000, 25_000]; // Number of concurrent tasks

    println!("Configuration:");
    println!("  Requests per task: {}", num_requests);
    println!(
        "  Request jitter: {}-{}ms (simulated HTTP work)",
        jitter_us.0, jitter_us.1
    );
    println!("  Concurrency levels: {:?}", concurrency_levels);
    println!("  Runtime: Multi-threaded");
    println!("  Scenario: All reads, no credential refreshes (uncontended)\n");

    // Warmup
    println!("Running warmup...");
    let _ = run_mutex_benchmark(100, 10, jitter_us).await;
    let _ = run_arc_swap_benchmark(100, 10, jitter_us).await;
    let _ = run_rwlock_benchmark(100, 10, jitter_us).await;

    // Run benchmarks for each concurrency level
    let mut results = Vec::new();
    for &concurrent_tasks in &concurrency_levels {
        let result = run_benchmark_for_concurrency(num_requests, concurrent_tasks, jitter_us).await;
        results.push(result);
    }

    // Print results table
    println!("\n\n=== Results Summary ===\n");

    println!("Median Latency (p50):");
    println!(
        "{:<12} │ {:>10} │ {:>10} │ {:>10} │ {:>12} │ {:>14}",
        "Concurrency", "Mutex", "Arc-swap", "RwLock", "Arc vs Mutex", "RwLock vs Mutex"
    );
    println!("{}", "─".repeat(92));

    for result in &results {
        let arc_improvement =
            (result.mutex_p50.as_secs_f64() / result.arc_p50.as_secs_f64() - 1.0) * 100.0;
        let rwlock_improvement =
            (result.mutex_p50.as_secs_f64() / result.rwlock_p50.as_secs_f64() - 1.0) * 100.0;

        let concurrency_label = if result.concurrent_tasks >= 1000 {
            format!("{}k", result.concurrent_tasks / 1000)
        } else {
            result.concurrent_tasks.to_string()
        };
        println!(
            "{:<12} │ {:>8.2}ms │ {:>8.2}ms │ {:>8.2}ms │ {:>11.1}% │ {:>13.1}%",
            concurrency_label,
            result.mutex_p50.as_secs_f64() * 1000.0,
            result.arc_p50.as_secs_f64() * 1000.0,
            result.rwlock_p50.as_secs_f64() * 1000.0,
            arc_improvement,
            rwlock_improvement
        );
    }

    println!("\n\nTail Latency (p99):");
    println!(
        "{:<12} │ {:>10} │ {:>10} │ {:>10} │ {:>12} │ {:>14}",
        "Concurrency", "Mutex", "Arc-swap", "RwLock", "Arc vs Mutex", "RwLock vs Mutex"
    );
    println!("{}", "─".repeat(92));

    for result in &results {
        let arc_improvement =
            (result.mutex_p99.as_secs_f64() / result.arc_p99.as_secs_f64() - 1.0) * 100.0;
        let rwlock_improvement =
            (result.mutex_p99.as_secs_f64() / result.rwlock_p99.as_secs_f64() - 1.0) * 100.0;

        let concurrency_label = if result.concurrent_tasks >= 1000 {
            format!("{}k", result.concurrent_tasks / 1000)
        } else {
            result.concurrent_tasks.to_string()
        };
        println!(
            "{:<12} │ {:>8.2}ms │ {:>8.2}ms │ {:>8.2}ms │ {:>11.1}% │ {:>13.1}%",
            concurrency_label,
            result.mutex_p99.as_secs_f64() * 1000.0,
            result.arc_p99.as_secs_f64() * 1000.0,
            result.rwlock_p99.as_secs_f64() * 1000.0,
            arc_improvement,
            rwlock_improvement
        );
    }

    println!("\n\nKey Findings:");
    println!("  - Positive % = improvement over Mutex (faster)");
    println!("  - Negative % = regression vs Mutex (slower)");
}
