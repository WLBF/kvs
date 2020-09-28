use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const KEY_LENGTH: usize = 64;
const WORKLOAD_SIZE: usize = 1000;
const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

fn rand_string() -> String {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(1, 100001);
    rng.sample_iter(&Alphanumeric).take(len).collect()
}

fn gen_read_keys(data_map: &HashMap<String, String>) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let keys = data_map.keys().cloned().collect::<Vec<String>>();
    let mut read_keys = vec![];
    for _ in 0..1000 {
        let i = rng.gen_range(0, keys.len());
        read_keys.push(keys[i].clone());
    }
    read_keys
}

fn gen_rand_data_map() -> HashMap<String, String> {
    let mut data_map = HashMap::new();
    for _ in 0..100 {
        data_map.insert(rand_string(), rand_string());
    }
    data_map
}

fn gen_data_map() -> HashMap<String, String> {
    let mut rng = rand::thread_rng();
    let mut data_map = HashMap::new();
    for _ in 0..WORKLOAD_SIZE {
        let key = rng.sample_iter(&Alphanumeric).take(KEY_LENGTH).collect();
        data_map.insert(key, "value".to_owned());
    }
    data_map
}

pub fn write_bench(c: &mut Criterion) {
    let data_map = gen_rand_data_map();
    let mut group = c.benchmark_group("write_bench");

    group.bench_function("kvs write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (
                    KvStore::open(temp_dir.path()).unwrap(),
                    data_map.clone(),
                    temp_dir,
                )
            },
            |(store, data_map, _temp_dir)| {
                for (key, value) in data_map {
                    assert!(store.set(key, value).is_ok(), "kvs store set error");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (
                    SledKvsEngine::new(sled::open(&temp_dir).unwrap()),
                    data_map.clone(),
                    temp_dir,
                )
            },
            |(store, data_map, _temp_dir)| {
                for (key, value) in data_map {
                    assert!(store.set(key, value).is_ok(), "sled store set error");
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

pub fn read_bench(c: &mut Criterion) {
    let data_map = gen_rand_data_map();
    let read_keys = gen_read_keys(&data_map);
    let mut group = c.benchmark_group("read_bench");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("kvs read", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.path()).unwrap();
                for (key, value) in data_map.clone() {
                    store.set(key, value).unwrap();
                }
                (store, read_keys.clone(), temp_dir)
            },
            |(store, read_keys, _temp_dir)| {
                for key in read_keys {
                    assert!(store.get(key).is_ok(), "kvs store get error");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled read", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
                for (key, value) in data_map.clone() {
                    store.set(key, value).unwrap();
                }
                (store, read_keys.clone(), temp_dir)
            },
            |(store, read_keys, _temp_dir)| {
                for key in read_keys {
                    assert!(store.get(key).is_ok(), "sled store get error");
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

pub fn write_shared_queue_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_shared_queue_kvs");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let temp_dir = TempDir::new().unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();

    let data_map = gen_data_map();
    for num in inputs.iter() {
        let pool = SharedQueueThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(
                        cli.set(key.clone(), value.clone()).is_ok(),
                        "client set error"
                    );
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_shared_queue_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_shared_queue_kvs");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let data_map = gen_data_map();
    let temp_dir = TempDir::new().unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in inputs.iter() {
        let pool = SharedQueueThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(cli.get(key.clone()).is_ok(), "client get error");
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn write_rayon_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_kvs");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let temp_dir = TempDir::new().unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();

    let data_map = gen_data_map();
    for num in inputs.iter() {
        let pool = RayonThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(
                        cli.set(key.clone(), value.clone()).is_ok(),
                        "client set error"
                    );
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_rayon_kvs(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_kvs");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let data_map = gen_data_map();
    let temp_dir = TempDir::new().unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in inputs.iter() {
        let pool = RayonThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(cli.get(key.clone()).is_ok(), "client get error");
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn write_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_sled");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let temp_dir = TempDir::new().unwrap();
    let engine = SledKvsEngine::new(sled::open(&temp_dir).unwrap());

    let data_map = gen_data_map();
    for num in inputs.iter() {
        let pool = RayonThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, value) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(
                        cli.set(key.clone(), value.clone()).is_ok(),
                        "client set error"
                    );
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

pub fn read_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_sled");
    group.measurement_time(Duration::from_secs(20));

    let inputs: [u32; 6] = [1, 2, 4, 8, 16, 32];
    let data_map = gen_data_map();
    let temp_dir = TempDir::new().unwrap();
    let engine = SledKvsEngine::new(sled::open(&temp_dir).unwrap());

    for (key, value) in data_map.clone() {
        engine.set(key, value).unwrap();
    }

    for num in inputs.iter() {
        let pool = RayonThreadPool::new(*num).unwrap();
        let mut server = KvsServer::new(engine.clone(), pool);
        server.run(DEFAULT_ADDRESS).unwrap();

        let client = Arc::new(Mutex::new(KvsClient::connect(DEFAULT_ADDRESS).unwrap()));
        let (tx, rx) = crossbeam::crossbeam_channel::unbounded();
        let barrier = Arc::new(Barrier::new(WORKLOAD_SIZE + 1));

        for (key, _) in data_map.clone() {
            let c = barrier.clone();
            let rx1 = rx.clone();
            let cli = client.clone();

            thread::spawn(move || loop {
                if let Err(_) = rx1.recv() {
                    return;
                }
                {
                    let mut cli = cli.lock().unwrap();
                    assert!(cli.get(key.clone()).is_ok(), "client get error");
                }
                c.wait();
            });
        }

        group.bench_with_input(BenchmarkId::from_parameter(num), num, |b, &_num| {
            let c = barrier.clone();
            let tx1 = tx.clone();
            b.iter(|| {
                for _ in 0..WORKLOAD_SIZE {
                    tx1.send(()).unwrap();
                }
                c.wait();
            });
        });

        server.shutdown();
    }
    group.finish();
}

criterion_group!(
    benches,
    write_bench,
    read_bench,
    write_shared_queue_kvs,
    read_shared_queue_kvs,
    write_rayon_kvs,
    read_rayon_kvs,
    write_rayon_sled,
    read_rayon_sled
);
criterion_main!(benches);
