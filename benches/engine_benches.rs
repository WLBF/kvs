use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use tempfile::TempDir;

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

fn gen_data_map() -> HashMap<String, String> {
    let mut data_map = HashMap::new();
    for _ in 0..100 {
        data_map.insert(rand_string(), rand_string());
    }
    data_map
}

pub fn write_bench(c: &mut Criterion) {
    let data_map = gen_data_map();
    let mut group = c.benchmark_group("write bench group");

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
    let data_map = gen_data_map();
    let read_keys = gen_read_keys(&data_map);
    let mut group = c.benchmark_group("read bench group");

    group.sample_size(10);

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

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
