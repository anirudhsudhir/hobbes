use criterion::{criterion_group, criterion_main, Criterion};
use rand::{seq::SliceRandom, thread_rng, Rng};

use std::{path::Path, str::FromStr};

use hobbes_kv::engine::{hobbes, sled_engine, Engine};

const HOBBES_DB_BENCH_PATH: &str = "bench-db/hobbes-bench-db";
const SLED_DB_BENCH_PATH: &str = "bench-db/sled-bench-db";

const LEN_LOWER: u64 = 10;
const LEN_UPPER: u64 = 10000;
const TEST_CHAR: &str = "A";
const SET_RUN_COUNT: usize = 500;
const GET_RUN_COUNT: usize = 500;

fn randomise(run_count: usize) -> Vec<(String, String)> {
    let mut test_vals: Vec<(String, String)> = Vec::with_capacity(run_count);
    let mut rng = thread_rng();
    for _ in 1..run_count {
        let key_len = rng.gen_range(LEN_LOWER..LEN_UPPER);
        let val_len = rng.gen_range(LEN_LOWER..LEN_UPPER);
        let mut key = String::with_capacity(key_len as usize);
        let mut val = String::with_capacity(val_len as usize);
        for _ in 1..key_len {
            key += TEST_CHAR;
        }
        for _ in 1..val_len {
            val += TEST_CHAR;
        }
        test_vals.push((key, val));
    }

    test_vals.shuffle(&mut rng);
    test_vals
}

fn bench_set(c: &mut Criterion) {
    let mut hobbes_eng = hobbes::HobbesEngine::open(Path::new(HOBBES_DB_BENCH_PATH))
        .expect("failed to start the hobbes engine");
    let rand_vals = randomise(SET_RUN_COUNT);
    c.bench_function("hobbes set bench", |b| {
        b.iter(|| {
            for (key, val) in &rand_vals {
                hobbes_eng
                    .set(
                        String::from_str(key).expect("key: failed to convert str slice to String"),
                        String::from_str(val).expect("val: failed to convert str slice to String"),
                    )
                    .expect("failed to set the value in the hobbes engine");
            }
        })
    });

    let mut sled_eng = sled_engine::SledEngine::open(Path::new(SLED_DB_BENCH_PATH))
        .expect("failed to start the sled engine");

    c.bench_function("sled set bench", |b| {
        b.iter(|| {
            for (key, val) in &rand_vals {
                sled_eng
                    .set(
                        String::from_str(key).expect("key: failed to convert str slice to String"),
                        String::from_str(val).expect("val: failed to convert str slice to String"),
                    )
                    .expect("failed to set the value in the sled engine");
            }
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let mut hobbes_eng = hobbes::HobbesEngine::open(Path::new(HOBBES_DB_BENCH_PATH))
        .expect("failed to start the hobbes engine");
    let rand_vals = randomise(GET_RUN_COUNT);
    for (key, val) in &rand_vals {
        hobbes_eng
            .set(
                String::from_str(key).expect("key: failed to convert str slice to String"),
                String::from_str(val).expect("val: failed to convert str slice to String"),
            )
            .expect("failed to set the value in the hobbes engine");
    }
    c.bench_function("hobbes get bench", |b| {
        b.iter(|| {
            for (key, val) in &rand_vals {
                let hobbes_val = hobbes_eng
                    .get(String::from_str(key).expect("key: failed to convert str slice to String"))
                    .expect("failed to get the value in the hobbes engine")
                    .expect("no value present for the key in hobbes");
                assert_eq!(hobbes_val.as_str(), val);
            }
        })
    });

    let mut sled_eng = sled_engine::SledEngine::open(Path::new(SLED_DB_BENCH_PATH))
        .expect("failed to start the sled engine");

    for (key, val) in &rand_vals {
        sled_eng
            .set(
                String::from_str(key).expect("key: failed to convert str slice to String"),
                String::from_str(val).expect("val: failed to convert str slice to String"),
            )
            .expect("failed to set the value in the sled engine");
    }

    c.bench_function("sled get bench", |b| {
        b.iter(|| {
            for (key, val) in &rand_vals {
                let sled_val = sled_eng
                    .get(String::from_str(key).expect("key: failed to convert str slice to String"))
                    .expect("failed to get the value in the hobbes engine")
                    .expect("no value present for the key in hobbes");
                assert_eq!(sled_val.as_str(), val);
            }
        })
    });
}

criterion_group!(benches, bench_set, bench_get);
criterion_main!(benches);
