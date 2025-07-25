#![cfg(target_arch = "x86_64")]

use std::arch::x86_64::*;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;
use rayon::prelude::*;

// AVX-512 比较函数
#[target_feature(enable = "avx512bw")]
unsafe fn simd_compare_avx512(a: __m512i, b: __m512i, dash: __m512i) -> u32 {
    // 比较不相等的位
    let ne = _mm512_cmpneq_epi8_mask(a, b);
    
    // 比较不是破折号的位
    let not_dash_a = _mm512_cmpneq_epi8_mask(a, dash);
    let not_dash_b = _mm512_cmpneq_epi8_mask(b, dash);
    
    // 合并所有条件
    let mask = ne & not_dash_a & not_dash_b;
    
    mask.count_ones()
}

// AVX2 比较函数
#[target_feature(enable = "avx2")]
unsafe fn simd_compare_avx2(a: __m256i, b: __m256i, dash: __m256i) -> u32 {
    let ne = _mm256_cmpeq_epi8(a, b);
    let ne = _mm256_andnot_si256(ne, _mm256_set1_epi8(-1));
    
    let not_dash_a = _mm256_cmpeq_epi8(a, dash);
    let not_dash_a = _mm256_andnot_si256(not_dash_a, _mm256_set1_epi8(-1));
    
    let not_dash_b = _mm256_cmpeq_epi8(b, dash);
    let not_dash_b = _mm256_andnot_si256(not_dash_b, _mm256_set1_epi8(-1));
    
    let mask = _mm256_and_si256(ne, _mm256_and_si256(not_dash_a, not_dash_b));
    let mask = _mm256_movemask_epi8(mask);
    mask.count_ones()
}

// 主计算函数
fn calculate_snp_distance(ref_seq: &[u8], query: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512bw") {
            unsafe { calculate_snp_distance_avx512(ref_seq, query) }
        } else if is_x86_feature_detected!("avx2") {
            unsafe { calculate_snp_distance_avx2(ref_seq, query) }
        } else {
            calculate_snp_distance_scalar(ref_seq, query)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        calculate_snp_distance_scalar(ref_seq, query)
    }
}

// AVX-512 版本
#[target_feature(enable = "avx512bw")]
unsafe fn calculate_snp_distance_avx512(ref_seq: &[u8], query: &[u8]) -> usize {
    let dash = b'-';
    let dash_simd = _mm512_set1_epi8(dash as i8);
    let mut distance = 0;
    let mut i = 0;

    // SIMD 处理（64字节块）
    let simd_chunks = ref_seq.len() / 64;
    for _ in 0..simd_chunks {
        let ref_simd = _mm512_loadu_si512(ref_seq[i..].as_ptr() as *const __m512i);
        let query_simd = _mm512_loadu_si512(query[i..].as_ptr() as *const __m512i);
        distance += simd_compare_avx512(ref_simd, query_simd, dash_simd) as usize;
        i += 64;
    }

    // 处理剩余字节
    while i < ref_seq.len() {
        let r = ref_seq[i];
        let q = query[i];
        if r != dash && q != dash && r != q {
            distance += 1;
        }
        i += 1;
    }

    distance
}

// AVX2 版本
#[target_feature(enable = "avx2")]
unsafe fn calculate_snp_distance_avx2(ref_seq: &[u8], query: &[u8]) -> usize {
    let dash = b'-';
    let dash_simd = _mm256_set1_epi8(dash as i8);
    let mut distance = 0;
    let mut i = 0;

    // SIMD 处理（32字节块）
    let simd_chunks = ref_seq.len() / 32;
    for _ in 0..simd_chunks {
        let ref_simd = _mm256_loadu_si256(ref_seq[i..].as_ptr() as *const __m256i);
        let query_simd = _mm256_loadu_si256(query[i..].as_ptr() as *const __m256i);
        distance += simd_compare_avx2(ref_simd, query_simd, dash_simd) as usize;
        i += 32;
    }

    // 处理剩余字节
    while i < ref_seq.len() {
        let r = ref_seq[i];
        let q = query[i];
        if r != dash && q != dash && r != q {
            distance += 1;
        }
        i += 1;
    }

    distance
}

// 标量版本
fn calculate_snp_distance_scalar(ref_seq: &[u8], query: &[u8]) -> usize {
    let dash = b'-';
    let mut distance = 0;
    
    for (r, q) in ref_seq.iter().zip(query.iter()) {
        if *r != dash && *q != dash && r != q {
            distance += 1;
        }
    }
    
    distance
}

// FASTA 记录结构
struct FastaRecord {
    id: String,
    seq: Vec<u8>,
}

// 高效 FASTA 解析器
fn parse_fasta_mmap<P: AsRef<Path>>(path: P) -> Vec<FastaRecord> {
    let file = File::open(path).expect("Failed to open file");
    let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap file") };
    let data = &mmap[..];

    let mut records = Vec::new();
    let mut lines = data.split(|&b| b == b'\n');
    let mut current_id = None;
    let mut current_seq = Vec::new();

    while let Some(line) = lines.next() {
        if line.is_empty() {
            continue;
        }
        if line[0] == b'>' {
            if let Some(id) = current_id.take() {
                records.push(FastaRecord {
                    id,
                    seq: current_seq,
                });
                current_seq = Vec::new();
            }
            current_id = Some(String::from_utf8_lossy(&line[1..]).into_owned());
        } else {
            current_seq.extend_from_slice(line);
        }
    }

    if let Some(id) = current_id {
        records.push(FastaRecord { id, seq: current_seq });
    }

    records
}

fn main() {
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();
    let output_count = if args.len() > 1 {
        args[1].parse().unwrap_or(20)
    } else {
        20
    };

    // 1. 读取参考序列
    let ref_records = parse_fasta_mmap("./output/really-ref.fa");
    let ref_record = &ref_records[0];
    let ref_seq = Arc::new(ref_record.seq.clone());

    // 2. 并行处理查询序列
    let query_records = parse_fasta_mmap("./DB/2k-snp.fa");

    // 预分配结果数组
    let mut results: Vec<(String, usize)> = Vec::with_capacity(query_records.len());

    // 使用并行迭代器处理
    query_records.into_par_iter().map(|record| {
        let distance = calculate_snp_distance(&ref_seq, &record.seq);
        (record.id, distance)
    }).collect_into_vec(&mut results);

    // 3. 并行排序
    results.par_sort_unstable_by_key(|&(_, dist)| dist);

    // 4. 高效输出
    let stdout = std::io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    writeln!(writer, "address").unwrap();
    for (id, _) in results.into_iter().take(output_count) {
        writeln!(writer, "{}", id).unwrap();
    }
    writer.flush().unwrap();
}