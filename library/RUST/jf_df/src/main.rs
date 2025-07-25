// src/main.rs
#![allow(clippy::uninlined_format_args)]

use ahash::AHashMap as HashMap;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write}; // 只用 io::Write
use std::sync::Arc;

use rayon::prelude::*;
use dashmap::DashMap;
use memmap2::Mmap;

/// 处理单行 → (处理后文本, min_count)
fn process_line(line: &str) -> Option<(String, usize)> {
    let mut cols = line.splitn(3, '\t');
    let col1 = cols.next()?;  // 第一列
    let _col2 = cols.next()?.trim_end();  // 第二列（现在忽略）
    let col3 = cols.next().unwrap_or("");  // 第三列（如果有）

    let mut cnt = HashMap::with_capacity(32);
    for c in _col2.chars() {
        *cnt.entry(c.to_ascii_uppercase()).or_insert(0) += 1;
    }

    // 仅含 * 的行
    if cnt.keys().all(|&c| c == '*') {
        let out = format!("{}\t0\t0\t0\t0", col1);  // 只保留第一列
        return Some((out, 0));
    }

    let min_cnt = cnt
        .iter()
        .filter(|&(&c, _)| c != '*')
        .map(|(_, &v)| v)
        .min()?;

    // 预分配字符串，手动拼接
    let mut count_str = String::with_capacity(256);
    let mut letters: Vec<char> = Vec::with_capacity(32);
    let mut total = 0usize;

    let mut first = true;
    for (&c, &v) in cnt.iter() {
        if c == '*' {
            continue;
        }
        if !first {
            count_str.push(',');
        }
        first = false;
        count_str.push(c);
        count_str.push(':');
        count_str.push_str(&v.to_string());
        letters.push(c);
        total += v;
    }
    letters.sort_unstable();
    let letters_str: String = letters.into_iter().collect();

    // 修改输出格式，跳过第二列
    let out = if col3.is_empty() {
        format!(
            "{}\t{}\t{}\t{}\t{}",
            col1, count_str, min_cnt, letters_str, total
        )
    } else {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            col1, col3, count_str, min_cnt, letters_str, total
        )
    };
    Some((out, min_cnt))
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let min_threshold = args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);

    let input_path  = "./output/2lie-mp-samtool";
    let stats_path  = "all-stats.txt";
    let filter_path = "filter-result-with-counts.txt";
    let poc_path    = "./output/poc.txt";

    // 内存映射 + 跳过 UTF-8 校验
    let file = File::open(input_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let content = unsafe { std::str::from_utf8_unchecked(&mmap) };

    // 并行处理
    let results: Arc<DashMap<usize, (String, usize, bool)>> = Arc::new(DashMap::new());
    content
        .lines()
        .collect::<Vec<&str>>()
        .par_iter()
        .enumerate()
        .for_each(|(idx, &line)| {
            if let Some((processed, min_cnt)) = process_line(line) {
                // 重新统计字母，确定是否多字母组合
                let mut cnt = HashMap::with_capacity(32);
                if let Some(col) = line.split('\t').nth(1) {
                    for c in col.chars() {
                        *cnt.entry(c.to_ascii_uppercase()).or_insert(0) += 1;
                    }
                }
                let multi = cnt
                    .keys()
                    .filter(|&&c| c != '*')
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    > 1;
                results.insert(idx, (processed, min_cnt, multi));
            }
        });

    // 8 MiB 缓冲写文件
    const BUF_CAP: usize = 8 * 1024 * 1024;
    let mut stats  = BufWriter::with_capacity(BUF_CAP, File::create(stats_path)?);
    let mut filter = BufWriter::with_capacity(BUF_CAP, File::create(filter_path)?);
    let mut poc    = BufWriter::with_capacity(BUF_CAP, File::create(poc_path)?);

    // 按原始顺序输出
    let mut sorted: Vec<_> = results.iter().collect();
    sorted.sort_by_key(|e| *e.key());

    for entry in sorted {
        let (line, min_cnt, multi) = entry.value();
        stats.write_all(line.as_bytes())?;
        stats.write_all(b"\n")?;
        if *multi {
            filter.write_all(line.as_bytes())?;
            filter.write_all(b"\n")?;
            if *min_cnt > min_threshold {
                let first = line.split('\t').next().unwrap_or("");
                poc.write_all(first.as_bytes())?;
                poc.write_all(b"\n")?;
            }
        }
    }

    println!("完整统计已保存到 '{}'", stats_path);
    println!("过滤结果已保存到 '{}'", filter_path);
    println!("POC 列表已保存到 '{}'，阈值 = {}", poc_path, min_threshold);

    Ok(())
}