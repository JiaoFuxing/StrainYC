use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::collections::HashMap;
use rayon::prelude::*;
use std::f64::consts::PI;

// KDE和峰值查找功能保持不变
fn gaussian(x: f64, mu: f64, sigma: f64) -> f64 {
    (-0.5 * ((x - mu) / sigma).powi(2)).exp() / (sigma * (2.0 * PI).sqrt())
}

fn gaussian_kde(data: &[f64], bw: Option<f64>, gridsize: usize) -> (Vec<f64>, Vec<f64>) {
    let bw = bw.unwrap_or_else(|| {
        let mean = data.iter().sum::<f64>() / data.len() as f64;
        let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / data.len() as f64;
        1.06 * variance.sqrt() * (data.len() as f64).powf(-0.2)
    });
    
    let x_min = data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let x_max = data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    
    let x: Vec<f64> = (0..gridsize)
        .map(|i| x_min + (x_max - x_min) * i as f64 / (gridsize - 1) as f64)
        .collect();
    
    let mut y = vec![0.0; gridsize];
    for &d in data {
        for i in 0..gridsize {
            y[i] += gaussian(x[i], d, bw);
        }
    }
    let y: Vec<f64> = y.iter().map(|&v| v / data.len() as f64).collect();
    
    (x, y)
}

fn smooth(y: &[f64], win: usize) -> Vec<f64> {
    let mut smoothed = vec![0.0; y.len()];
    let half_win = win / 2;
    for i in 0..y.len() {
        let start = i.saturating_sub(half_win);
        let end = (i + half_win + 1).min(y.len());
        smoothed[i] = y[start..end].iter().sum::<f64>() / (end - start) as f64;
    }
    smoothed
}

fn find_highest_peak(x: &[f64], y: &[f64], threshold: f64) -> Option<(f64, f64)> {
    let y_max = y.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let threshold = threshold * y_max;
    
    let mut peaks = Vec::new();
    for i in 1..y.len()-1 {
        if y[i] > y[i-1] && y[i] > y[i+1] {
            let left_min = y[..=i].iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let right_min = y[i..].iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let prominence = y[i] - left_min.max(right_min);
            if prominence >= threshold {
                peaks.push((x[i], y[i]));
            }
        }
    }
    peaks.into_iter().max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
}

fn calculate_averages(depths: &[f64], counts: &[f64]) -> (f64, f64) {
    // 计算 TotalDepth 的平均值 (对应原脚本的 average1)
    let mut depth_counts: HashMap<String, usize> = HashMap::new();
    let mut depth_totals: HashMap<String, f64> = HashMap::new();
    
    for &depth in depths {
        let key = format!("{:.6}", depth);
        *depth_counts.entry(key.clone()).or_insert(0) += 1;
        *depth_totals.entry(key).or_insert(0.0) += depth;
    }
    
    // 按出现次数降序排序
    let mut depth_stats: Vec<_> = depth_counts.iter()
        .map(|(k, &cnt)| (cnt, depth_totals[k]))
        .collect();
    depth_stats.sort_by(|a, b| b.0.cmp(&a.0));
    
    // 取前10个最常出现的 TotalDepth 值
    let (depth_sum, depth_cnt) = depth_stats.iter()
        .take(10)
        .fold((0.0, 0), |(sum, cnt), &(c, t)| (sum + t, cnt + c));
    let depth_avg = if depth_cnt > 0 { depth_sum / depth_cnt as f64 } else { 0.0 };

    // 计算 BaseCount 的平均值 (对应原脚本的 average2)
    let mut count_counts: HashMap<String, usize> = HashMap::new();
    let mut count_totals: HashMap<String, f64> = HashMap::new();
    
    for &count in counts {
        let key = format!("{:.6}", count);
        *count_counts.entry(key.clone()).or_insert(0) += 1;
        *count_totals.entry(key).or_insert(0.0) += count;
    }
    
    // 按出现次数降序排序
    let mut count_stats: Vec<_> = count_counts.iter()
        .map(|(k, &cnt)| (cnt, count_totals[k]))
        .collect();
    count_stats.sort_by(|a, b| b.0.cmp(&a.0));
    
    // 取前5个最常出现的 BaseCount 值
    let (count_sum, count_cnt) = count_stats.iter()
        .take(5)
        .fold((0.0, 0), |(sum, cnt), &(c, t)| (sum + t, cnt + c));
    let count_avg = if count_cnt > 0 { count_sum / count_cnt as f64 } else { 0.0 };

    (depth_avg, count_avg)
}

fn process_pc(i: usize, filter_file: &str, pc_file: &str) -> (usize, f64, usize, usize, Option<f64>, f64) {
    let pc_data: HashMap<String, String> = BufReader::new(File::open(pc_file).unwrap())
        .lines()
        .skip(1)
        .filter_map(|l| l.ok())
        .filter_map(|line| {
            let mut parts = line.split('\t');
            Some((parts.next()?.to_string(), parts.next()?.to_string()))
        })
        .collect();
    
    let mut ratios = Vec::new();
    let mut z = 0;
    let mut total = 0;
    let mut depths = Vec::new();
    let mut counts = Vec::new();
    
    if let Ok(filter) = File::open(filter_file) {
        for line in BufReader::new(filter).lines().filter_map(|l| l.ok()) {
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() >= 5 {
                if let Some(expected_base) = pc_data.get(fields[0]) {
                    total += 1;
                    let counts_str = fields[1];
                    let total_depth: f64 = fields[4].parse().unwrap_or(0.0);
                    depths.push(total_depth);
                    
                    let base_count = counts_str.split(',')
                        .filter_map(|pair| {
                            let mut parts = pair.split(':');
                            if parts.next()? == expected_base {
                                parts.next()?.parse().ok()
                            } else {
                                None
                            }
                        })
                        .next()
                        .unwrap_or(0.0);
                    
                    counts.push(base_count);
                    
                    let ratio = if total_depth > 0.0 { base_count / total_depth } else { 0.0 };
                    if ratio > 0.0 {
                        z += 1;
                        ratios.push(ratio);
                    }
                }
            }
        }
    }
    
    let ratio = if total > 0 { z as f64 / total as f64 } else { 0.0 };
    
    // 对每个菌株的ratios进行KDE分析并找到最高峰
    let highest_peak = if !ratios.is_empty() {
        let (x, y) = gaussian_kde(&ratios, None, 1024);
        let y_smoothed = smooth(&y, 11);
        find_highest_peak(&x, &y_smoothed, 0.05).map(|(pos, _)| pos)
    } else {
        None
    };
    
    // 计算新的平均值
    let (depth_avg, count_avg) = calculate_averages(&depths, &counts);
    let avg_ratio = if depth_avg > 0.0 { count_avg / depth_avg } else { 0.0 };
    
    (i, ratio, z, total, highest_peak, avg_ratio)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <filter_file> <strain_file> <output_file>", args[0]);
        eprintln!("Example: {} ./filter-result-with-counts.txt ./output/2.txt DF-result-1.txt", args[0]);
        std::process::exit(1);
    }

    let filter_file = &args[1];
    let strain_file = &args[2];
    let output_file = &args[3];
    
    let strain_lines: Vec<String> = BufReader::new(File::open(strain_file).unwrap())
        .lines()
        .skip(1)
        .filter_map(|l| l.ok())
        .collect();
    
    let results: Vec<_> = (1..=strain_lines.len())
        .into_par_iter()
        .map(|i| {
            let pc_file = format!("PC{}.add", i);
            process_pc(i, filter_file, &pc_file)
        })
        .collect();
    
    // 直接写入用户指定的输出文件
    let mut output = BufWriter::new(File::create(output_file).unwrap());
    
    // 写入表头
    writeln!(output, "Strain\tMatch(N/D)\tN\tD\tPeak(abundance)\tAvgRatio(5|10)").unwrap();
    
    for ((_i, ratio, z, m, highest_peak, avg_ratio), strain_line) in results.into_iter().zip(strain_lines.into_iter()) {
        let peak_ratio = highest_peak.unwrap_or(0.0);
        writeln!(
            output, 
            "{}\t{:.4}\t{}\t{}\t{:.4}\t{:.5}", 
            strain_line, ratio, z, m, peak_ratio, avg_ratio
        ).unwrap();
    }
    
    println!("details: {}", output_file);
}