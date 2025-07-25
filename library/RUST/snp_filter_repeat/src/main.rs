use std::fs::File;
use std::io::{BufWriter, Write};
use std::collections::HashSet;
use std::error::Error;
use std::env;
use memmap2::Mmap;
use rayon::prelude::*;

type ThreadSafeError = Box<dyn Error + Send + Sync>;

fn main() -> Result<(), ThreadSafeError> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!("Usage: {} input.snp cols.txt rows.txt output.snp", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let cols_path = &args[2];
    let rows_path = &args[3];
    let output_path = &args[4];

    // 读取列ID并保持原始顺序
    let target_cols = read_ids_to_vec(cols_path)?;
    // 读取行ID使用HashSet快速查找
    let target_rows = read_ids_to_hashset(rows_path)?;
    
    let input_file = File::open(input_path)?;
    let mmap = unsafe { Mmap::map(&input_file)? };
    let output_file = File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);
    
    let content = std::str::from_utf8(&mmap)?;
    let mut lines = content.lines();
    
    // 处理标题行
    let header = match lines.next() {
        Some(header) => header,
        None => return Err("Empty input file".into()),
    };
    
    let header_parts: Vec<&str> = header.split('\t').collect();
    
    // 构建列索引映射（保持cols.txt的顺序）
    let header_indices: Vec<usize> = target_cols
        .iter()
        .filter_map(|col| header_parts.iter().position(|&x| x == col))
        .collect();
    
    // 写入标题行（严格按cols.txt顺序）
    writeln!(&mut writer, "{}", target_cols.join("\t"))?;
    
    // 并行处理数据行
    let mut filtered_lines: Vec<(u32, String)> = lines
        .par_bridge()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.is_empty() || !target_rows.contains(parts[0]) {
                return None;
            }
            
            // 解析第一列为数字用于排序
            let address = parts[0].parse::<u32>().ok()?;
            
            // 按header_indices顺序提取字段
            let extracted_fields: Vec<&str> = header_indices
                .iter()
                .map(|&i| parts.get(i).copied().unwrap_or(""))
                .collect();
            
            // 检查行内字段是否全为"-"
            let mut all_dash = true;
            let mut base_value = None;
            let mut all_equal = true;
            
            // 跳过第一个字段（行ID）
            for field in extracted_fields.iter().skip(1) {
                if *field != "-" {
                    all_dash = false;
                    
                    match base_value {
                        Some(base) if base != *field => {
                            all_equal = false;
                            // 提前退出：发现不等字段
                            break;
                        }
                        Some(_) => {}
                        None => base_value = Some(*field),
                    }
                }
            }
            
            // 如果所有字段都是"-"或者所有非"-"字段相同，则跳过该行
            if all_dash || (all_equal && base_value.is_some()) {
                return None;
            }
            
            // 构建行数据字符串
            let row_data = extracted_fields.join("\t");
            Some((address, row_data))
        })
        .collect();
    
    // 按第一列数字排序
    filtered_lines.par_sort_unstable_by_key(|(address, _)| *address);
    
    // 写入排序后的数据
    for (_, line) in filtered_lines {
        writeln!(&mut writer, "{}", line)?;
    }
    
    Ok(())
}

// 保持列顺序的读取函数
fn read_ids_to_vec(filename: &str) -> Result<Vec<String>, ThreadSafeError> {
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    Ok(mmap
        .split(|&c| c == b'\n')
        .filter_map(|line| std::str::from_utf8(line).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

// 快速查找的行ID读取函数
fn read_ids_to_hashset(filename: &str) -> Result<HashSet<String>, ThreadSafeError> {
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    Ok(mmap
        .split(|&c| c == b'\n')
        .par_bridge()
        .filter_map(|line| std::str::from_utf8(line).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}