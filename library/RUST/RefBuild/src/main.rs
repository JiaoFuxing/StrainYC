use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::error::Error;

fn process_file(input_path: &str, output_path: &str) -> Result<(), Box<dyn Error>> {
    // 一次性把文件读进来
    let data = fs::read(input_path)?;

    let mut out = BufWriter::new(File::create(output_path)?);
    out.write_all(b">really\n")?;

    let mut start = 0;
    while start < data.len() {
        // 找到行尾
        let end = data[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(data.len(), |p| start + p);
        let line = &data[start..end];

        // 找第二列（第一个 tab 之后）
        if let Some(tab) = line.iter().position(|&b| b == b'\t') {
            let seq = &line[tab + 1..];

            // 过滤 '*' 并转大写
            let iter = seq.iter().copied().filter(|&b| b != b'*').map(|b| {
                if b.is_ascii_lowercase() { b.to_ascii_uppercase() } else { b }
            });

            let mut iter = iter;
            if let Some(first) = iter.next() {
                if iter.all(|b| b == first) {
                    out.write_all(&[first])?;
                } else {
                    out.write_all(b"-")?;
                }
            } else {
                out.write_all(b"-")?;
            }
        }
        start = end + 1; // 跳过 '\n'
    }

    out.write_all(b"\n")?;
    Ok(())
}

fn main() {
    if let Err(e) = process_file("./output/hi", "./output/really-ref.fa") {
        eprintln!("错误: {}", e);
    } else {
        println!("build---> 'really-ref.fa'");
    }
}