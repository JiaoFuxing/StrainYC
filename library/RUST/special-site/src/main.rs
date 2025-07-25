use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    hash::BuildHasherDefault,
};

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<fxhash::FxHasher>>;

fn cleanup() -> std::io::Result<()> {
    fs::read_dir(".")?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.starts_with("PC") && s.ends_with(".add"))
                .unwrap_or(false)
        })
        .try_for_each(fs::remove_file)
}

fn process(path: &str) -> std::io::Result<()> {
    let mut writers = HashMap::new();
    let mut reader = BufReader::new(File::open(path)?);

    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break; // EOF
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        // 把字段收集到 Vec<&str>
        let parts: Vec<&str> = trimmed.split('\t').collect();
        if parts.len() < 2 {
            continue;
        }

        let key = parts[0];
        let cols = &parts[1..];

        // 一次 O(n) 统计
        let mut freq = FxHashMap::default();
        for &v in cols {
            *freq.entry(v).or_insert(0) += 1;
        }

        // 按需写文件
        for (i, &val) in cols.iter().enumerate() {
            if val == "-" || freq[val] != 1 {
                continue;
            }
            let writer = writers.entry(i + 1).or_insert_with(|| {
                let f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(format!("PC{}.add", i + 1))
                    .unwrap();
                BufWriter::new(f)
            });
            writeln!(writer, "{}\t{}", key, val)?;
        }
    }

    writers.into_values().try_for_each(|mut w| w.flush())?;
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <tsv_file>", args[0]);
        std::process::exit(1);
    }
    cleanup().unwrap();
    if let Err(e) = process(&args[1]) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}