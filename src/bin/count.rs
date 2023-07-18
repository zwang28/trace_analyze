use base64::Engine;
use itertools::Itertools;
use plotters::prelude::*;
use std::cmp;
use std::cmp::max;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Display, Formatter};

use clap::Parser;
#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    input_path: String,
    #[arg(long, default_value = ".")]
    output_dir: String,
    #[arg(long)]
    target_table_id: u64,
    #[arg(long, default_value_t = 1000)]
    max_timestamp_bucket: u64,
    #[arg(long, default_value_t = 1000_000_000)]
    max_key_bucket: u64,
    #[arg(long, default_value_t = false)]
    key_appearance_cdf: bool,
    #[arg(long, default_value_t = false)]
    key_access_count: bool,
    #[arg(long, default_value_t = false)]
    key_time_series: bool,
    #[arg(long, default_value_t = false)]
    key_reuse_period: bool,
    #[arg(long, default_value_t = false)]
    locality_over_time: bool,
    #[arg(long, default_value_t = 600)]
    locality_over_time_bin_sec: u64,
    #[arg(long, default_value_t = false)]
    key_time_span: bool,
    #[arg(long, default_value_t = 2048)]
    output_width: u32,
    #[arg(long, default_value_t = 1536)]
    output_height: u32,
}

struct Metadata {
    sample_count: u64,
    key_seq_count: u64,
    ts_bucket_num: u64,
    ts_bucket_size_sec: u64,
    key_bucket_num: u64,
    key_bucket_size: u64,
}

impl Display for Metadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("sample_count: {}\n", self.sample_count))?;
        f.write_fmt(format_args!("key_seq_count: {}\n", self.key_seq_count))?;
        f.write_fmt(format_args!("ts_bucket_num: {}\n", self.ts_bucket_num))?;
        f.write_fmt(format_args!(
            "ts_bucket_size_sec: {}\n",
            self.ts_bucket_size_sec
        ))?;
        f.write_fmt(format_args!("key_bucket_num: {}\n", self.key_bucket_num))?;
        f.write_fmt(format_args!("key_bucket_size: {}\n", self.key_bucket_size))?;
        Ok(())
    }
}

type KeyBucketId = u64;
type TsBucketId = u64;
type HistogramVec = BTreeMap<TsBucketId, Vec<KeyBucketId>>;

fn key_appearance_cdf(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    let mut key_counts: HashMap<KeyBucketId, u64> = HashMap::new();
    for (_, v) in hist_vec {
        for s in v {
            let e = key_counts.entry(*s).or_default();
            *e += 1;
        }
    }
    let mut cdf = vec![];
    let mut sum = 0;
    for v in key_counts.into_values().sorted() {
        sum += v;
        cdf.push(sum);
    }
    let data = cdf.into_iter().map(|v| (v as f64) / (sum as f64));
    let data_len = data.len();

    // println!("{:?}", cdf);
    let path = format!("{}/key_appearance_cdf.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_appearance_cdf", ("sans-serif", 40))
        .build_cartesian_2d(0.0..1.0, 0.0..1.0)
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "key seq. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("percentage")
        .draw()
        .unwrap();
    chart
        .draw_series(
            AreaSeries::new(
                (1..)
                    .zip(data)
                    .map(|(x, y)| (x as f64 / data_len as f64, y)),
                0.0,
                &RED.mix(0.2),
            )
            .border_style(&RED),
        )
        .unwrap();
    root.present().unwrap();
}

fn key_time_series(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    // let mut max_key_count = 0;
    // let mut ts_key_counts: HashMap<TsBucketId, HashMap<KeyBucketId, u64>> =
    //     HashMap::with_capacity(hist_vec.len());
    // for (ts, v) in hist_vec {
    //     let mut key_counts: HashMap<KeyBucketId, u64> = HashMap::new();
    //     for s in v {
    //         let e = key_counts.entry(*s).or_default();
    //         *e += 1;
    //     }
    //     let m = key_counts.values().max().copied().unwrap();
    //     if m > max_key_count {
    //         max_key_count = m;
    //     }
    //     ts_key_counts.insert(*ts, key_counts);
    // }

    let path = format!("{}/key_time_series.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    // root.fill(&HSLColor(240.0 / 360.0, 0.7, 0.1)).unwrap();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_time_series", ("sans-serif", 40))
        .build_cartesian_2d(0..metadata.ts_bucket_num, 0..metadata.key_bucket_num)
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "time. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("key seq")
        // .axis_desc_style(("sans-serif", 20, &WHITE))
        // .label_style(&WHITE)
        // .axis_style(&WHITE)
        .draw()
        .unwrap();
    hist_vec.iter().for_each(|(ts, k)| {
        chart
            .draw_series(
                k.iter()
                    .map(|key| (*ts, *key))
                    .map(|(t, k)| Rectangle::new([(t, k), (t + 1, k + 1)], GREEN.filled())),
            )
            .unwrap();
    });
    // chart
    //     .draw_series(
    //         ts_key_counts
    //             .iter()
    //             .map(|(ts, k)| k.iter().map(|(key, count)| (*ts, *key, *count)))
    //             .flatten()
    //             .map(|(t, k, _v)| {
    //                 // let v = v as f64 / max_key_count as f64;
    //                 // let color =
    //                 //     HSLColor(240.0 / 360.0 - 240.0 / 360.0 * v, 0.7, 0.1 + 0.4 * v).filled();
    //                 Rectangle::new([(t, k), (t + 1, k + 1)], GREEN.filled())
    //             }),
    //     )
    //     .unwrap();
    root.present().unwrap();
}

fn key_access_count(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    let mut key_counts: BTreeMap<KeyBucketId, u64> = BTreeMap::new();
    for (_, v) in hist_vec {
        for s in v {
            let e = key_counts.entry(*s).or_default();
            *e += 1;
        }
    }
    let max_key_count = key_counts.values().max().copied().unwrap();

    let path = format!("{}/key_access_count.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_access_count", ("sans-serif", 40))
        .build_cartesian_2d(0..metadata.key_bucket_num, 0..max_key_count)
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "key seq. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("count")
        .draw()
        .unwrap();
    chart
        .draw_series(
            key_counts
                .iter()
                .map(|(k, c)| Circle::new((*k, *c), 2, GREEN.filled())),
        )
        .unwrap();
    root.present().unwrap();
}

fn key_time_span(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    let mut span: BTreeMap<KeyBucketId, (u64, u64)> = BTreeMap::new();
    for (ts, v) in hist_vec {
        for k in v {
            let e = span.entry(*k).or_insert((*ts, *ts));
            e.1 = *ts;
        }
    }

    let mut distribution: BTreeMap<u64, u64> = BTreeMap::new();
    for (_, v) in &span {
        let s = v.1 - v.0;
        let e = distribution.entry(s).or_default();
        *e += 1;
    }
    // println!("{distribution:?}");

    let path = format!("{}/key_time_span_distribution.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_time_span_distribution", ("sans-serif", 40))
        .build_cartesian_2d(
            0..distribution.keys().max().copied().unwrap() + 1,
            0..distribution.values().max().copied().unwrap() + 1,
        )
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "key time span. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("count")
        .draw()
        .unwrap();
    chart
        .draw_series(AreaSeries::new(distribution, 0, &RED.mix(0.2)).border_style(&RED))
        .unwrap();
    // chart.draw_series(distribution.iter().map(|(x,y)|{
    //     Circle::new((*x, *y), 2, GREEN.filled())
    // })).unwrap();
    root.present().unwrap();
}

fn key_reuse_period(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    let max_diff = (hist_vec.keys().last().copied().unwrap()
        - hist_vec.keys().next().copied().unwrap())
        * metadata.ts_bucket_size_sec;
    let buckets_sec: Vec<u64> = vec![
        0,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096,
        cmp::max(4096, max_diff) + 1,
    ];
    let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();
    let mut key_last_ts_sec: HashMap<KeyBucketId, u64> = HashMap::new();

    for (ts, keys) in hist_vec {
        for k in keys {
            match key_last_ts_sec.entry(*k) {
                Entry::Occupied(mut last) => {
                    let diff = (*ts - *last.get()) * metadata.ts_bucket_size_sec;
                    last.insert(*ts);
                    let diff_bucket = buckets_sec.partition_point(|&x| x < diff) as u64;
                    let e = buckets.entry(diff_bucket).or_default();
                    *e += 1;
                }
                Entry::Vacant(_) => {
                    key_last_ts_sec.insert(*k, *ts);
                }
            }
        }
    }
    let hit_max = buckets.values().max().copied().unwrap();

    let path = format!("{}/key_reuse_period.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_reuse_period", ("sans-serif", 40))
        .build_cartesian_2d(
            (0..buckets_sec[buckets_sec.len() - 1] as i32)
                .log_scale()
                .with_key_points(buckets_sec.iter().map(|b| *b as i32).collect()),
            0..hit_max as i32 + 1,
        )
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "time. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("count")
        .draw()
        .unwrap();
    chart
        .draw_series(LineSeries::new(
            buckets.iter().map(|(b, c)| (*b as i32, *c as i32)),
            &RED,
        ))
        .unwrap();
    root.present().unwrap();
}

fn locality_over_time(args: &Args, hist_vec: &HistogramVec, metadata: &Metadata) {
    let bin_size_bucket_num = max(
        1,
        args.locality_over_time_bin_sec / metadata.ts_bucket_size_sec,
    );
    let mut bin: HashMap<KeyBucketId, Vec<u64>> = HashMap::new();
    let mut percentages: BTreeMap<u64, f64> = BTreeMap::new();
    println!("bin_size_bucket_num: {bin_size_bucket_num}");
    for (ts, keys) in hist_vec {
        for k in keys {
            bin.entry(*k).or_default().push(*ts);
        }
        if *ts >= bin_size_bucket_num - 1 {
            for b in bin.values_mut() {
                let new_start = b.partition_point(|bb| ts - *bb >= bin_size_bucket_num);
                b.drain(0..new_start);
            }
            bin.retain(|_k, v| !v.is_empty());
            let total = bin.values().map(|v| v.len()).sum::<usize>();
            let unique = bin.values().filter(|v| v.len() == 1).count();
            let percentage = unique as f64 / total as f64;
            percentages.insert(*ts, percentage);
        }
    }

    let path = format!("{}/key_uniqueness_over_time.png", args.output_dir);
    let root =
        BitMapBackend::new(&path, (args.output_width, args.output_height)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .caption("key_uniqueness_over_time", ("sans-serif", 40))
        .build_cartesian_2d(0..metadata.ts_bucket_num as i32, 0.0..1.0)
        .unwrap();
    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_desc(format!(
            "time. {}-{};{}-{}",
            metadata.ts_bucket_size_sec,
            metadata.ts_bucket_num,
            metadata.key_bucket_size,
            metadata.key_bucket_num
        ))
        .y_desc("count")
        .draw()
        .unwrap();
    chart
        .draw_series(LineSeries::new(
            percentages.iter().map(|(b, c)| (*b as i32, *c)),
            &RED,
        ))
        .unwrap();
    root.present().unwrap();
}

fn main() {
    let args = Args::parse();

    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut min_timestamp: Option<u64> = None;
    let mut max_timestamp: Option<u64> = None;
    let mut sample_count: u64 = 0;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(&args.input_path)
        .unwrap();
    for r in reader.records() {
        let r = r.unwrap();
        let table_id = str::parse::<u64>(r.get(0).unwrap()).unwrap();
        if args.target_table_id != table_id {
            continue;
        }
        let data = r.get(1).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(data)
            .unwrap();
        let timestamp = str::parse::<u64>(r.get(2).unwrap()).unwrap();
        keys.insert(decoded);
        match min_timestamp {
            None => {
                min_timestamp = Some(timestamp);
            }
            Some(prev) => {
                if prev > timestamp {
                    min_timestamp = Some(timestamp);
                }
            }
        }
        match max_timestamp {
            None => {
                max_timestamp = Some(timestamp);
            }
            Some(prev) => {
                if prev < timestamp {
                    max_timestamp = Some(timestamp);
                }
            }
        }
        sample_count += 1;
    }
    let min_timestamp = min_timestamp.unwrap();
    let max_timestamp = max_timestamp.unwrap();
    let ts_bucket_size_sec = (max_timestamp - min_timestamp) / args.max_timestamp_bucket + 1;
    let ts_bucket_num = (max_timestamp - min_timestamp) / ts_bucket_size_sec + 1;

    let mut key_seq_map: HashMap<Vec<u8>, u64> = HashMap::with_capacity(keys.len());
    let mut key_seq_count = 0;
    while let Some(k) = keys.pop_first() {
        key_seq_map.insert(k, key_seq_count);
        key_seq_count += 1;
    }
    let key_bucket_size = (key_seq_count + args.max_key_bucket - 1) / args.max_key_bucket;
    let key_bucket_num = (key_seq_count + key_bucket_size - 1) / key_bucket_size;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(&args.input_path)
        .unwrap();
    let mut histogram_vec: HistogramVec = BTreeMap::new();
    for r in reader.records() {
        let r = r.unwrap();
        let table_id = str::parse::<u64>(r.get(0).unwrap()).unwrap();
        if args.target_table_id != table_id {
            continue;
        }
        let data = r.get(1).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(data)
            .unwrap();
        let timestamp = str::parse::<u64>(r.get(2).unwrap()).unwrap();
        let key_seq_id = *key_seq_map.get(&decoded).unwrap();
        let ts_bucket_id = (timestamp - min_timestamp) / ts_bucket_size_sec;
        let key_bucket_id = key_seq_id / key_bucket_size;
        histogram_vec
            .entry(ts_bucket_id)
            .or_default()
            .push(key_bucket_id);
    }

    let metadata = Metadata {
        sample_count,
        key_seq_count,
        ts_bucket_num,
        ts_bucket_size_sec,
        key_bucket_num,
        key_bucket_size,
    };
    println!();
    println!("{metadata}");
    if args.key_appearance_cdf {
        key_appearance_cdf(&args, &histogram_vec, &metadata);
    }
    if args.key_access_count {
        key_access_count(&args, &histogram_vec, &metadata);
    }
    if args.key_time_series {
        key_time_series(&args, &histogram_vec, &metadata);
    }
    if args.key_reuse_period {
        key_reuse_period(&args, &histogram_vec, &metadata);
    }
    if args.locality_over_time {
        locality_over_time(&args, &histogram_vec, &metadata);
    }
    if args.key_time_span {
        key_time_span(&args, &histogram_vec, &metadata);
    }
}
