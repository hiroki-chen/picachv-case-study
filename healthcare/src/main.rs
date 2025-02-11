use polars::{lazy::native::*, prelude::*};

fn task1() {
    let healthcare = "./healthcare/healthcare_dataset.parquet";
    let healthcare_policy = "./healthcare/healthcare_dataset.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(healthcare_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(healthcare, args).unwrap();

    let now = std::time::Instant::now();
    // What is the percentage of each gender in the data?
    let df_plan = df
        .group_by(["Gender"])
        .agg([col("Blood Type").count().alias("count")])
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect()
        .unwrap();

    println!("Time: {:?}", now.elapsed());
}

fn task2() {
    let healthcare = "./healthcare/healthcare_dataset.parquet";
    let healthcare_policy = "./healthcare/healthcare_dataset.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(healthcare_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(healthcare, args).unwrap();

    let now = std::time::Instant::now();
    // What is the percentage of each gender in the data?
    let df_plan = df
        .clone()
        .group_by(["Medical Condition"])
        .agg([col("Age").mean().alias("age_mean")])
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect()
        .unwrap();

    // Who is the oldest patient in the dataset, and what is their age?
    let max_age = df
        .sort(["Age"], Default::default())
        .limit(1)
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect()
        .unwrap();

    println!("Time: {:?}", now.elapsed());
}

fn task3() {
    let healthcare = "./healthcare/healthcare_dataset.parquet";
    let healthcare_policy = "./healthcare/healthcare_dataset.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(healthcare_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(healthcare, args).unwrap();

    let now = std::time::Instant::now();

    // Which medication used different medical conditions
    let top = df
        .clone()
        .group_by(["Medical Condition"])
        .agg([col("Medication").count().alias("medication")])
        .sort(["medication"], Default::default())
        .limit(5)
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect()
        .unwrap();

    // Top 10 hight fees colleting hosptial
    let top = df
        .clone()
        .group_by(["Hospital"])
        .agg([col("Billing Amount").sum().alias("fees")])
        .sort(["fees"], Default::default())
        .limit(10)
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect()
        .unwrap();

    println!("Time: {:?}", now.elapsed());
}

fn main() {
    println!("Task 1\n");
    task1();
    println!("Task 2\n");
    task2();
    println!("Task 3\n");
    task3();
}
