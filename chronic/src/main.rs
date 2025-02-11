use polars::{
    lazy::native::{enable_profiling, enable_tracing, open_new},
    prelude::{col, concat, lit, LazyFrame, ScanArgsParquet},
};

fn task1() {
    let chronic = "./chronic/chronic.parquet";
    let chronic_policy = "./chronic/chronic.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(chronic_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(chronic, args).unwrap();

    // We will remove all countries with less than 100 patients
    let df_plan = df
        .group_by(["country"])
        .agg(vec![col("user_id").count().alias("count")])
        .filter(col("count").gt_eq(100))
        .with_row_index("index", Some(0))
        .select([col("index")]);

    let now = std::time::Instant::now();
    let filtered_df = df_plan
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect();
    println!("Time: {:?}", now.elapsed());

    // This would fail!
    if let Err(e) = filtered_df {
        eprintln!("Error: {}", e);
    }
}

fn task2() {
    let chronic = "./chronic/chronic.parquet";
    let chronic_policy = "./chronic/chronic.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(chronic_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(chronic, args).unwrap();

    let now = std::time::Instant::now();
    let df = df
        .group_by(["trackable_type"])
        .agg([col("*").count()])
        .set_ctx_id(ctx_id)
        .set_policy_checking(true)
        .collect();

    println!("Time: {:?}", now.elapsed());
    // This would fail!
    if let Err(e) = df {
        eprintln!("Error: {}", e);
    }
}

fn task3() {
    let chronic = "./chronic/chronic.parquet";
    let chronic_policy = "./chronic/chronic.policy.parquet";

    let args = ScanArgsParquet {
        with_policy: Some(chronic_policy.into()),
        ..Default::default()
    };
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = LazyFrame::scan_parquet(chronic, args).unwrap();

    let now = std::time::Instant::now();

    let top_conditions = df
        .clone()
        .filter(col("trackable_type").eq(lit("Condition")))
        .group_by(["trackable_name"])
        .agg([col("*").count()]);
    let top_treatments = df
        .clone()
        .filter(col("trackable_type").eq(lit("Treatment")))
        .group_by(["trackable_name"])
        .agg([col("*").count()]);

    let top_symptoms = df
        .clone()
        .filter(col("trackable_type").eq(lit("Symptom")))
        .group_by(["trackable_name"])
        .agg([col("*").count()]);

    let top_conditions = top_conditions
        .set_ctx_id(ctx_id)
        .set_policy_checking(false)
        .collect()
        .unwrap();
    let top_treatments = top_treatments
        .set_ctx_id(ctx_id)
        .set_policy_checking(false)
        .collect()
        .unwrap();
    let top_symptoms = top_symptoms
        .set_ctx_id(ctx_id)
        .set_policy_checking(false)
        .collect()
        .unwrap();

    // get conditions within top 100
    let top_conditions = df
        .clone()
        .filter(col("trackable_type").eq(lit("Condition")))
        .filter(col("trackable_name").is_in(lit(
            top_conditions.column("trackable_name").unwrap().clone(),
        )));

    // get treatments within top 100
    let top_treatments = df
        .clone()
        .filter(col("trackable_type").eq(lit("Treatment")))
        .filter(col("trackable_name").is_in(lit(
            top_treatments.column("trackable_name").unwrap().clone(),
        )));

    // get symptoms within top 100
    let top_symptoms = df
        .clone()
        .filter(col("trackable_type").eq(lit("Symptom")))
        .filter(
            col("trackable_name")
                .is_in(lit(top_symptoms.column("trackable_name").unwrap().clone())),
        );
    // combine the dataframes
    let df = concat(
        [
            top_conditions.clone(),
            top_treatments.clone(),
            top_symptoms.clone(),
        ],
        Default::default(),
    )
    .unwrap();

    // drop the users that have less than 2 different checkin months
    let df = df
        .clone()
        .group_by(["user_id"])
        .agg([col("checkin_date").n_unique().alias("n_unique")])
        .filter(col("n_unique").gt_eq(2))
        .join(
            df.clone(),
            [col("user_id")],
            [col("user_id")],
            Default::default(),
        )
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
