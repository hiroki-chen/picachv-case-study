use clap::{Parser, ValueEnum};
use picachv_core::{
    constants::GroupByMethod,
    dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame},
    policy::{types::AnyValue, AggType, BinaryTransformType, Policy, PolicyLabel, TransformType},
};
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Dataset {
    Chronic,
    Healthcare,
    Stackoverflow,
    Github,
}

#[derive(Parser, Debug)]
pub struct Args {
    dataset: Dataset,
}


// user_id,age,sex,country,checkin_date,trackable_id,trackable_type,trackable_name,trackable_value
fn healthcare(args: Args) {
    let df = LazyFrame::scan_parquet("./healthcare/healthcare_dataset.parquet", Default::default()).unwrap();
    let row_num = df.clone().collect().unwrap().shape().0;
    let cols = df.schema().unwrap();

    let name_policy = Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyTop.into(),
        next: Policy::PolicyClean.into(),
    };

    let medical_policy = Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyAgg {
            ops: picachv_core::policy::AggOps(vec![
                AggType {
                    how: GroupByMethod::Max,
                    group_size: 1,
                },
                AggType {
                    how: GroupByMethod::Mean,
                    group_size: 1,
                },
                AggType {
                    how: GroupByMethod::Count { include_nulls: false },
                    group_size: 1,
                },
            ]),
        }
        .into(),
        next: Policy::PolicyClean.into(),
    };

    println!("healthcare");

    let mut columns = vec![];
    for (c, ty) in cols.iter() {
        if c == "name" {
            let c = vec![name_policy.clone().into(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        } else if c == "medical_condition" {
            let c = vec![medical_policy.clone().into(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        } else {
            let c = vec![Default::default(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        }
    }

    PolicyGuardedDataFrame::new(columns)
        .to_parquet("./healthcare/healthcare_dataset.policy.parquet")
        .unwrap();
}


// user_id,age,sex,country,checkin_date,trackable_id,trackable_type,trackable_name,trackable_value
fn chronic(args: Args) {
    let df = LazyFrame::scan_parquet("./chronic/chronic.parquet", Default::default()).unwrap();
    let row_num = df.clone().collect().unwrap().shape().0;
    let cols = df.schema().unwrap();

    let user_id_policy = Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyTop.into(),
        next: Policy::PolicyClean.into(),
    };

    let age_policy = Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyTop.into(),
        next: Policy::PolicyClean.into(),
    };

    let trackable_policy = Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyAgg {
            ops: picachv_core::policy::AggOps(vec![
                AggType {
                    how: GroupByMethod::Max,
                    group_size: 1,
                },
                AggType {
                    how: GroupByMethod::Mean,
                    group_size: 1,
                },
                AggType {
                    how: GroupByMethod::Count { include_nulls: false },
                    group_size: 1,
                },
            ]),
        }
        .into(),
        next: Policy::PolicyClean.into(),
    };

    println!("Chronic");

    let mut columns = vec![];
    for (c, ty) in cols.iter() {
        if c == "user_id" {
            let c = vec![user_id_policy.clone().into(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        } else if c.starts_with("trackable_") {
            let c = vec![trackable_policy.clone().into(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        } else if c == "age" {
            // Get indices for all rows where age is greater than 89
            let indices = df
                .clone()
                .with_row_index("index", Some(0))
                .filter(col("age").gt(lit(89)))
                .select([col("index")])
                .collect()
                .unwrap();
            let indices = indices
                .get(0)
                .unwrap()
                .into_iter()
                .map(|v| v.try_extract::<u32>().unwrap() as usize)
                .collect::<Vec<_>>();

            let mut c = vec![Default::default(); row_num as usize];
            for i in indices {
                c[i] = age_policy.clone().into()
            }

            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        } else {
            let c = vec![Default::default(); row_num as usize];
            columns.push(PolicyGuardedColumn::new_from_iter(c.iter()).unwrap().into());
        }
    }

    PolicyGuardedDataFrame::new(columns)
        .to_parquet("./chronic/chronic.policy.parquet")
        .unwrap();
}

fn main() {
    let args = Args::parse();

    match args.dataset {
        Dataset::Chronic => chronic(args),
        Dataset::Healthcare =>  healthcare(args),
        Dataset::Stackoverflow => println!("Stackoverflow"),
        Dataset::Github => println!("Github"),
    }
}
