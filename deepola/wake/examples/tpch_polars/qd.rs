use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

// TODO: You need to implement the query d.sql in this file.
/*
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
    p_partkey = l_partkey
    and l_shipinstruct = 'DELIVER IN PERSON'
    and
    (
        (
            p_brand = 'Brand#12'
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
        )
        or
        (
            p_brand = 'Brand#23'
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
        )
        or
        (
            p_brand = 'Brand#34'
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
        )
    )
*/
pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec![
                "l_partkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_shipinstruct"
            ],
        ),
        (
            "part".into(),
            vec!["p_partkey", "p_brand", "p_size", "p_container"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // Predicate Pushdown
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_shipinstruct = df.column("l_shipinstruct").unwrap();
            let mask = l_shipinstruct.equal("DELIVER IN PERSON").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_size = df.column("p_size").unwrap();
            let p_brand = df.column("p_brand").unwrap();
            let l_quantity = df.column("l_quantity").unwrap();

            let mask1 = p_brand.equal("Brand#12").unwrap()
                & p_size.gt_eq(1i32).unwrap()
                & p_size.lt_eq(5i32).unwrap()
                & l_quantity.gt_eq(1i32).unwrap()
                & l_quantity.lt_eq(11i32).unwrap();

            let mask2 = p_brand.equal("Brand#23").unwrap()
                & p_size.gt_eq(1i32).unwrap()
                & p_size.lt_eq(10i32).unwrap()
                & l_quantity.gt_eq(10i32).unwrap()
                & l_quantity.lt_eq(20i32).unwrap();

            let mask3 = p_brand.equal("Brand#34").unwrap()
                & p_size.gt_eq(1i32).unwrap()
                & p_size.lt_eq(15i32).unwrap()
                & l_quantity.gt_eq(20i32).unwrap()
                & l_quantity.lt_eq(30i32).unwrap();

            let mask = mask1 | mask2 | mask3;
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![Series::new(
                "disc_price",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            )];
            DataFrame::new(columns).unwrap()
        })))
        .build();

    // GROUP BY Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    hash_join_node.subscribe_to_node(&lineitem_where_node, 0); // Left Node
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1); // Right Node
    where_node.subscribe_to_node(&hash_join_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(hash_join_node);
    service.add(lineitem_where_node);
    service.add(part_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}
