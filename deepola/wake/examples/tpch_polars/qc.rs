use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

// TODO: You need to implement the query c.sql in this file.
/*
select
	c_custkey,
	c_name,
	c_acctbal,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
from
	customer,
	orders,
	lineitem,
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
group by
	c_custkey,
	c_name,
	c_acctbal,
order by
	revenue desc;
 */

 pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        ("lineitem".into(),
        vec![
            "l_orderkey", // For COUNT(*) counting a key.
            "l_extendedprice",
            "l_discount"])
        ,
        ("orders".into(),
        vec![
            "o_orderkey", // For COUNT(*) counting a key.
            "o_totalprice",
            "o_orderdate",
            "o_custkey"]
        ),
        ("customer".into(), 
        vec![
            "c_custkey", "c_name",
            "c_acctbal"]
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = 
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customers_csvreader_node = 
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("o_orderdate").unwrap();
            let mask = a.gt_eq("1993-10-01").unwrap() & a.lt("1994-01-01").unwrap() ;
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Nodes
    let hash_join_node1 = HashJoinBuilder::new()       
    .left_on(vec!["c_custkey".into()])
    .right_on(vec!["o_custkey".into()])
    .build();

    let hash_join_node2 = HashJoinBuilder::new()       
    .left_on(vec!["l_orderkey".into()])
    .right_on(vec!["o_orderkey".into()])
    .build();
    
    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![
                Series::new(
                    "disc_price",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                )
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["c_custkey".to_string(), "c_name".to_string(), "c_acctbal".to_string()])
        .set_aggregates(vec![
            ("disc_price".into(), vec!["sum".into()])
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();


    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Compute AVG from SUM/COUNT.
            let columns = vec![
                Series::new("c_custkey", df.column("c_custkey").unwrap()),
                Series::new("c_name", df.column("c_name").unwrap()),
                Series::new("c_acctbal", df.column("c_acctbal").unwrap()),
                Series::new("revenue", df.column("disc_price_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["revenue"],vec![true])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&orders_csvreader_node, 0);
    hash_join_node1.subscribe_to_node(&where_node, 1); // Right Node
    hash_join_node1.subscribe_to_node(&customers_csvreader_node, 0); // Left Node
    hash_join_node2.subscribe_to_node(&hash_join_node1, 1); // Right Node
    hash_join_node2.subscribe_to_node(&lineitem_csvreader_node, 0); // Left Node
    expression_node.subscribe_to_node(&hash_join_node2, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);


    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(where_node);
    service.add(customers_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(hash_join_node1);
    service.add(hash_join_node2);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
