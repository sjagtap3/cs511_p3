use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

// TODO: You need to implement the query b.sql in this file.
/*
select
	c_name,
	sum(o_totalprice) as o_totalprice_sum
from
	orders,
	customer
where
	o_custkey = c_custkey
	and c_mktsegment = 'AUTOMOBILE'
group by
	c_name
order by
	o_totalprice_sum desc
*/

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "orders".into(),
        vec![
            "o_totalprice",
            "o_custkey"
        ],
        ),
        ("customer".into(), 
        vec!["c_custkey", "c_mktsegment", "c_name"]),
    
    ]);

    // CSVReaderNode would be created for this table.
    let orders_csvreader_node =
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);


    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("c_mktsegment").unwrap();
            let mask = a.equal("AUTOMOBILE").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let hash_join_node = HashJoinBuilder::new()
    .left_on(vec!["c_custkey".into()])        
    .right_on(vec!["o_custkey".into()])
    .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["c_name".to_string()])
        .set_aggregates(vec![
            ("o_totalprice".into(), vec!["sum".into()])
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![
                Series::new("c_name", df.column("c_name").unwrap()),
                Series::new("o_totalprice_sum", df.column("o_totalprice_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["o_totalprice_sum"], vec![true])
                .unwrap()
        })))
        .build();


     // Connect nodes with subscription
     where_node.subscribe_to_node(&customer_csvreader_node, 0);
     hash_join_node.subscribe_to_node(&where_node, 0); // Left Node
     hash_join_node.subscribe_to_node(&orders_csvreader_node, 1); // Right Node
     //expression_node.subscribe_to_node(&hash_join_node, 0);
     groupby_node.subscribe_to_node(&hash_join_node, 0);
     select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(groupby_node);
    //service.add(expression_node);
    service.add(where_node);
    service.add(hash_join_node);
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);

    service
}
