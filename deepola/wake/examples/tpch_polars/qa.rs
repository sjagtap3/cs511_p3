// TODO: You need to implement the query a.sql in this file.
use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/*select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
*/



pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
        // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "lineitem".into(),
        vec![
            "l_extendedprice",
            "l_discount",
            "l_shipdate",
        ],
    )]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
    build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("l_shipdate").unwrap();
            let mask = a.gt_eq("1994-01-01").unwrap() & a.lt("1995-01-01").unwrap();
            let result = df.filter(&mask).unwrap();
            result
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let extended_price = df.column("l_extendedprice").unwrap();
        let discount = df.column("l_discount").unwrap();
        let columns = vec![Series::new("disc_price", extended_price * discount)];
            DataFrame::new(columns).unwrap()
    })))
    .build();

    // GROUP BY NODE
    let mut agg_accumulator = SumAccumulator::new();
    agg_accumulator
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();


     // SELECT Node
     let select_node = AppenderNode::<DataFrame, MapAppender>::new()
     .appender(MapAppender::new(Box::new(|df: &DataFrame| {
         let columns = vec![
             Series::new("revenue", df.column("disc_price").unwrap())
         ];
         DataFrame::new(columns)
             .unwrap()
     })))
     .build();

    
    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node,0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);
    service

}
