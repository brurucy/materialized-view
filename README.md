### materialized-view

[![crates.io](https://img.shields.io/crates/v/materialized-view.svg)](https://crates.io/crates/materialized-view)
[![docs](https://docs.rs/materialized-view/badge.svg)](https://docs.rs/materialized-view)

`materialized-view` is a database-agnostic incremental computation engine whose **focus** is in graph data models, such 
as RDF and Property graphs. As it is an incremental system, the latency incurred to update a query view is proportional
to the size of the update.

There are two other projects that are similar in intent, albeit far more general as they support SQL, and are partially
built upon the same underlying theory:
1. [materialize](https://github.com/MaterializeInc/materialize)
2. [feldera](https://github.com/feldera/feldera)

The major differences from `materialized-view` to them are:
1. A focus in graph-shaped data
2. The query language is **Datalog** instead of SQL
3. Creating a query does not require compilation 
4. Queries can be changed on the fly - this includes adding new queries that query queries
5. `materialized-view` is a higher-order interpreter of a variant of incremental lambda calculus

#### Is it a database?

No. It does however offer **two** always-updated views of the underlying queries:
1. Consolidated - The always-up-to-date state of the materialisation.
2. Frontier - The most recent updates. You can query the frontier each time after a `poll` event happens
to retrieve the latest updates to the materialisation, and then store it in whichever database you are using.

#### What are its limitations?

`materialized-view`'s Datalog is significantly constrained in terms of expressivity. It is equivalent to SQL, without
aggregates and negation, but with a powerful declarative recursion construct that allows you to do more (and far more efficiently) than
`WITH RECURSIVE`. You will see it in the example.

#### Example

```rust
use materialized_view::*;

type NodeIndex = i32;
type Edge = (NodeIndex, NodeIndex);

// The following recursive query is "equivalent" to this pseudo-SQL statement:
// WITH RECURSIVE reaches(x, y) AS (
//    SELECT x, y FROM edge
//
//    UNION ALL
//
//    SELECT e.x, r.y
//    FROM edge e
//    JOIN reaches r ON e.y = r.x
// )
fn main() { 
 let recursive_query = program! {
  reaches(?x, ?y) <- [edge(?x, ?y)],
  reaches(?x, ?z) <- [edge(?x, ?y), reaches(?y, ?z)]
 };
 let mut dynamic_view = MaterializedDatalogView::new(recursive_query);

 // Add some edges.
 dynamic_view.push_fact("edge", (1, 2));
 dynamic_view.push_fact("edge", (2, 3));
 dynamic_view.push_fact("edge", (3, 4));
 dynamic_view.push_fact("edge", (4, 5));
 dynamic_view.push_fact("edge", (5, 6));

 // Then poll to incrementally update the view
 dynamic_view.poll();

 // Confirm that 6 is reachable from 1
 assert!(dynamic_view.contains("reaches", (1, 6)).unwrap());
 
 // Retract a fact
 dynamic_view.retract_fact("edge", (5, 6));
 dynamic_view.poll();
 
 // Query everything that is reachable from 1
 dynamic_view 
         // The arity of the relation being queried must be specified. e.g to query
         // a relation with two columns, `query_binary` ought to be used. 
         .query_binary::<NodeIndex, NodeIndex>("reaches", (Some(1), ANY_VALUE))
         .unwrap()
         .for_each(|edge| println!("{} is reachable from 1", *edge.1));

 // You are also able to query only the __most recent__ updates.
 dynamic_view
         // The arity of the relation being queried must be specified. e.g to query
         // a relation with two columns, `query_binary` ought to be used.
         .query_frontier_binary::<NodeIndex, NodeIndex>("reaches", (Some(1), ANY_VALUE))
         .unwrap()
         // The second argument is the weight. It represents whether the given value should be added
         // or retracted.
         .for_each(|((from, to), weight)| println!("Diff: {} - Value: ({}, {})", weight, *from, *to));

 // By extending the query with another query, it is possible to incrementally query the incrementally
 // maintained queries
 dynamic_view
         // Queries can also be assembled both a macro a-la program! called rule!:
         // rule! { reachableFromOne(1isize, ?x) <- reaches(1isize, ?x) }
         .push_rule((("reachableFromOne", (Const(1), Var("x"))), vec![("reaches", (Const(1), Var("x")))]));

 dynamic_view.poll();
 dynamic_view
         .query_binary::<NodeIndex, NodeIndex>("reachableFromOne", (Some(1), ANY_VALUE))
         .unwrap()
         .for_each(|edge| println!("{} is reachable from 1", edge.1));

 // And of course, you can retract rules as well!
 assert!(dynamic_view.contains("reachableFromOne", (1, 5)).unwrap());
 dynamic_view
         .retract_rule((("reachableFromOne", (Const(1), Var("x"))), vec![("reaches", (Const(1), Var("x")))]));
 dynamic_view.poll();
 assert!(!dynamic_view.contains("reachableFromOne", (1, 5)).unwrap());
}
```