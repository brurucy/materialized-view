### materialized-view 

Database-agnostic just-in-time iterative dynamic incremental materialized views for the masses!
 * **Materialized View**: A query that gets updated once new data arrives
 * **Incremental Materialized View**: Materialized views whose update latency is proportional to
 the size of the update
 * **Dynamic Incremental Materialized View**: Materialized views that can be "shortened" or "extended",
 with the time taken to adjust to this change being proportional to the new updates generated from this change
 * **Iterative dynamic incremental materialized views**: Dynamic materialized views that support recursive
 statements (that of course also get incrementally maintained)
 * **Database-agnostic just-in-time iterative dynamic incremental materialized views**: All that was said so far, with
 zero compilation required **and** being easily integrated with any database engine!

 `MaterializedDatalogView` is both a **property graph** computation engine, incrementally maintaining recursive
 and non-recursive queries, and a storage engine. The storage engine controls access to the materialisation by restricting it
 to happen over two views:
 1. Consolidated - The always-up-to-date state of the materialisation.
 2. Frontier - The most recent updates. You can query the frontier each time after `poll` event happens
 to retrieve the latest updates to the materialisation, and then store it in whichever database you are using.

 # Examples

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