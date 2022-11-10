//
// Created by sxy on 22-11-8.
//

/**
* 整个查询优化的思路
 * 1. Rewriter: input(operator tree), output(rewritten operator tree)
 * 2. Enumerator: input(operator tree), search space exploration algorithm with pruning, output(operator tree)
 * 3. Planner: input(operator tree), output(distributed query execution plan DQEP)
 *
 * Rewriter:
 * - projection push down
 * - predicates merge(eg. 0 < val < 50 or 45 < val < 100 -> 0 < val < 100)
 * - predicates simplify(eg. where 0 = 1 -> null)
 * - predicate push down
 * - group by push down (selective, need to cost estimate)
 *
 * Enumerator:
 * - join order, cost-based query rewrite(derived table, left-deep join tree -> bushy tree)
 *
 * Planner:
 *
 *
*/