package com.taobao.tddl.optimizer.core.ast.query.strategy;

import java.util.Map;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * join策略
 * 
 * <pre>
 * 一些算法介绍：http://blog.csdn.net/panzhaobo775/article/details/7327896
 * 
 * NEST_LOOP_JOIN :  源1的每一条记录，依次去匹配源2表，将符合连接条件的放到结果集中，直到源1的遍历完全
 * INDEX_NEST_LOOP : 链接条件刚好为源1的索引字段，遍历源2表的所有记录，查询源1索引表判断是否匹配
 * SORT_MERGE_JOIN ：将源1和源2的表按照链接条件字段各自进行排序，然后采取归并的算法进行匹配遍历
 * HAHS_JOIN : 比如将源1表的链接条件字段进行hash计算，并创建临时hash表，遍历源2表的所有记录，查询hash表判断是否匹配
 * </pre>
 * 
 * @author Dreamond
 * @author jianghang 2013-11-14 下午6:17:15
 * @since 5.1.0
 */
public interface JoinStrategy {

    IJoin getQuery(JoinNode joinNode, Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan) throws QueryException;

    JoinType getType();
}
