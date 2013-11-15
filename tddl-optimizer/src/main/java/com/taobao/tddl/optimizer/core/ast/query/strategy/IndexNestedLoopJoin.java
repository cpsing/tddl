package com.taobao.tddl.optimizer.core.ast.query.strategy;

import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;

/**
 * 对于IndexNestLoop，因为应该迭代执行，与常规的递归执行不同 所以需要对IndexNestLoop的子树做一下变形，让其执行顺序与其他保持一致
 * 
 * <pre>
 * IndexNestLoop的一些约束条件： 
 * 1、如果IndexNestLoop的子节点为Join，那么该Join一定是IndexNestLoop，如Join1为IndexNestLoop，则Join2，Join3，Join4必须为IndexNestLoop
 * 2、q0与Join2的最左的子Join是可Join的 
 * 3、各个Join的列应该是同样的形式与数目（逻辑上的名字应该是相同的，如都是name=name）
 * 4、IndexNestLoop的右节点只能是Join或者Query 
 * 5、IndexNestLoop的子Join的左节点只能是Join或者Query
 * 
 * Join1 / \ q0 Join2 / \ Join3 Join4 / \ / \ q1 q2 q3 q4 也即，上面的计划树会被转换为如下形式
 * Join(Join(Join(Join(q0,q1),q2),q3),q4) Join / \ q4 Join / \ q3 Join / \ q2
 * join / \ q1 q0
 * </pre
 * 
 * ?
 * 
 * @author jianghang 2013-11-14 下午10:33:54
 * @since 5.1.0
 */
public class IndexNestedLoopJoin extends AbstractJoinStrategy {

    public JoinType getType() {
        return JoinType.INDEX_NEST_LOOP;
    }
}
