package com.taobao.tddl.optimizer.costbased;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 预先处理join
 * 
 * <pre>
 * 1. 会遍历所有节点将right join的左右节点进行调换，转换成left join.
 * 
 * 比如 A right join B on A.id = B.id
 * 转化为 B left join B on A.id = B.id
 * 
 * 2. 尝试将子查询调整为join
 * 
 * 比如 select * from table1 where table1.id = (select id from table2) 
 * 转化为 selct table1.* from table1 join table2 on (table1.id = table2.id)
 * 
 * </pre>
 */
public class JoinPreProcessor {

    public static QueryTreeNode optimize(QueryTreeNode qtn) throws QueryException {
        qtn = findAndChangeRightJoinToLeftJoin(qtn);
        return qtn;
    }

    /**
     * 会遍历所有节点将right join的左右节点进行调换，转换成left join.
     * 
     * <pre>
     * 比如 A right join B on A.id = B.id
     * 转化为 B left join B on A.id = B.id
     * </pre>
     */
    private static QueryTreeNode findAndChangeRightJoinToLeftJoin(QueryTreeNode qtn) {
        for (ASTNode child : qtn.getChildren()) {
            findAndChangeRightJoinToLeftJoin((QueryTreeNode) child);
        }

        if (qtn instanceof JoinNode && ((JoinNode) qtn).isRightOuterJoin()) {
            /**
             * 如果带有其他非column=column条件，不能做这种转换，否则语义改变
             */
            if (qtn.getOtherJoinOnFilter() != null) {
                return qtn;
            }

            JoinNode jn = (JoinNode) qtn;
            jn.exchangeLeftAndRight();
            jn.build();
        }

        return qtn;
    }

}
