package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 历史代码，select push已经在builder中完成了
 */
public class SelectPusher {

    public static QueryTreeNode optimize(QueryTreeNode qtn) throws QueryException {
        pushSelect(qtn);
        return qtn;
    }

    /**
     * 将select查询push到叶子节点，需要从最底层将数据带上来
     * 
     * @param qn
     * @throws QueryException
     */
    private static void pushSelect(QueryTreeNode qn) throws QueryException {
        if (qn instanceof QueryNode) {
            QueryTreeNode child = ((QueryNode) qn).getChild();
            if (child == null || child.isSubQuery()) {
                return;
            } else {
                List<ISelectable> columnsToPush = new LinkedList<ISelectable>();
                for (ISelectable c : qn.getColumnsRefered()) {
                    if (c instanceof IColumn) {
                        columnsToPush.add(c.copy().setAlias(null));
                    }
                }
                child.select(columnsToPush);
                child.build();
                pushSelect(child);
                return;
            }
        } else if (qn instanceof JoinNode) {
            List<ISelectable> columns = new LinkedList<ISelectable>(qn.getColumnsRefered());
            for (ASTNode child : qn.getChildren()) {
                if (((QueryTreeNode) child).isSubQuery()) {
                    continue;
                }

                // 老逻辑，感觉代码有点问题，调整下
                if (!columns.containsAll(((QueryTreeNode) child).getColumnsSelected())) {
                    List<ISelectable> childSelected = ((QueryTreeNode) child).getColumnsSelectedForParent();
                    List<ISelectable> newChildSelected = new ArrayList<ISelectable>(columns.size());
                    for (ISelectable c : columns) {
                        boolean flag = false;
                        if (c instanceof IFunction) {
                            continue;
                        }

                        if (childSelected.contains(c)) {
                            flag = true;
                        }

                        if (flag) {
                            newChildSelected.add(c.copy().setAlias(null));
                        }
                    }

                    ((QueryTreeNode) child).select(newChildSelected);
                    child.build();
                }
            }
        }
    }
}
