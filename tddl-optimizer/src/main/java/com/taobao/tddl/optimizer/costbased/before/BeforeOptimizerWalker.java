package com.taobao.tddl.optimizer.costbased.before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.costbased.after.RelationQueryOptimizer;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * @author Whisper
 */
public class BeforeOptimizerWalker implements RelationQueryOptimizer {

    private List<IBooleanFilterProcessor> processors = new ArrayList<IBooleanFilterProcessor>();

    public ASTNode optimize(ASTNode node, Map<Integer, ParameterContext> parameterSettings,
                            Map<String, Comparable> extraCmd) throws QueryException {
        if (node instanceof QueryTreeNode) {
            processQueryFilter((QueryTreeNode) node);
        }

        if (node instanceof DMLNode) {
            this.optimize(((DMLNode) node).getNode(), parameterSettings, extraCmd);
        }
        return node;
    }

    private void processQueryFilter(QueryTreeNode qtn) {
        qtn.setOtherJoinOnFilter(processOneFilter(qtn.getOtherJoinOnFilter()));
        qtn.having(processOneFilter(qtn.getHavingFilter()));
        if (qtn instanceof JoinNode) {
            for (int i = 0; i < ((JoinNode) qtn).getJoinFilter().size(); i++) {
                processOneFilter(((JoinNode) qtn).getJoinFilter().get(i));
            }
        }
        qtn.query(this.processOneFilter(qtn.getWhereFilter()));

        for (ASTNode child : qtn.getChildren()) {
            this.processQueryFilter((QueryTreeNode) child);
        }

    }

    /**
     * 目前直接忽略掉约束中的const=const，比如1=1 1=2之类的，不管是恒true还是恒false...
     * 
     * @param root
     * @return
     */
    private IFilter processOneFilter(IFilter root) {
        if (root == null) {
            return null;
        }
        if (root instanceof IBooleanFilter) {
            return processBoolFilter(root);
        } else if (root instanceof ILogicalFilter) {
            ILogicalFilter lf = (ILogicalFilter) root;
            List<IFilter> children = new LinkedList<IFilter>();
            for (IFilter child : lf.getSubFilter()) {
                IFilter childProcessed = this.processOneFilter(child);
                if (childProcessed != null) {
                    children.add(childProcessed);
                }
            }

            if (children.isEmpty()) {
                return null;
            }

            if (children.size() == 1) {
                return children.get(0);
            }

            lf.setSubFilter(children);
            return lf;
        }

        return root;
    }

    protected IFilter processBoolFilter(IFilter root) {
        for (IBooleanFilterProcessor processor : processors) {
            root = processor.processBoolFilter(root);
            if (root == null) {
                return null;
            }
        }
        return root;
    }

    public boolean add(IBooleanFilterProcessor arg0) {
        return processors.add(arg0);
    }

    public IBooleanFilterProcessor get(int arg0) {
        return processors.get(arg0);
    }

    public boolean isEmpty() {
        return processors.isEmpty();
    }

    public Iterator<IBooleanFilterProcessor> iterator() {
        return processors.iterator();
    }

}
