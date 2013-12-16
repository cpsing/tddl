package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class Merge extends QueryTree implements IMerge {

    protected List<IDataNodeExecutor> subNodes  = new LinkedList<IDataNodeExecutor>();
    protected Boolean                 isSharded = true;
    protected Boolean                 isUnion   = false;

    public IMerge assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);

        for (IDataNodeExecutor iDataNodeExecutor : getSubNode()) {
            iDataNodeExecutor.assignment(parameterSettings);
        }

        return this;
    }

    public IQueryTree copy() {
        IMerge merge = ASTNodeFactory.getInstance().createMerge();
        this.copySelfTo((QueryTree) merge);
        merge.setSubNode(this.getSubNode());
        merge.setSharded(this.isSharded);
        merge.setUnion(this.isUnion);
        return merge;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public List<IDataNodeExecutor> getSubNode() {
        return subNodes;
    }

    public IMerge setSubNode(List<IDataNodeExecutor> subNodes) {
        this.subNodes = subNodes;
        return this;
    }

    public IMerge addSubNode(IDataNodeExecutor subNode) {
        subNodes.add(subNode);
        return this;
    }

    public Boolean isSharded() {
        return isSharded;
    }

    public IMerge setSharded(boolean isSharded) {
        this.isSharded = isSharded;
        return this;
    }

    public Boolean isUnion() {
        return isUnion;
    }

    public IMerge setUnion(boolean isUnion) {
        this.isUnion = isUnion;
        return this;
    }

    public String toStringWithInden(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getAlias() != null) {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge") + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge"));
        }

        appendField(sb, "valueFilter", printFilterString(this.getValueFilter()), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter()), tabContent);
        if (!(this.getLimitFrom() != null && this.getLimitFrom().equals(-1L) && this.getLimitTo() != null && this.getLimitTo()
            .equals(-1L))) {
            appendField(sb, "limitFrom", this.getLimitFrom(), tabContent);
            appendField(sb, "limitTo", this.getLimitTo(), tabContent);
        }
        if (this.isSubQuery() != null && this.isSubQuery()) {
            appendField(sb, "isSubQuery", this.isSubQuery(), tabContent);
        }

        appendField(sb, "orderBy", this.getOrderBys(), tabContent);
        appendField(sb, "queryConcurrency", this.getQueryConcurrency(), tabContent);
        appendField(sb, "columns", this.getColumns(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        // if(this.getThread()!=null)
        // appendField(sb, "thread",
        // this.getThread(), tabContent);
        // appendField(sb, "requestID",
        // this.getRequestID(), tabContent);
        // appendField(sb, "subRequestID",
        // this.getSubRequestID(), tabContent);

        appendln(sb, tabContent + "subQueries");
        for (IDataNodeExecutor s : this.getSubNode()) {
            sb.append(s.toStringWithInden(inden + 2));
        }

        return sb.toString();
    }

}
