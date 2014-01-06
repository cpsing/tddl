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
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class Join extends QueryTree implements IJoin {

    /**
     * 左面join 子树 所有join都以key-value对的形式存在
     */
    protected IQueryTree        left;
    /**
     * 右面join 子树 所有join的子节点都以key-value对的形式存在
     */
    protected IQueryTree        right;
    /**
     * 左面join的列
     */
    protected List<ISelectable> leftColumns  = new LinkedList<ISelectable>();
    /**
     * 右面join的对应列
     */
    protected List<ISelectable> rightColumns = new LinkedList<ISelectable>(); ;

    /**
     * 非column=column的join列
     */
    protected IFilter           otherJoinOnFilter;
    /**
     * join的策略 ，具体请看对应解说
     */
    protected JoinStrategy      joinStrategy;
    /**
     * 左outer join,即使右表没有匹配的对应行，也输出左行 默认是false,也就是内联join
     */
    protected Boolean           leftOuter;
    /**
     * 右outer join 即使左表没有匹配的对应行，也输出右行 默认是false，也就是内联join
     */
    protected Boolean           rightOuter;
    protected IFilter           whereFilter;

    public IJoin copy() {
        IJoin join = ASTNodeFactory.getInstance().createJoin();
        copySelfTo((QueryTree) join);
        join.setJoinNodes(this.getLeftNode(), this.getRightNode());
        join.setJoinOnColumns(this.getLeftJoinOnColumns(), this.getRightJoinOnColumns());
        join.setJoinStrategy(this.getJoinStrategy());
        join.setLeftOuter(this.getLeftOuter());
        join.setRightOuter(this.getRightOuter());
        join.setOtherJoinOnFilter((IFilter) (otherJoinOnFilter == null ? null : otherJoinOnFilter.copy()));
        join.setWhereFilter(this.getWhereFilter());
        return join;
    }

    public Join assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);
        IQueryTree left = getLeftNode();
        if (left != null) {
            left.assignment(parameterSettings);
        }

        IQueryTree right = getRightNode();
        if (right != null) {
            right.assignment(parameterSettings);
        }

        if (this.otherJoinOnFilter != null) {
            otherJoinOnFilter = (IFilter) otherJoinOnFilter.assignment(parameterSettings);
        }

        return this;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public IJoin setJoinNodes(IQueryTree left, IQueryTree right) {
        this.left = left;
        this.right = right;
        return this;
    }

    public IJoin setLeftNode(IQueryTree left) {
        this.left = left;
        return this;
    }

    public IQueryTree getLeftNode() {
        return left;
    }

    public IJoin setRightNode(IQueryTree right) {
        this.right = right;
        return this;
    }

    public IQueryTree getRightNode() {
        return right;
    }

    public IJoin setJoinOnColumns(List<ISelectable> leftColumns, List<ISelectable> rightColumns) {
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        return this;
    }

    public IJoin addLeftJoinOnColumn(ISelectable left) {
        this.leftColumns.add(left);
        return this;
    }

    public List<ISelectable> getLeftJoinOnColumns() {
        return leftColumns;
    }

    public IJoin addRightJoinOnColumn(ISelectable right) {
        rightColumns.add(right);
        return this;
    }

    public List<ISelectable> getRightJoinOnColumns() {
        return rightColumns;
    }

    public IJoin setJoinStrategy(JoinStrategy joinStrategy) {
        this.joinStrategy = joinStrategy;
        return this;
    }

    public JoinStrategy getJoinStrategy() {
        return joinStrategy;
    }

    public Boolean getLeftOuter() {
        return leftOuter;
    }

    public Boolean getRightOuter() {
        return rightOuter;
    }

    public IJoin setLeftOuter(boolean on_off) {
        this.leftOuter = on_off;
        return this;
    }

    public IJoin setRightOuter(boolean on_off) {
        this.rightOuter = on_off;
        return this;
    }

    public IFilter getOtherJoinOnFilter() {
        return otherJoinOnFilter;
    }

    public IJoin setOtherJoinOnFilter(IFilter otherJoinOnFilter) {
        this.otherJoinOnFilter = otherJoinOnFilter;
        return this;
    }

    public IFilter getWhereFilter() {
        return whereFilter;
    }

    public IJoin setWhereFilter(IFilter whereFilter) {
        this.whereFilter = whereFilter;
        return this;
    }

    public String toStringWithInden(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "Join" + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "Join");
        }

        appendField(sb, "leftColumns", this.getLeftJoinOnColumns(), tabContent);
        appendField(sb, "rightColumns", this.getRightJoinOnColumns(), tabContent);

        if (this.leftOuter && this.rightOuter) {
            appendField(sb, "type", "outter join", tabContent);
        }

        if (!this.leftOuter && this.rightOuter) {
            appendField(sb, "type", "right outter join", tabContent);
        }
        if (this.leftOuter && !this.rightOuter) {
            appendField(sb, "type", "left outter join", tabContent);
        }
        if (!this.leftOuter && !this.rightOuter) {
            appendField(sb, "type", "inner join", tabContent);
        }

        appendField(sb, "valueFilter", printFilterString(this.getValueFilter()), tabContent);
        appendField(sb, "otherJoinOnFilter", printFilterString(this.getOtherJoinOnFilter()), tabContent);
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
        appendField(sb, "strategy", this.getJoinStrategy(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        // if(this.getThread()!=null)
        // appendField(sb, "thread",
        // this.getThread(), tabContent);
        // appendField(sb, "requestID",
        // this.getRequestID(), tabContent);
        // appendField(sb, "subRequestID",
        // this.getSubRequestID(), tabContent);

        appendln(sb, tabContent + "left:");
        sb.append(this.getLeftNode().toStringWithInden(inden + 2));

        appendln(sb, tabContent + "right:");
        sb.append(this.getRightNode().toStringWithInden(inden + 2));

        return sb.toString();
    }

}
