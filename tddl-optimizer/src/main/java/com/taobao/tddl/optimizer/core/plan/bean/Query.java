package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class Query extends QueryTree implements IQuery {

    protected IFilter    keyFilter;
    protected LOCK_MODEL lockModel = LOCK_MODEL.SHARED_LOCK;
    protected String     tableName;
    protected String     indexName;
    protected IQueryTree subQuery;

    @Override
    public IFilter getKeyFilter() {
        return keyFilter;
    }

    @Override
    public IQuery setKeyFilter(IFilter keyFilter) {
        this.keyFilter = keyFilter;
        return this;
    }

    @Override
    public LOCK_MODEL getLockModel() {
        return lockModel;
    }

    @Override
    public IQuery setLockModel(LOCK_MODEL lockModel) {
        this.lockModel = lockModel;
        return this;
    }

    @Override
    public IQuery setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public IQuery setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    @Override
    public IQueryTree assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);

        IQueryTree iqc = getSubQuery();
        if (iqc != null) {
            iqc.assignment(parameterSettings);
        }

        IFilter kf = getKeyFilter();
        if (kf != null) {
            kf.assignment(parameterSettings);
        }

        return this;

    }

    @Override
    public IQuery setSubQuery(IQueryTree subQuery) {
        this.subQuery = subQuery;
        return this;
    }

    @Override
    public IQueryTree getSubQuery() {
        return subQuery;
    }

    @Override
    public IQuery copy() {
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        copySelfTo((QueryTree) query);
        query.setLockModel(this.getLockModel());
        query.setSubQuery(this.getSubQuery());
        query.setTableName(this.getTableName());
        query.setKeyFilter(this.getKeyFilter());
        return query;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getTableName() != null) {
            if (this.getAlias() != null) {
                appendln(sb, tabTittle + "Query from " + this.getIndexName() + " as " + this.getAlias());
            } else {
                appendln(sb, tabTittle + "Query from " + this.getIndexName());
            }
        } else {
            if (this.getAlias() != null) {
                appendln(sb, tabTittle + "Query" + " as " + this.getAlias());
            } else {
                appendln(sb, tabTittle + "Query");
            }
        }
        appendField(sb, "tableName", this.getTableName(), tabContent);
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter()), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getValueFilter()), tabContent);
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
        appendField(sb, "lockModel", this.getLockModel(), tabContent);
        appendField(sb, "columns", this.getColumns(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);

        appendField(sb, "sql", this.getSql(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        // appendField(sb, "requestID",
        // this.getRequestID(), tabContent);
        // appendField(sb, "subRequestID",
        // this.getSubRequestID(), tabContent);
        // if (this.getThread() != null)
        // appendField(sb, "thread",
        // this.getThread(), tabContent);

        if (this.getSubQuery() != null) {
            appendln(sb, tabContent + "from:");
            sb.append(this.getSubQuery().toStringWithInden(inden + 2));
        }

        return sb.toString();
    }

}
