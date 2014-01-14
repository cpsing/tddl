package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class Put<RT extends IPut> extends DataNodeExecutor<RT> implements IPut<RT> {

    protected IQueryTree                     queryTree;
    protected List<ISelectable>              columns;
    protected List<Comparable>               values;
    protected PUT_TYPE                       putType;
    protected String                         tableName;        // 真实表名
    protected String                         indexName;        // 逻辑索引信息
    protected boolean                        ignore = false;
    protected List<List<Comparable>>         multiValues;
    protected boolean                        isMutiValues;
    protected Map<Integer, ParameterContext> parameterSettings;

    public Put(){
        putType = PUT_TYPE.REPLACE;
    }

    public IQueryTree getQueryTree() {
        return queryTree;
    }

    public RT setQueryTree(IQueryTree queryTree) {
        this.queryTree = queryTree;
        return (RT) this;
    }

    public RT setUpdateColumns(List<ISelectable> columns) {
        this.columns = columns;
        return (RT) this;
    }

    public List<ISelectable> getUpdateColumns() {
        return columns;
    }

    public RT setTableName(String tableName) {
        this.tableName = tableName;
        return (RT) this;
    }

    public String getTableName() {
        return tableName;
    }

    public RT setUpdateValues(List<Comparable> values) {
        this.values = values;
        return (RT) this;
    }

    public List<Comparable> getUpdateValues() {
        return values;
    }

    public com.taobao.tddl.optimizer.core.plan.IPut.PUT_TYPE getPutType() {
        return putType;
    }

    public RT assignment(Map<Integer, ParameterContext> parameterSettings) {
        IQueryTree qt = getQueryTree();
        if (qt != null) {
            qt.assignment(parameterSettings);
        }

        if (values != null) {
            List<Comparable> comps = new ArrayList<Comparable>(values.size());
            for (Comparable comp : values) {
                if (comp instanceof IBindVal) {
                    comps.add(((IBindVal) comp).assignment(parameterSettings));
                } else {
                    comps.add(comp);

                }
            }
            this.setUpdateValues(comps);
        }
        return (RT) this;
    }

    public RT setIndexName(String indexName) {
        this.indexName = indexName;
        return (RT) this;
    }

    public String getIndexName() {
        return indexName;
    }

    public RT setIgnore(boolean ignore) {
        this.ignore = ignore;
        return (RT) this;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public List<List<Comparable>> getMultiValues() {
        return multiValues;
    }

    public RT setMultiValues(List<List<Comparable>> multiValues) {
        this.multiValues = multiValues;
        return (RT) this;
    }

    public boolean isMutiValues() {
        return isMutiValues;
    }

    public RT setMutiValues(boolean isMutiValues) {
        this.isMutiValues = isMutiValues;
        return (RT) this;
    }

    public int getMuiltValuesSize() {
        if (this.isMutiValues) {
            return this.multiValues.size();
        } else {
            return 1;
        }

    }

    public List<Comparable> getValues(int index) {
        if (this.isMutiValues) {
            return this.multiValues.get(index);
        }

        if (index != 0) {
            throw new NotSupportException("这不可能");
        } else {
            return this.values;
        }
    }

    @Override
    public Map<Integer, ParameterContext> getParameterSettings() {
        return parameterSettings;
    }

    @Override
    public RT setParameterSettings(Map<Integer, ParameterContext> parameterSettings) {
        this.parameterSettings = parameterSettings;
        return (RT) this;
    }

    public void accept(PlanVisitor visitor) {
        if (this instanceof IInsert) {
            visitor.visit((IInsert) this);
        } else if (this instanceof IDelete) {
            visitor.visit((IDelete) this);
        } else if (this instanceof IUpdate) {
            visitor.visit((IUpdate) this);
        } else if (this instanceof IReplace) {
            visitor.visit((IReplace) this);
        }
    }

    public RT copy() {
        return null;
    }

    public void copySelfTo(RT executor) {
        throw new IllegalArgumentException("should not be here");
    }

    public String toString() {
        return this.toStringWithInden(0);
    }

    public String toStringWithInden(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + "Put:" + this.getPutType());
        appendField(sb, "tableName", this.getTableName(), tabContent);
        appendField(sb, "indexName", this.getIndexName(), tabContent);
        appendField(sb, "columns", this.getUpdateColumns(), tabContent);
        appendField(sb, "values", this.getUpdateValues(), tabContent);
        appendField(sb, "requestID", this.getRequestID(), tabContent);
        appendField(sb, "subRequestID", this.getSubRequestID(), tabContent);
        appendField(sb, "thread", this.getThread(), tabContent);
        appendField(sb, "hostname", this.getRequestHostName(), tabContent);

        if (this.getQueryTree() != null) {
            appendln(sb, tabContent + "query:");
            sb.append(this.getQueryTree().toStringWithInden(inden + 2));
        }

        return sb.toString();
    }
}
