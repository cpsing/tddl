package com.taobao.tddl.optimizer.core.ast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * DML操作树
 * 
 * @author jianghang 2013-11-8 下午2:34:39
 * @since 5.1.0
 */
public abstract class DMLNode<RT extends DMLNode> extends ASTNode<RT> {

    protected static final Logger            logger            = LoggerFactory.getLogger(DMLNode.class);
    protected List<ISelectable>              columns;
    protected List<Comparable>               values;
    protected QueryTreeNode                  qtn               = null;
    protected Map<Integer, ParameterContext> parameterSettings = null;
    protected boolean                        needBuild         = true;

    public DMLNode(QueryTreeNode qtn){
        this.qtn = qtn;
    }

    public DMLNode setParameterSettings(Map<Integer, ParameterContext> parameterSettings) {
        this.parameterSettings = parameterSettings;
        return this;
    }

    public QueryTreeNode getQueryTreeNode() {
        return this.qtn;
    }

    public DMLNode setQueryTreeNode(QueryTreeNode qtn) {
        this.qtn = qtn;
        return this;
    }

    public DMLNode setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getColumns() {
        return this.columns;
    }

    public DMLNode setValues(List<Comparable> values) {
        this.values = values;
        return this;

    }

    public List<Comparable> getValues() {
        return this.values;
    }

    public TableMeta getTableMeta() {
        return null;
    }

    public boolean isNeedBuild() {
        return needBuild;
    }

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

    public void build() {
        if (this.qtn != null) {
            qtn.build();
        }

        if ((this.getColumns() == null || this.getColumns().isEmpty())
            && (this.getValues() == null || this.getValues().isEmpty())) {
            return;
        }

        if (columns == null || columns.isEmpty()) { // 如果字段为空，默认为所有的字段数据
            columns = columnMetaListToIColumnList(this.getTableMeta().getAllColumns(), this.getTableMeta()
                .getTableName());
        }

        if (columns.size() != values.size()) {
            if (!columns.isEmpty()) {
                throw new IllegalArgumentException("The size of the columns and values is not matched."
                                                   + " columns' size is " + columns.size() + ". values' size is "
                                                   + values.size());
            }

        }

        for (ISelectable s : this.getColumns()) {
            ISelectable res = null;
            for (Object obj : qtn.getColumnsReferedForParent()) {
                ISelectable querySelected = (ISelectable) obj;
                if ((querySelected.getAlias() != null && s.getColumnName().equals(querySelected.getAlias()))
                    || s.getColumnName().equals(querySelected.getColumnName())) { // 尝试查找对应的字段信息
                    res = querySelected;
                    break;
                }
            }

            if (res == null) {
                throw new IllegalArgumentException("column: " + s.getColumnName() + " is not existed in either "
                                                   + qtn.getName() + " or select clause");
            }
        }

        convertTypeToSatifyColumnMeta(this.getColumns(), this.getValues());
    }

    /**
     * 将columnMeta转化为column列
     */
    protected List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms, String tableName) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m, tableName));
        }

        return cs;
    }

    protected IColumn columnMetaToIColumn(ColumnMeta m, String tableName) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(tableName);
        return c;
    }

    /**
     * 尝试根据字段类型进行value转化
     */
    protected List<Comparable> convertTypeToSatifyColumnMeta(List<ISelectable> cs, List<Comparable> vs) {
        for (int i = 0; i < cs.size(); i++) {
            Comparable c = cs.get(i);
            Comparable v = vs.get(i);
            DATA_TYPE type = null;
            if (v == null || v instanceof IBindVal || v instanceof NullValue) {
                continue;
            }

            if (c instanceof ISelectable) {
                type = ((ISelectable) c).getDataType();
            }

            vs.set(i, (Comparable) OptimizerUtils.convertType(v, type));
        }
        return vs;
    }

    public RT executeOn(String dataNode) {
        super.executeOn(dataNode);
        return (RT) this;
    }

    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        QueryTreeNode qct = getQueryTreeNode();

        if (qct != null) {
            qct.assignment(parameterSettings);
        }

        if (values != null) {
            List<Comparable> comps = new ArrayList<Comparable>(values.size());
            for (Comparable comp : values) {
                if (comp instanceof IBindVal) {
                    comps.add(((IBindVal) comp).assignment(parameterSettings));
                } else if (comp instanceof ISelectable) {
                    comps.add(((ISelectable) comp).assignment(parameterSettings));
                } else {
                    comps.add(comp);
                }
            }

            this.setValues(comps);
        }

    }

    protected void copySelfTo(DMLNode to) {
        to.columns = this.columns;
        to.values = this.values;
        to.qtn = this.qtn;
    }

    protected void deepCopySelfTo(DMLNode to) {
        to.columns = OptimizerUtils.deepCopySelectableList(this.columns);
        if (this.values != null) {
            to.values = new ArrayList(this.values.size());

            for (Comparable value : this.values) {
                if (value instanceof ISelectable) {
                    to.values.add(((ISelectable) value).copy());
                } else to.values.add(value);
            }
        }

        to.qtn = (QueryTreeNode) this.qtn.deepCopy();
    }

    public String toString(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        OptimizerToString.appendln(sb, tabTittle + this.getClass().getSimpleName());

        OptimizerToString.appendField(sb, "columns", this.getColumns(), tabContent);
        OptimizerToString.appendField(sb, "values", this.getValues(), tabContent);

        if (this.getQueryTreeNode() != null) {
            OptimizerToString.appendln(sb, tabContent + "query:");
            sb.append(this.getQueryTreeNode().toString(inden + 1));
        }
        return sb.toString();
    }
}
