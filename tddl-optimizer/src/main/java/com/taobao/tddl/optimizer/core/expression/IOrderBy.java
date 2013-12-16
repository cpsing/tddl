package com.taobao.tddl.optimizer.core.expression;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @since 5.1.0
 */
public interface IOrderBy extends CanVisit {

    public IOrderBy setColumn(ISelectable columnName);

    public ISelectable getColumn();

    public Boolean getDirection();

    public IOrderBy assignment(Map<Integer, ParameterContext> parameterSettings);

    public IOrderBy setDirection(Boolean direction);

    public String getAlias();

    public IOrderBy setTableName(String alias);

    public IOrderBy setColumnName(String alias);

    public String getTableName();

    public String getColumnName();

    public DATA_TYPE getDataType();

    public String toStringWithInden(int inden);

    public IOrderBy copy();

}
