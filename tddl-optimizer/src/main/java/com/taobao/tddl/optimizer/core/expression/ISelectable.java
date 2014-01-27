package com.taobao.tddl.optimizer.core.expression;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 描述一个列信息，可能会是字段列，函数列，常量列<br>
 * 使用RT泛型解决子类流式API需要的返回结果为子类
 * 
 * @since 5.0.0
 */
public interface ISelectable<RT extends ISelectable> extends CanVisit, Comparable {

    /**
     * 参数赋值，计算过程获取parameter参数获取?对应的数据
     * 
     * <pre>
     * a. 比如原始sql中使用了?占位符，通过PrepareStatement.setXXX设置参数
     * b. sql解析后构造了，将?解析为IBindVal对象
     */
    public RT assignment(Map<Integer, ParameterContext> parameterSettings);

    // public static enum DATA_TYPE {
    // BYTES_VAL, LONG_VAL, SHORT_VAL, BOOLEAN_VAL, CHAR_VAL, STRING_VAL,
    // FLOAT_VAL, DOUBLE_VAL, INT_VAL, BIND_VAL,
    // DATE_VAL, TIMESTAMP_VAL, TIME_VAL, BLOB_VAL, BIT_VAL
    // }

    public RT setDataType(DataType dataType);

    public DataType getDataType();

    // --------------- name相关信息 ----------------------

    /**
     * alias ，只在一个表（索引）对象结束的时候去处理 一般情况下，取值不需要使用这个东西。 直接使用columnName即可。
     */
    public String getAlias();

    public String getTableName();

    public String getColumnName();

    public RT setAlias(String alias);

    public RT setTableName(String tableName);

    public RT setColumnName(String columnName);

    /**
     * 是否为相同的名字，如果目标的alias name存在则对比alias name，否则对比column name
     */
    public boolean isSameName(ISelectable select);

    /**
     * @return tableName + name
     */
    public String getFullName();

    // -----------------特殊标记------------------

    public boolean isDistinct();

    public boolean isNot();

    public RT setDistinct(boolean distinct);

    public RT setIsNot(boolean isNot);

    // -----------------对象复制 -----------------

    public RT copy();

}
