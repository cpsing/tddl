package com.taobao.tddl.optimizer.core.expression;

import java.util.Map;

import com.taobao.tddl.common.utils.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.IRowSet;
import com.taobao.tddl.optimizer.core.PlanVisitor;

/**
 * 描述一个列信息，可能会是字段列，函数列，常量列<br>
 * 使用RT泛型解决子类流式API需要的返回结果为子类
 * 
 * @author jianghang 2013-11-8 上午11:40:50
 * @since 5.1.0
 */
public interface ISelectable<RT extends ISelectable> extends PlanVisitor, Comparable {

    // --------------- map/reduce计算支持------------------
    /**
     * 就是给一行数据，他给你一个结果，对于列来说就是直接返回该列，对于行来说就是返回该行
     * 
     * @param args
     * @return
     * @throws Exception
     */
    public void serverMap(IRowSet kvPair) throws Exception;

    public void serverReduce(IRowSet kvPair) throws Exception;

    public RT assignment(Map<Integer, ParameterContext> parameterSettings);

    // ----------------- data_type -----------------

    public static enum DATA_TYPE {
        BYTES_VAL, LONG_VAL, SHORT_VAL, BOOLEAN_VAL, CHAR_VAL, STRING_VAL, FLOAT_VAL, DOUBLE_VAL, INT_VAL, BIND_VAL,
        DATE_VAL, TIMESTAMP, BLOB
    }

    public RT setDataType(DATA_TYPE data_type);

    public DATA_TYPE getDataType();

    // --------------- name相关信息 ----------------------

    /**
     * alias ，只在一个表（索引）对象结束的时候去处理 一般情况下，取值不需要使用这个东西。 直接使用columnName即可。
     */
    public String getAlias();

    public String getTableName();

    public String getName();

    public RT setAlias(String alias);

    public RT setTableName(String tableName);

    public RT setName(String name);

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
    // TODO 需要改名为clone
    public RT copy();

    // TODO 需要改名为cloneInstance
    public RT getNewInstance();
}
