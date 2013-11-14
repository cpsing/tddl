package com.taobao.tddl.optimizer.core.expression;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.IRowSet;

/**
 * 描述一个列信息，可能会是字段列，函数列，常量列<br>
 * 使用RT泛型解决子类流式API需要的返回结果为子类
 * 
 * @author jianghang 2013-11-8 上午11:40:50
 * @since 5.1.0
 */
public interface ISelectable<RT extends ISelectable> extends CanVisit, Comparable {

    // --------------- map/reduce计算支持------------------
    /**
     * 就是给一行数据，他给你一个结果，对于列来说就是直接返回该列，对于行来说就是返回该行
     * 
     * <pre>
     * 场景：
     * 1. scalar函数计算，比如substring(column)，给定一个参数列，返回一个值
     * 2. 分布式场景，比如sum请求，会先发送map请求到所有数据节点，每个数据节点本地进行reduce的sum计算，并返回单一结果
     *    最后由客户端收集所有数据节点的结果，再进行reduce运算，得出sum的最终结果
     * </pre>
     */
    public void serverMap(IRowSet kvPair) throws Exception;

    /**
     * 就是给一批数据，他只给你特定的一条结果
     * 
     * <pre>
     * 场景：
     * 1. aggregate函数计算，比如count(*)，给定一个参数列，返回一个聚合值
     * 2. 分布式场景，比如sum请求，会先发送map请求到所有数据节点，每个数据节点本地进行reduce的sum计算，并返回单一结果
     *    最后由客户端收集所有数据节点的结果，再进行reduce运算，得出sum的最终结果
     * </pre>
     */
    public void serverReduce(IRowSet kvPair) throws Exception;

    /**
     * 参数赋值，计算过程获取parameter参数获取?对应的数据
     * 
     * <pre>
     * a. 比如原始sql中使用了?占位符，通过PrepareStatement.setXXX设置参数
     * b. sql解析后构造了，将?解析为IBindVal对象
     */
    public RT assignment(Map<Integer, ParameterContext> parameterSettings);

    // ----------------- data_type -----------------

    public static enum DATA_TYPE {
        BYTES_VAL, LONG_VAL, SHORT_VAL, BOOLEAN_VAL, CHAR_VAL, STRING_VAL, FLOAT_VAL, DOUBLE_VAL, INT_VAL, BIND_VAL,
        DATE_VAL, TIMESTAMP, BLOB
    }

    public RT setDataType(DATA_TYPE dataType);

    public DATA_TYPE getDataType();

    /**
     * 获取column/function的结果
     */
    public Object getResult();

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
