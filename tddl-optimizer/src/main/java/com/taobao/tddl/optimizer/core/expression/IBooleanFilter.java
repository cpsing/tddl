package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

/**
 * boolean filter.
 * 
 * <pre>
 * 例子：
 * a. column > 100
 * b. count(id) > 100
 * </pre>
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author jianghang 2013-11-8 下午2:01:21
 * @since 5.0.0
 */
public interface IBooleanFilter extends IFilter<IBooleanFilter> {

    @Override
    public IBooleanFilter setOperation(OPERATION operation);

    public Object getColumn();

    public IBooleanFilter setColumn(Object column);

    public Object getValue();

    public IBooleanFilter setValue(Object value);

    /**
     * 多个value，出现id in ()
     */
    public List<Object> getValues();

    public IBooleanFilter setValues(List<Object> values);

}
