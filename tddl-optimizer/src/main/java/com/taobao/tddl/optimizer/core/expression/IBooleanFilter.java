package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

/**
 * boolean filter . BooleanFilter represent boolean operation .ex. Column > 100
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author jianghang 2013-11-8 下午2:01:21
 * @since 5.1.0
 */
public interface IBooleanFilter extends IFilter<IBooleanFilter> {

    public IBooleanFilter setOperation(OPERATION operation);

    public Comparable getColumn();

    public IBooleanFilter setColumn(Comparable column);

    public Comparable getValue();

    public List<Comparable> getValues();

    public IBooleanFilter setValues(List<Comparable> values);

    public IBooleanFilter setValue(Comparable value);

}
