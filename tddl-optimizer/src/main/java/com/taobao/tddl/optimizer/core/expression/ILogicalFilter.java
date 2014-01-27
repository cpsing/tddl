package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

/**
 * logical expression . usually they are "and" and "or" filter
 * 
 * @author whisper
 * @author jianghang 2013-11-8 下午2:04:20
 * @since 5.0.0
 */
public interface ILogicalFilter extends IFilter<ILogicalFilter> {

    public List<IFilter> getSubFilter();

    public IFilter getLeft();

    public IFilter getRight();

    public IFilter setLeft(IFilter left);

    public IFilter setRight(IFilter Right);

    public ILogicalFilter setSubFilter(List<IFilter> subFilters);

    public ILogicalFilter addSubFilter(IFilter subFilter);
}
