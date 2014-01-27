package com.taobao.tddl.optimizer.utils.range;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @author jianghang 2013-11-13 下午4:08:47
 * @since 5.0.0
 */
public class AndRangeProcessor extends AbstractRangeProcessor {

    private final Comparable    column;
    private Range               wholeRange   = null;
    private final List<IFilter> otherFilters = new ArrayList();
    boolean                     emptySet     = false;

    public AndRangeProcessor(Comparable c){
        this.column = c;
    }

    @Override
    public boolean process(IFilter f) {
        if (f != null && ((IBooleanFilter) f).getValue() instanceof ISelectable) {
            // 不处理column = column的filter
            otherFilters.add(f);
            return true;
        }

        if (!(((IBooleanFilter) f).getColumn() instanceof ISelectable)) {
            // 不处理value=value的filter
            otherFilters.add(f);
            return true;
        }

        Range range = getRange((IBooleanFilter) f);
        // 类似like noteq等操作符
        if (range == null) {
            otherFilters.add(f);
            return true;
        }

        if (this.wholeRange == null) {
            this.wholeRange = range;
            return true;
        }

        // 若有交集，则交
        // 否则为空集，直接返回
        if (wholeRange.intersects(range)) {
            wholeRange = wholeRange.intersect(range);
            return true;
        } else {
            emptySet = true;
            return false;
        }

    }

    public List<IFilter> toFilterList() {
        if (this.emptySet) {
            return Lists.newLinkedList();
        }

        List<IFilter> boolNodes = new LinkedList();
        boolNodes.addAll(otherFilters);
        boolNodes.addAll(buildFilter(wholeRange, column));
        return boolNodes;
    }
}
