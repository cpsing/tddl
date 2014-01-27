package com.taobao.tddl.optimizer.utils.range;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;

/**
 * 比如条件 a>10 || a=5 ||a < 1 min=1 max=10 equalList=[5]
 * 
 * @author jianghang 2013-11-13 下午4:23:29
 * @since 5.0.0
 */
public class OrRangeProcessor extends AbstractRangeProcessor {

    private boolean                   fullSet      = false;
    private final List<List<IFilter>> otherFilters = new ArrayList();
    private final List<Range>         ranges       = new LinkedList<Range>();
    private final Object              column;

    public OrRangeProcessor(Object c){
        this.column = c;
    }

    public boolean isFullSet() {
        return fullSet;
    }

    @Override
    public boolean process(IFilter node) {
        if (isFullSet()) {
            return true;
        }

        Range range = getRange((IBooleanFilter) node);
        if (range == null) {
            List<IFilter> f = new ArrayList();
            f.add(node);
            otherFilters.add(new ArrayList(f));
        }

        for (int i = 0; i < ranges.size(); i++) {
            Range rangeExisted = ranges.get(i);
            if (rangeExisted.intersects(range)) {
                // 两个区间有交际，但又互不是包含关系，则为全集，比如 a < 2 or a > 1
                if (!rangeExisted.contains(range) && !range.contains(rangeExisted)) {
                    this.fullSet = true;
                    return true;
                } else {
                    // 若存在包含关系，则取并集
                    ranges.set(i, rangeExisted.union(range));
                    return true;
                }
            } else {
                // 如果不相交，则判断是否是=，并判断能否合并，比如 A > 5 , A = 5
                if (range.isSingleValue()) {
                    if (range.getMaxValue().equals(rangeExisted.getMinValue())
                        || range.getMaxValue().equals(rangeExisted.getMaxValue())) {
                        ranges.set(i, rangeExisted.union(range));
                        return true;
                    }
                }

                if (rangeExisted.isSingleValue()) {
                    if (rangeExisted.getMaxValue().equals(range.getMinValue())
                        || rangeExisted.getMaxValue().equals(range.getMaxValue())) {
                        ranges.set(i, rangeExisted.union(range));
                        return true;
                    }
                }
            }

        }

        // 若跟已存在的哪个区间都没有交集，则为独立的区间，加到range列表里
        if (range != null) {
            ranges.add(range);
        }

        return true;
    }

    public List<List<IFilter>> toFilterList() {
        List<List<IFilter>> DNFNodes = new LinkedList();
        if (this.isFullSet()) {
            return new LinkedList<List<IFilter>>();
        }

        DNFNodes.addAll(this.otherFilters);
        for (Range range : ranges) {
            DNFNodes.add(buildFilter(range, column));
        }
        return DNFNodes;
    }

}
