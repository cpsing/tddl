package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.IMergeSortJoinCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * <pre>
 * sort merge join. 
 * 实现 支持inner join，left join，right join，outter join
 * 要求两边均按照join列完全有序 
 * <a>http://en.wikipedia.org/wiki/Sort-merge_join</a>
 * 
 * 
 *  function sortMerge(relation left, relation right, attribute a)
 *      var relation output
 *      var list left_sorted := sort(left, a) // Relation left sorted on attribute a
 *      var list right_sorted := sort(right, a)
 *      var attribute left_key, right_key
 *      var set left_subset, right_subset // These sets discarded except where join predicate is satisfied
 *      advance(left_subset, left_sorted, left_key, a)
 *      advance(right_subset, right_sorted, right_key, a)
 *      while not empty(left_subset) and not empty(right_subset)
 *          if left_key = right_key // Join predicate satisfied
 *              add cross product of left_subset and right_subset to output
 *              advance(left_subset, left_sorted, left_key, a)
 *              advance(right_subset, right_sorted, right_key, a)
 *          else if left_key < right_key
 *             advance(left_subset, left_sorted, left_key, a)
 *          else // left_key > right_key
 *             advance(right_subset, right_sorted, right_key, a)
 *      return output
 * 
 *  // Remove tuples from sorted to subset until the sorted[1].a value changes
 *  function advance(subset out, sorted inout, key out, a in)
 *      key := sorted[1].a
 *      subset := emptySet
 *      while not empty(sorted) and sorted[1].a = key
 *          insert sorted[1] into subset
 *          remove sorted[1]
 * </pre>
 * 
 * @author mengshi.sunmengshi 2013-12-18 下午2:13:29
 * @since 5.0.0
 */
@SuppressWarnings("rawtypes")
public class SortMergeJoinCursor extends JoinSchematicCursor implements IMergeSortJoinCursor {

    protected IRowSet            current;

    private Iterator<IRowSet>    resultsIter      = null;

    private IRowSet              left_key;

    private IRowSet              right_key;

    private boolean              needAdvanceLeft  = true;

    private boolean              needAdvanceRight = true;

    private List                 leftSubSet;

    private List                 rightSubSet;

    private List<List<IOrderBy>> joinOrderbys;

    public SortMergeJoinCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor, List leftJoinOnColumns,
                               List rightJoinOnColumns) throws TddlException{
        super(left_cursor, right_cursor, leftJoinOnColumns, rightJoinOnColumns);
        this.left_cursor = left_cursor;
        this.right_cursor = right_cursor;
        // 暂时以右表的顺序为准，因为目前选择sort merge join主要是针对outter右表存在排序字段
        // 后续需要优化orderbys信息，针对sort merge join，左右表的顺序字段都是正确的
        this.orderBys = right_cursor.getOrderBy();
        this.joinOrderbys = new ArrayList<List<IOrderBy>>();
        joinOrderbys.add(left_cursor.getOrderBy());
        joinOrderbys.add(right_cursor.getOrderBy());
    }

    public SortMergeJoinCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor, List leftJoinOnColumns,
                               List rightJoinOnColumns, IJoin join) throws TddlException{
        this(left_cursor, right_cursor, leftJoinOnColumns, rightJoinOnColumns);
        setLeftRightJoin(join);
    }

    @Override
    public IRowSet next() throws TddlException {
        if (resultsIter != null && resultsIter.hasNext()) {
            this.current = resultsIter.next();
            return this.current;
        }

        // right join情况下，若上轮迭代没有匹配，则没有消耗leftSubSet，不需要前移
        if (needAdvanceLeft) {
            this.leftSubSet = new LinkedList();
            left_key = advance(leftSubSet, left_cursor, leftJoinOnColumns);
        }

        // left join情况下，若上轮迭代没有匹配，则没有消耗rightSubSet，不需要前移
        if (needAdvanceRight) {
            this.rightSubSet = new LinkedList();
            right_key = advance(rightSubSet, right_cursor, rightJoinOnColumns);
        }

        while (!leftSubSet.isEmpty() && !rightSubSet.isEmpty()) {
            int compare = compare(left_key, right_key, leftJoinOnColumns, rightJoinOnColumns);
            if (compare == 0) {
                this.needAdvanceLeft = true;
                this.needAdvanceRight = true;
                List<IRowSet> results = acrossProduct(leftSubSet, rightSubSet);
                resultsIter = results.iterator();
                this.current = resultsIter.next();
                return this.current;
            } else {
                // outter join情况下，没有消耗就不需要前移
                if (this.isLeftOutJoin() || this.isRightOutJoin()) {
                    needAdvanceLeft = false;
                    needAdvanceRight = false;
                }

                if (compare < 0) {
                    if (this.isLeftOutJoin()) {
                        this.needAdvanceLeft = true;
                        List<IRowSet> results = acrossProduct(leftSubSet,
                            getNullSubSet(right_cursor.getReturnColumns()));
                        resultsIter = results.iterator();
                        this.current = resultsIter.next();
                        return this.current;
                    }
                    left_key = advance(leftSubSet, left_cursor, leftJoinOnColumns);
                } else {
                    if (this.isRightOutJoin()) {
                        this.needAdvanceRight = true;
                        List<IRowSet> results = acrossProduct(getNullSubSet(left_cursor.getReturnColumns()),
                            rightSubSet);
                        resultsIter = results.iterator();
                        this.current = resultsIter.next();
                        return this.current;
                    }
                    right_key = advance(rightSubSet, right_cursor, rightJoinOnColumns);
                }
            }
        }

        if (!(leftSubSet.isEmpty() && rightSubSet.isEmpty())) {
            // outter join情况下，要将两个cursor都取完
            if (leftSubSet.isEmpty() && this.isRightOutJoin()) {
                this.needAdvanceRight = true;
                List<IRowSet> results = acrossProduct(getNullSubSet(left_cursor.getReturnColumns()), rightSubSet);
                resultsIter = results.iterator();
                this.current = resultsIter.next();
                return this.current;
            }

            if (rightSubSet.isEmpty() && this.isLeftOutJoin()) {
                this.needAdvanceLeft = true;
                List<IRowSet> results = acrossProduct(leftSubSet, getNullSubSet(right_cursor.getReturnColumns()));
                resultsIter = results.iterator();
                this.current = resultsIter.next();
                return this.current;
            }
        }

        current = null;
        return current;
    }

    private List<IRowSet> getNullSubSet(List columns) {
        List subSet = new ArrayList(1);
        List value = new ArrayList(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            value.add(null);
        }
        ArrayRowSet row = new ArrayRowSet(CursorMetaImp.buildNew(columns), value.toArray());
        subSet.add(row);

        return subSet;
    }

    /**
     * 两边做笛卡尔积，返回
     * 
     * @param leftSubSet
     * @param rightSubSet
     * @return
     */
    private List<IRowSet> acrossProduct(List<IRowSet> leftSubSet, List<IRowSet> rightSubSet) {
        List<IRowSet> results = new ArrayList<IRowSet>(leftSubSet.size() * rightSubSet.size());

        for (IRowSet left : leftSubSet) {
            for (IRowSet right : rightSubSet) {
                results.add(joinRecord(left, right));
            }
        }

        return results;

    }

    private int compare(IRowSet row1, IRowSet row2, List columns1, List columns2) {
        Comparator kvPairComparator = ExecUtils.getComp(columns1,
            columns2,
            row1.getParentCursorMeta(),
            row2.getParentCursorMeta());
        return kvPairComparator.compare(row1, row2);
    }

    private IRowSet advance(List<IRowSet> subSet, ISchematicCursor cursor, List columns) throws TddlException {
        subSet.clear();
        if (cursor.current() == null) {
            if (cursor.next() == null) {
                return null;
            }
        }

        // 这里的数据要固化下来，否则next之后数据就丢了
        IRowSet key = ExecUtils.fromIRowSetToArrayRowSet(cursor.current());
        subSet.add(key);

        while (cursor.next() != null && compare(key, cursor.current(), columns, columns) == 0) {
            subSet.add(ExecUtils.fromIRowSetToArrayRowSet(cursor.current()));
        }

        return key;
    }

    @Override
    public List<List<IOrderBy>> getJoinOrderBys() {
        return joinOrderbys;
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String subQueryTab = GeneralUtil.getTab(inden);
        ExecUtils.printMeta(joinCursorMeta, inden, sb);
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(subQueryTab).append("【Sort Merge Join : ").append("\n");
        sb.append(subQueryTab).append("leftCursor:").append("\n");
        sb.append(left_cursor.toStringWithInden(inden + 1));
        sb.append(subQueryTab).append("rightCursor:").append("\n");
        sb.append(right_cursor.toStringWithInden(inden + 1));

        return sb.toString();
    }
}
