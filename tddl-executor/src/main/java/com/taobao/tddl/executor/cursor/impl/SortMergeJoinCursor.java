package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.IANDCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.JoinRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

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
 * @since 5.1.0
 */
@SuppressWarnings("rawtypes")
public class SortMergeJoinCursor extends JoinSchematicCursor implements IANDCursor {

    protected IRowSet current;

    Iterator<IRowSet> resultsIter = null;

    @SuppressWarnings("unchecked")
    public SortMergeJoinCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor, List left_columns,
                               List right_columns, List<IOrderBy> orderBys, List leftColumns, List rightColumns)
                                                                                                                throws TddlException{
        super(left_cursor, right_cursor, left_columns, right_columns, leftColumns, rightColumns);
        this.left_cursor = left_cursor;
        this.right_cursor = right_cursor;

    }

    @Override
    public IRowSet next() throws TddlException {
        if (resultsIter != null && resultsIter.hasNext()) {
            this.current = resultsIter.next();
            return this.current;
        }

        List<IRowSet> leftSubSet = new LinkedList();
        List<IRowSet> rightSubSet = new LinkedList();

        IRowSet left_key = advance(leftSubSet, left_cursor, leftJoinOnColumns);
        IRowSet right_key = advance(rightSubSet, right_cursor, rightJoinOnColumns);

        while (!leftSubSet.isEmpty() && !rightSubSet.isEmpty()) {

            if (compare(left_key, right_key) == 0) {

                List<IRowSet> results = acrossProduct(leftSubSet, rightSubSet);
                resultsIter = results.iterator();

                left_key = advance(leftSubSet, left_cursor, leftJoinOnColumns);
                right_key = advance(rightSubSet, right_cursor, rightJoinOnColumns);

                this.current = resultsIter.next();
                return this.current;

            } else if (compare(left_key, right_key) < 0) {
                left_key = advance(leftSubSet, left_cursor, leftJoinOnColumns);
            } else {
                right_key = advance(rightSubSet, right_cursor, rightJoinOnColumns);
            }
        }

        return null;
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

    private int compare(IRowSet row1, IRowSet row2) {
        return kvPairComparator.compare(row1, row2);
    }

    private IRowSet advance(List<IRowSet> subSet, ISchematicCursor cursor, List<ISelectable> columns)
                                                                                                     throws TddlException {

        if (cursor.current() == null) {
            if (cursor.next() == null) {
                return null;
            }
        }

        IRowSet key = cursor.current();

        while (cursor.next() != null && compare(key, cursor.current()) == 0) {
            subSet.add(cursor.current());
        }

        return key;
    }

    public IRowSet joinRecord(IRowSet kv1, IRowSet kv2) {
        ICursorMeta leftCursorMeta = null;
        ICursorMeta rightCursorMeta = null;
        if (kv1 != null) {
            leftCursorMeta = kv1.getParentCursorMeta();
        }
        if (kv2 != null) {
            rightCursorMeta = kv2.getParentCursorMeta();
        }
        buildSchemaInJoin(leftCursorMeta, rightCursorMeta);
        ;
        IRowSet joinedRowSet = new JoinRowSet(rightCursorOffset, kv1, kv2, joinCursorMeta);
        return joinedRowSet;
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
