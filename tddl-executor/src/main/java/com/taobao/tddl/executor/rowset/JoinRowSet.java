package com.taobao.tddl.executor.rowset;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:05:48
 * @since 5.0.0
 */
public class JoinRowSet extends AbstractRowSet implements IRowSet {

    private int     rightCursorOffset;
    private IRowSet leftRowSet;
    private IRowSet rightRowSet;

    public JoinRowSet(int rightCursorOffset, IRowSet leftRowSet, IRowSet rightRowSet, ICursorMeta iCursorMeta){
        super(iCursorMeta);
        this.rightCursorOffset = rightCursorOffset;
        this.leftRowSet = leftRowSet;
        this.rightRowSet = rightRowSet;
    }

    public int getRightCursorOffset() {
        return rightCursorOffset;
    }

    public void setRightCursorOffset(int rightCursorOffset) {
        this.rightCursorOffset = rightCursorOffset;
    }

    public IRowSet getLeftRowSet() {
        return leftRowSet;
    }

    public void setLeftRowSet(IRowSet leftRowSet) {
        this.leftRowSet = leftRowSet;
    }

    public IRowSet getRightRowSet() {
        return rightRowSet;
    }

    public void setRightRowSet(IRowSet rightRowSet) {
        this.rightRowSet = rightRowSet;
    }

    @Override
    public Object getObject(int index) {
        if (rightCursorOffset <= index) {
            if (rightRowSet == null) return null;
            return rightRowSet.getObject(index - rightCursorOffset);
        }

        if (leftRowSet == null) return null;
        return leftRowSet.getObject(index);
    }

    @Override
    public void setObject(int index, Object value) {
        if (rightCursorOffset <= index) {
            rightRowSet.setObject(index - rightCursorOffset, value);
            return;
        }
        leftRowSet.setObject(index, value);
    }

    @Override
    public List<Object> getValues() {
        ArrayList<Object> values = new ArrayList<Object>();
        if (leftRowSet == null && rightRowSet == null) {
            int size = this.getParentCursorMeta().getColumns().size();
            for (int i = 0; i < size; i++) {
                values.add(null);
            }
        } else if (rightRowSet == null) {
            values.addAll(leftRowSet.getValues());
            for (int i = 0; i < this.getParentCursorMeta().getColumns().size() - leftRowSet.getValues().size(); i++)
                values.add(null);

        } else if (leftRowSet == null) {

            for (int i = 0; i < this.getParentCursorMeta().getColumns().size() - rightRowSet.getValues().size(); i++)
                values.add(null);
            values.addAll(rightRowSet.getValues());

        } else {
            values.addAll(leftRowSet.getValues());
            values.addAll(rightRowSet.getValues());
        }
        return values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ColumnMeta cm : this.getParentCursorMeta().getColumns()) {
            Integer index = this.getParentCursorMeta().getIndex(cm.getTableName(), cm.getName());
            sb.append(cm.getName() + ":" + this.getValues().get(index) + " ");
        }
        return sb.toString();
    }

}
