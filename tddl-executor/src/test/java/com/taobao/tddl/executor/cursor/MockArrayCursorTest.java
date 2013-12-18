package com.taobao.tddl.executor.cursor;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

public class MockArrayCursorTest {

    @Test
    public void test1() throws TddlException {
        MockArrayCursor cursor = new MockArrayCursor("table1");
        cursor.addColumn("id", DATA_TYPE.INT_VAL);
        cursor.addColumn("name", DATA_TYPE.STRING_VAL);
        cursor.addColumn("school", DATA_TYPE.STRING_VAL);
        cursor.initMeta();

        cursor.addRow(new Object[] { 1, "name1", "school1" });
        cursor.addRow(new Object[] { 2, "name2", "school2" });
        cursor.addRow(new Object[] { 3, "name3", "school3" });
        cursor.addRow(new Object[] { 4, "name4", "school4" });
        cursor.addRow(new Object[] { 5, "name5", "school5" });
        cursor.addRow(new Object[] { 6, "name6", "school6" });
        cursor.addRow(new Object[] { 7, "name7", "school7" });

        cursor.init();

        IRowSet row = null;
        int count = 0;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
            count++;

        }

        Assert.assertEquals(7, count);
    }
}
