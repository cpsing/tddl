package com.taobao.tddl.executor.cursor.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.MockArrayCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.cursor.impl.MergeSortedCursors;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.bean.Column;
import com.taobao.tddl.optimizer.core.expression.bean.OrderBy;

public class MergeSortedCursorTest {

    MockArrayCursor getCursor(String tableName, Integer[] ids) throws TddlException {
        MockArrayCursor cursor = new MockArrayCursor(tableName);
        cursor.addColumn("id", DataType.IntegerType);
        cursor.addColumn("name", DataType.StringType);
        cursor.addColumn("school", DataType.StringType);
        cursor.initMeta();

        for (Integer id : ids) {
            cursor.addRow(new Object[] { id, "name" + id, "school" + id });

        }

        cursor.init();

        return cursor;

    }

    @Test
    public void testSortDuplicated() throws TddlException {

        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] { 1, 3, 5, 8, 8, 9, 10 });

        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] { 2, 2, 4, 5, 6, 7, 7, 9, 9, 10, 13 });
        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);

        List<ISchematicCursor> cursors = new ArrayList();
        cursors.add(new SchematicCursor(mockCursor1, orderBys));
        cursors.add(new SchematicCursor(mockCursor2, orderBys));
        MergeSortedCursors c = new MergeSortedCursors(cursors, true);
        Object[] expected = new Object[] { 1, 2, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 9, 10, 10, 13 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }
        c.close(new ArrayList());
        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor1.isClosed());
    }

    @Test
    public void testSortNotDuplicated() throws TddlException {

        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] { 1, 3, 5, 8, 8, 9, 10 });
        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] { 2, 2, 4, 5, 6, 7, 7, 9, 9, 10, 13 });
        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);

        List<ISchematicCursor> cursors = new ArrayList();
        cursors.add(new SchematicCursor(mockCursor1, orderBys));
        cursors.add(new SchematicCursor(mockCursor2, orderBys));
        MergeSortedCursors c = new MergeSortedCursors(cursors, false);
        Object[] expected = new Object[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }
        c.close(new ArrayList());
        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor1.isClosed());
    }

    @Test
    public void testSortThree() throws TddlException {

        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] { 1, 3, 5, 8, 9, 10 });
        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] { 2, 4, 6, 7, 7, 9, 10, 13 });

        MockArrayCursor mockCursor3 = this.getCursor("T1", new Integer[] { 2, 5, 8, 9 });
        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);

        List<ISchematicCursor> cursors = new ArrayList();
        cursors.add(new SchematicCursor(mockCursor1, orderBys));
        cursors.add(new SchematicCursor(mockCursor2, orderBys));
        cursors.add(new SchematicCursor(mockCursor3, orderBys));
        MergeSortedCursors c = new MergeSortedCursors(cursors, true);
        Object[] expected = new Object[] { 1, 2, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 9, 10, 10, 13 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }
        c.close(new ArrayList());
        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor1.isClosed());
        Assert.assertTrue(mockCursor3.isClosed());
    }

    @Test
    public void testGetOrderBysBeforeNext() throws TddlException {

        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] { 1, 3, 5, 8, 8, 9, 10 });
        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] { 2, 2, 4, 5, 6, 7, 7, 9, 9, 10, 13 });
        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);

        List<ISchematicCursor> cursors = new ArrayList();
        cursors.add(new SchematicCursor(mockCursor1, orderBys));
        cursors.add(new SchematicCursor(mockCursor2, orderBys));
        MergeSortedCursors c = new MergeSortedCursors(cursors, true);

        Assert.assertEquals("[T1.ID, T1.NAME, T1.SCHOOL]", c.getReturnColumns().toString());
        Assert.assertEquals("[OrderBy [columnName=T1.ID, direction=true]]", c.getOrderBy().toString());

    }

    @Test
    public void testGetOrderBysAfterNext() throws TddlException {

        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] { 1, 3, 5, 8, 8, 9, 10 });
        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] { 2, 2, 4, 5, 6, 7, 7, 9, 9, 10, 13 });
        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);

        List<ISchematicCursor> cursors = new ArrayList();
        cursors.add(new SchematicCursor(mockCursor1, orderBys));
        cursors.add(new SchematicCursor(mockCursor2, orderBys));
        MergeSortedCursors c = new MergeSortedCursors(cursors, true);

        c.next();

        Assert.assertEquals("[T1.ID, T1.NAME, T1.SCHOOL]", c.getReturnColumns().toString());
        Assert.assertEquals("[OrderBy [columnName=T1.ID, direction=true]]", c.getOrderBy().toString());

    }

}
