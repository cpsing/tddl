package com.taobao.tddl.executor.cursor.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.MockArrayCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.cursor.impl.TempTableSortCursor;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.CursorFactoryDefaultImpl;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.bean.Column;
import com.taobao.tddl.optimizer.core.expression.bean.OrderBy;

public class TempTableCursorTest {

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
    public void testSort() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP);
        ICursorFactory cf = new CursorFactoryDefaultImpl();
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);
        TempTableSortCursor c = new TempTableSortCursor(cf,
            bdbRepo,
            subCursor,
            orderBys,
            true,
            0,
            new ExecutionContext());
        Object[] expected = new Object[] { 1, 2, 3, 4, 5, 5 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor.isClosed());
    }

    @Test
    public void testNull() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP);
        ICursorFactory cf = new CursorFactoryDefaultImpl();

        SchematicCursor subCursor = new SchematicCursor(this.getCursor("T1",
            new Integer[] { 5, null, 4, 3, 2, null, 1 }));

        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);
        TempTableSortCursor c = new TempTableSortCursor(cf,
            bdbRepo,
            subCursor,
            orderBys,
            true,
            0,
            new ExecutionContext());
        Object[] expected = new Object[] { 1, 2, 3, 4, 5, null, null };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        Assert.assertArrayEquals(expected, actual.toArray());

    }

    @Test
    public void testEmpty() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP);
        ICursorFactory cf = new CursorFactoryDefaultImpl();

        SchematicCursor subCursor = new SchematicCursor(this.getCursor("T1", new Integer[] {}));

        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);
        TempTableSortCursor c = new TempTableSortCursor(cf,
            bdbRepo,
            subCursor,
            orderBys,
            true,
            0,
            new ExecutionContext());
        Object[] expected = new Object[] {};
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void testGetReturnColumnsBeforeNext() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP);
        ICursorFactory cf = new CursorFactoryDefaultImpl();
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);
        TempTableSortCursor c = new TempTableSortCursor(cf,
            bdbRepo,
            subCursor,
            orderBys,
            true,
            0,
            new ExecutionContext());
        Assert.assertEquals("[T1.ID, T1.NAME, T1.SCHOOL]", c.getReturnColumns().toString());
        Assert.assertEquals("[OrderBy [columnName=T1.ID, direction=true]]", c.getOrderBy().toString());

    }

    @Test
    public void testGetReturnColumnsAfterNext() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP);
        ICursorFactory cf = new CursorFactoryDefaultImpl();
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        IOrderBy order = new OrderBy();
        order.setColumn(new Column().setColumnName("ID").setTableName("T1").setDataType(DataType.IntegerType));
        List<IOrderBy> orderBys = new ArrayList();

        orderBys.add(order);
        TempTableSortCursor c = new TempTableSortCursor(cf,
            bdbRepo,
            subCursor,
            orderBys,
            true,
            0,
            new ExecutionContext());

        while (c.next() != null)
            ;

        Assert.assertEquals("[T1.ID, T1.NAME, T1.SCHOOL]", c.getReturnColumns().toString());
        Assert.assertEquals("[OrderBy [columnName=T1.ID, direction=true]]", c.getOrderBy().toString());
    }

}
