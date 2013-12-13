import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.matrix.config.ConfigHolder;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class ConfigHolderTest {

    @Test
    public void initTestWithConfigHolder() throws TddlException {

        ConfigHolder configHolder = new ConfigHolder();
        configHolder.setAppName("andor_show");
        configHolder.setTopologyFilePath("test_matrix.xml");
        configHolder.setSchemaFilePath("test_schema.xml");

        try {
            configHolder.init();
        } catch (TddlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        MatrixExecutor me = new MatrixExecutor();
        ResultCursor rc = me.execute("select * from bmw_users", new ExecutionContext());

        IRowSet row = null;
        while ((row = rc.next()) != null) {
            System.out.println(row);
        }
    }

    @Test
    public void initTestWithDS() throws TddlException, SQLException {

        TDataSource ds = new TDataSource();
        ds.setAppName("andor_show");
        ds.setMachineTopologyFile("test_matrix.xml");
        ds.setSchemaFile("test_schema.xml");

        ds.init();

        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement("select * from bmw_users limit 10");
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {

                String key = rs.getMetaData().getColumnName(i);
                Object val = rs.getObject(i);
                sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" + val + "]");
            }
            System.out.println(sb.toString());
        }

    }

}
