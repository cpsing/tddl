package com.taobao.tddl.qatest.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.qatest.BaseTestCase;

public class PrepareData extends BaseTestCase {

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void normaltblPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);

        String sql = "REPLACE INTO " + normaltblTableName
                     + " (pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) VALUES(?,?,?,?,?,?,?)";
        con.setAutoCommit(false);
        ps = con.prepareStatement(sql);
        andorPs = andorCon.prepareStatement(sql);
        for (int i = start; i < end / 2; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i % 4 * 100);
            ps.setObject(2, i % 4 * 100);
            andorPs.setObject(3, gmtDay);
            ps.setObject(3, gmtDay);
            andorPs.setObject(4, gmt);
            ps.setObject(4, gmt);
            andorPs.setObject(5, gmt);
            ps.setObject(5, gmt);
            andorPs.setObject(6, name);
            ps.setObject(6, name);
            andorPs.setObject(7, 1.1);
            ps.setObject(7, 1.1);
            andorPs.execute();
            ps.addBatch();
        }

        for (int i = end / 2; i < end - 1; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            ps.setObject(2, i * 100);
            andorPs.setObject(3, gmtDayNext);
            ps.setObject(3, gmtDayNext);
            andorPs.setObject(4, gmtNext);
            ps.setObject(4, gmtNext);
            andorPs.setObject(5, gmtNext);
            ps.setObject(5, gmtNext);
            andorPs.setObject(6, newName);
            ps.setObject(6, newName);
            andorPs.setObject(7, 1.1);
            ps.setObject(7, 1.1);
            andorPs.execute();
            ps.addBatch();
        }

        for (int i = end - 1; i < end; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            ps.setObject(2, i * 100);
            andorPs.setObject(3, gmtDayBefore);
            ps.setObject(3, gmtDayBefore);
            andorPs.setObject(4, gmtBefore);
            ps.setObject(4, gmtBefore);
            andorPs.setObject(5, gmtBefore);
            ps.setObject(5, gmtBefore);
            andorPs.setObject(6, name1);
            ps.setObject(6, name1);
            andorPs.setObject(7, (float) (i * 0.01));
            ps.setObject(7, (float) (i * 0.01));
            andorPs.execute();
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();

    }

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据，部分name字段插入数据为null
     */
    public void normaltblNullPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);

        String sql = "REPLACE INTO " + normaltblTableName
                     + " (pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) VALUES(?,?,?,?,?,?,?)";
        con.setAutoCommit(false);
        ps = con.prepareStatement(sql);
        PreparedStatement andorPs = andorCon.prepareStatement(sql);
        for (int i = start; i < end / 2; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            ps.setObject(2, i * 100);
            andorPs.setObject(3, gmtDay);
            ps.setObject(3, gmtDay);
            andorPs.setObject(4, gmt);
            ps.setObject(4, gmt);
            andorPs.setObject(5, gmt);
            ps.setObject(5, gmt);
            andorPs.setObject(6, name);
            ps.setObject(6, name);
            andorPs.setObject(7, 1.1);
            ps.setObject(7, 1.1);
            andorPs.execute();
            ps.addBatch();
        }

        for (int i = end / 2; i < end - 1; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            ps.setObject(2, i * 100);
            andorPs.setObject(3, gmtNext);
            ps.setObject(3, gmtNext);
            andorPs.setObject(4, gmtNext);
            ps.setObject(4, gmtNext);
            andorPs.setObject(5, gmtNext);
            ps.setObject(5, gmtNext);
            andorPs.setObject(6, null);
            ps.setObject(6, null);
            andorPs.setObject(7, 1.1);
            ps.setObject(7, 1.1);
            andorPs.execute();
            ps.addBatch();
        }

        for (int i = end - 1; i < end; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            ps.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            ps.setObject(2, i * 100);
            andorPs.setObject(3, gmtBefore);
            ps.setObject(3, gmtBefore);
            andorPs.setObject(4, gmtBefore);
            ps.setObject(4, gmtBefore);
            andorPs.setObject(5, gmtBefore);
            ps.setObject(5, gmtBefore);
            andorPs.setObject(6, name1);
            ps.setObject(6, name1);
            andorPs.setObject(7, (float) (i * 0.01));
            ps.setObject(7, (float) (i * 0.01));
            andorPs.execute();
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();

    }

    /**
     * hostinfo表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void hostinfoPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + host_info, null);
        mysqlUpdateData("delete from  " + host_info, null);
        try {
            String sql = "replace into " + host_info + "(host_id,host_name,hostgroup_id,station_id) values(?,?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);

            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);

                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, "hostname" + i);
                andorPs.setObject(2, "hostname" + i);
                ps.setObject(3, Long.parseLong(i + ""));
                andorPs.setObject(3, Long.parseLong(i + ""));
                ps.setObject(4, "station" + i / 2);
                andorPs.setObject(4, "station" + i / 2);
                andorPs.execute();
                ps.addBatch();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
        ;
    }

    /**
     * 插入hostinfo表数据，其中start为插入起始数据，end为插入结束数据 groupidValue为字段hostgroup_id的值
     */
    public void hostinfoDataAdd(int start, int end, long groupidValue) throws Exception, SQLException {
        try {
            String sql = "replace into " + host_info + "(host_id,host_name,hostgroup_id,station_id) values(?,?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);

            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);

                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, "hostname" + i);
                andorPs.setObject(2, "hostname" + i);
                ps.setObject(3, groupidValue);
                andorPs.setObject(3, groupidValue);
                ps.setObject(4, "station" + i / 2);
                andorPs.setObject(4, "station" + i / 2);
                andorPs.execute();
                ps.addBatch();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
    }

    public void hostgroupPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + hostgroup, null);
        mysqlUpdateData("delete from  " + hostgroup, null);
        try {
            String sql = "replace into " + hostgroup
                         + "(hostgroup_id,hostgroup_name,module_id,station_id) values(?,?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, "hostgroupname" + i);
                andorPs.setObject(2, "hostgroupname" + i);
                ps.setObject(3, Long.parseLong(i + ""));
                andorPs.setObject(3, Long.parseLong(i + ""));
                ps.setObject(4, "station" + i / 2);
                andorPs.setObject(4, "station" + i / 2);

                ps.addBatch();
                andorPs.execute();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
    }

    /**
     * 插入hostinfo表数据 start为插入起始数据，end为插入结束数据 其中moduleIdValue为字段module_id的值
     */
    public void hostgroupDataAdd(int start, int end, long moduleIdValue) throws Exception, SQLException {
        try {
            String sql = "replace into " + hostgroup
                         + "(hostgroup_id,hostgroup_name,module_id,station_id) values(?,?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, "hostgroupname" + i);
                andorPs.setObject(2, "hostgroupname" + i);
                ps.setObject(3, moduleIdValue);
                andorPs.setObject(3, moduleIdValue);
                ps.setObject(4, "station" + i / 2);
                andorPs.setObject(4, "station" + i / 2);
                ps.addBatch();
                andorPs.execute();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
    }

    /**
     * hostgroupInfo表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void hostgroupInfoPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + hostgroup_info, null);
        mysqlUpdateData("delete from  " + hostgroup_info, null);
        try {
            String sql = "replace into " + hostgroup_info + "(hostgroup_id,hostgroup_name,station_id) values(?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, "hostgroupname" + i);
                andorPs.setObject(2, "hostgroupname" + i);
                ps.setObject(3, "station" + i / 2);
                andorPs.setObject(3, "station" + i / 2);
                ps.addBatch();
                andorPs.execute();
            }

            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
        ;
    }

    /**
     * module_info表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void module_infoPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + module_info, null);
        mysqlUpdateData("delete from  " + module_info, null);
        try {
            String sql = "replace into " + module_info + "(module_id,product_id,module_name) values(?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = 0; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, Long.parseLong(i + ""));
                andorPs.setObject(2, Long.parseLong(i + ""));
                ps.setObject(3, "module" + i);
                andorPs.setObject(3, "module" + i);
                ps.addBatch();
                andorPs.execute();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
    }

    /**
     * module_host表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void module_hostPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from " + module_host, null);
        mysqlUpdateData("delete from  " + module_host, null);
        try {
            String sql = "replace into " + module_host + "(id,module_id,host_id) values(?,?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, Long.parseLong(i / 2 + ""));
                andorPs.setObject(2, Long.parseLong(i / 2 + ""));
                ps.setObject(3, Long.parseLong(i % 3 + ""));
                andorPs.setObject(3, Long.parseLong(i % 3 + ""));
                ps.addBatch();
                andorPs.execute();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
    }

    /**
     * 为like测试准备单独的2条normaltbl表数据
     */
    public void normaltblTwoPrepare() throws Exception {
        String sql = "REPLACE INTO " + normaltblTableName + "(pk,name,gmt_create) VALUES(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.clear();
        param.add(20l);
        param.add(name1);
        param.add(gmtDay);
        andorUpdateData(sql, param);
        mysqlUpdateData(sql, param);

        sql = "REPLACE INTO " + normaltblTableName + "(pk,name,gmt_create) VALUES(?,?,?)";
        param.clear();
        param.add(21l);
        param.add(name2);
        param.add(gmtDay);
        andorUpdateData(sql, param);
        mysqlUpdateData(sql, param);
    }

    /**
     * student表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void studentPrepare(int start, int end) throws Exception, SQLException {
        andorUpdateData("delete from  " + studentTableName, null);
        mysqlUpdateData("delete from  " + studentTableName, null);
        try {
            String sql = "replace into  " + studentTableName + " (id,name) values(?,?)";
            con.setAutoCommit(false);
            ps = con.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                andorPs = andorCon.prepareStatement(sql);
                ps.setObject(1, Long.parseLong(i + ""));
                andorPs.setObject(1, Long.parseLong(i + ""));
                ps.setObject(2, name);
                andorPs.setObject(2, name);
                ps.addBatch();
                andorPs.execute();
            }
            ps.executeBatch();
            con.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
    }
}
