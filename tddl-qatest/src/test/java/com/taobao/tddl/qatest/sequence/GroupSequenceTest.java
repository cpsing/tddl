package com.taobao.tddl.qatest.sequence;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.sequence.exception.SequenceException;
import com.taobao.tddl.sequence.impl.GroupSequence;
import com.taobao.tddl.sequence.impl.GroupSequenceDao;

/**
 * @author yaolingling.pt
 */
public class GroupSequenceTest extends BaseMatrixTestCase {

    protected static ClassPathXmlApplicationContext context = null;
    protected static GroupSequence                  seque   = null;
    private Set<Long>                               set     = new HashSet<Long>();
    private AtomicInteger                           seqCnt  = new AtomicInteger();

    @BeforeClass
    public static void setUp() throws Exception {
        MockServer.setUpMockServer();
        setMatrixMockInfo(MATRIX_DBGROUPS_PATH, TDDL_DBGROUPS);
    }

    @Before
    public void init() throws Exception {
        context = new ClassPathXmlApplicationContext(new String[] { "classpath:sequence/spring_context_group_sequence.xml" });
        seque = (GroupSequence) context.getBean("sequence");
    }

    /**
     * @throws SequenceException
     */
    @Test
    public void getNextValueTest() throws SequenceException {
        long value = seque.nextValue();
        long nextValue = seque.nextValue();
        Assert.assertEquals(value + 1, nextValue);
    }

    /**
     * @throws Exception
     */
    @Test
    public void nextValueTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();
        long value = seque.nextValue();
        Assert.assertTrue(value == 301 || value == 201);
    }

    /**
     * @throws SequenceException
     */
    @Test
    public void sameTreadTest() throws SequenceException {
        long value = seque.nextValue();
        GroupSequence seque1 = (GroupSequence) context.getBean("sequence");
        long value1 = seque1.nextValue();
        Assert.assertEquals(value + 1, value1);
    }

    /**
     * *
     * 
     * @throws SequenceException
     */
    @Test
    public void difTreadTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();
        long value = seque.nextValue();
        Assert.assertTrue(value == 301 || value == 201);
        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);
        long value1 = seque.nextValue();
        Assert.assertTrue(value1 == 201 || value1 == 301 || value1 == 401 || value1 == 501);
    }

    /**
     * @throws Exception
     */
    @Test
    public void greaterStepTest() throws Exception {

        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();

        long value = 0l;
        for (int i = 0; i < 150; i++) {
            value = seque.nextValue();
        }
        Assert.assertTrue(value == 250 || value == 350 || value == 450 || value == 550);
    }

    /**
     * @throws Exception
     */
    @Test
    public void lessStepTest() throws Exception {

        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();

        long value = 0l;
        for (int i = 0; i < 50; i++) {
            value = seque.nextValue();
        }
        Assert.assertTrue(value == 250 || value == 350);

    }

    /**
     * @throws Exception
     */
    @Test
    public void startWith0GetTwoValueLessStep() throws Exception {

        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        stmt.close();
        con.close();

        Long value = seque.nextValue();

        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);

        value = seque.nextValue();
        int key1 = 0;
        int key2 = 0;
        con = getConnection("qatest_normal_0");
        stmt = (Statement) con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key1 = rs.getInt(2);
        }
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key2 = rs.getInt(2);
        }

        int a = (key1 / 100) % 2;
        int b = (key2 / 100) % 2;
        Assert.assertFalse(a == b);
    }

    /**
     * @throws Exception
     */
    @Test
    public void statrWith0GreaterStep() throws Exception {

        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        stmt.close();
        con.close();

        Long value = 0l;
        for (int i = 0; i < 150; i++) {
            value = seque.nextValue();
        }
        value = seque.nextValue();

        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);

        value = seque.nextValue();
        int key1 = 0;
        int key2 = 0;
        con = getConnection("qatest_normal_0");
        stmt = (Statement) con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key1 = rs.getInt(2);
        }
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key2 = rs.getInt(2);
        }

        int a = (key1 / 100) % 2;
        int b = (key2 / 100) % 2;
        Assert.assertFalse(a == b);
    }

    /**
     * @throws SequenceException
     */
    @Test
    public void multiTest() throws SequenceException {
        int times = 100;
        for (int i = 0; i < times; i++) {
            GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
            GroupSequence sq = new GroupSequence();
            sq.setName("ni");
            sq.setSequenceDao(sequeDao);
            set.add(sq.nextValue());
        }
        Assert.assertEquals(times, set.size());
    }

    /**
     * @throws SequenceException
     * @throws InterruptedException
     */
    @Test
    public void multiThreadTwoDbTest() throws SequenceException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(100);
        final CountDownLatch count = new CountDownLatch(1);
        int times = 20;
        for (int i = 0; i < times; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
                        GroupSequence sq = new GroupSequence();
                        sq.setName("ni");
                        sq.setSequenceDao(sequeDao);
                        set.add(sq.nextValue());
                        seqCnt.getAndIncrement();
                    } catch (DataAccessException e) {
                    } catch (SequenceException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        count.countDown();
        while (seqCnt.get() < times) {
            TimeUnit.MICROSECONDS.sleep(10);
        }
        Assert.assertEquals(times, set.size());
    }

    /**
     * @throws SequenceException
     * @throws InterruptedException
     */
    @Test
    public void multiThreadOneDbTest() throws SequenceException, InterruptedException {

        ExecutorService es = Executors.newFixedThreadPool(100);
        final CountDownLatch count = new CountDownLatch(1);
        int times = 15;
        for (int i = 0; i < times; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao_one_db");
                        GroupSequence sq = new GroupSequence();
                        sq.setName("ni");
                        sq.setSequenceDao(sequeDao);
                        set.add(sq.nextValue());
                        seqCnt.getAndIncrement();
                    } catch (DataAccessException e) {
                    } catch (SequenceException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        count.countDown();
        while (seqCnt.get() < times) {
            TimeUnit.MICROSECONDS.sleep(10);
        }
        Assert.assertEquals(times, set.size());
    }

    public Connection getConnection(String db) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://10.232.31.118:3306/" + db;
            String user = "tddl";
            String passWord = "tddl";
            conn = (Connection) DriverManager.getConnection(url, user, passWord);
            if (conn != null) {
                System.out.println("conn is null!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

}
