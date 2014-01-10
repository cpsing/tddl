package com.taobao.tddl.qatest.sequence;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;

import com.taobao.tddl.sequence.exception.SequenceException;
import com.taobao.tddl.sequence.impl.DefaultSequence;
import com.taobao.tddl.sequence.impl.DefaultSequenceDao;

/**
 * 对sequence正确性的验证
 * <p/>
 * Created Date: 2010-12-28 下午02:21:04
 */
public class DefaultSequenceTest {

    protected static ClassPathXmlApplicationContext context   = new ClassPathXmlApplicationContext(new String[] { "classpath:sequence/spring_context_default_sequence.xml" });
    protected static DataSource                     normal0DS = (DataSource) context.getBean("normal_0");
    private int                                     step      = 1000;
    protected static DefaultSequence                seque     = null;
    protected static DefaultSequence                sq1       = null;
    protected static DefaultSequence                sq2       = null;
    protected static DefaultSequence                sq3       = null;
    protected static DefaultSequence                sq4       = null;
    protected static DefaultSequence                sq5       = null;
    protected static DefaultSequence                sq6       = null;
    private Set<Long>                               set       = new HashSet<Long>();
    private AtomicInteger                           seqCnt    = new AtomicInteger();

    @BeforeClass
    public static void setup() {
        seque = (DefaultSequence) context.getBean("sequence");
        DefaultSequenceDao sequeDao = (DefaultSequenceDao) context.getBean("sequenceDao");
        sq1 = new DefaultSequence();
        sq1.setName("ni");
        sq1.setSequenceDao(sequeDao);

        sq2 = new DefaultSequence();
        sq2.setName("ni");
        sq2.setSequenceDao(sequeDao);

        sq3 = new DefaultSequence();
        sq3.setName("ni");
        sq3.setSequenceDao(sequeDao);

        sq4 = new DefaultSequence();
        sq4.setName("ni");
        sq4.setSequenceDao(sequeDao);

        sq5 = new DefaultSequence();
        sq5.setName("ni");
        sq5.setSequenceDao(sequeDao);

        sq6 = new DefaultSequence();
        sq6.setName("ni");
        sq6.setSequenceDao(sequeDao);
    }

    /**
     * 验证取到下个值为上个值加1
     * 
     * @throws SequenceException
     */
    @Test
    public void getNextValueTest() throws SequenceException {
        long value = seque.nextValue();
        long nextValue = seque.nextValue();
        Assert.assertEquals(value + 1, nextValue);
    }

    /**
     * 同个线程，验证取下个值为当前加1
     * 
     * @throws SequenceException
     */
    @Test
    public void sameTreadTest() throws SequenceException {
        long value = seque.nextValue();
        DefaultSequence seque1 = (DefaultSequence) context.getBean("sequence");
        long value1 = seque1.nextValue();
        Assert.assertEquals(value + 1, value1);
    }

    /**
     * 不同线程，验证取到的值为上个取到值的下个区间
     * 
     * @throws SequenceException
     */
    @Test
    public void difTreadTest() throws SequenceException {
        long value = sq1.nextValue();
        long value1 = sq2.nextValue();
        Assert.assertEquals(value + step, value1);
    }

    /**
     * 不同线程，上个线程为执行数小于步长情况
     * 
     * @throws SequenceException
     */
    @Test
    public void difTreadInStepTest() throws SequenceException {
        int times = 10;
        long value = 0;
        value = sq3.nextValue();
        for (int i = 0; i < 10; i++) {
            sq3.nextValue();
        }
        long value1 = sq4.nextValue();
        Assert.assertEquals(times % step == 0 ? (times / step) * step + value : (times / step + 1) * step + value,
            value1);
    }

    /**
     * 不同线程，上个本线程执行数大于步长情况
     * 
     * @throws SequenceException
     */
    @Test
    public void difTreadOverStepTest() throws SequenceException {
        int times = 2020;
        long value = 0;
        value = sq5.nextValue();
        for (int i = 1; i < times; i++) {
            sq5.nextValue();
        }
        long value1 = sq6.nextValue();
        Assert.assertEquals(times % step == 0 ? (times / step) * step + value : (times / step + 1) * step + value,
            value1);
    }

    /**
     * 验证从不同的记录的正确性
     * 
     * @throws SequenceException
     */
    @Test
    public void difRecordTest() throws SequenceException {
        long value = 0;
        long temp = 0;
        int times = 10;
        DefaultSequenceDao sequeDao = (DefaultSequenceDao) context.getBean("sequenceDao");
        DefaultSequence sqR = new DefaultSequence();
        sqR.setName("hao");
        sqR.setSequenceDao(sequeDao);
        value = seque.nextValue();
        for (int i = 0; i < times; i++) {
            temp = seque.nextValue();
        }
        Assert.assertEquals(value + times, temp);

        value = sqR.nextValue();
        for (int i = 0; i < times; i++) {
            temp = sqR.nextValue();
        }
        Assert.assertEquals(value + times, temp);
    }

    /**
     * 多个线程去取，验证取到的数据都不一样
     * 
     * @throws SequenceException
     */
    @Test
    public void multiTest() throws SequenceException {
        int times = 100;
        for (int i = 0; i < times; i++) {
            DefaultSequenceDao sequeDao = (DefaultSequenceDao) context.getBean("sequenceDao");
            DefaultSequence sq = new DefaultSequence();
            sq.setName("hao");
            sq.setSequenceDao(sequeDao);
            set.add(sq.nextValue());
        }
        Assert.assertEquals(times, set.size());
    }

    /**
     * 同时起多个线程
     * 
     * @throws SequenceException
     * @throws InterruptedException
     */
    @Test
    public void multiThreadTest() throws SequenceException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(100);
        final CountDownLatch count = new CountDownLatch(1);
        int times = 100;
        for (int i = 0; i < times; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        DefaultSequenceDao sequeDao = (DefaultSequenceDao) context.getBean("sequenceDao");
                        DefaultSequence sq = new DefaultSequence();
                        sq.setName("hao");
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
}
