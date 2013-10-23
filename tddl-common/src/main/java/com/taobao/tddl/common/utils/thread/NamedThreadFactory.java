package com.taobao.tddl.common.utils.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可以产生命名的线程，方便查找问题
 * 
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2010-12-28下午02:05:23
 */
public class NamedThreadFactory implements ThreadFactory {

    static final AtomicInteger poolNumber   = new AtomicInteger();
    final AtomicInteger        threadNumber = new AtomicInteger();
    final ThreadGroup          group;
    final String               namePrefix;
    final boolean              isDaemon;

    public NamedThreadFactory(){
        this("pool");
    }

    public NamedThreadFactory(String prefix){
        this(prefix, false);
    }

    public NamedThreadFactory(String prefix, boolean daemon){
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
        isDaemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        t.setDaemon(isDaemon);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
