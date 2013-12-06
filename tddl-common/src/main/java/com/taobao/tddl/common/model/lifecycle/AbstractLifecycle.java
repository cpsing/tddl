package com.taobao.tddl.common.model.lifecycle;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author jianghang 2013-10-23 下午5:19:38
 * @since 5.1.0
 */
public class AbstractLifecycle implements Lifecycle {

    private final Object       lock     = new Object();
    protected volatile boolean isInited = false;

    public void init() throws TddlException {
        synchronized (lock) {
            if (isInited()) {
                return;
            }
            isInited = true;
            doInit();
        }
    }

    public void destory() throws TddlException {
        synchronized (lock) {
            if (!isInited()) {
                return;
            }

            doDestory();
            isInited = false;
        }
    }

    public boolean isInited() {
        return isInited;
    }

    protected void doInit() throws TddlException {
    }

    protected void doDestory() throws TddlException {
    }

}
