package com.taobao.tddl.common.model.lifecycle;

/**
 * @author jianghang 2013-10-23 下午5:19:38
 * @since 5.1.0
 */
public class AbstractLifecycle implements Lifecycle {

    private final Object       lock     = new Object();
    protected volatile boolean isInited = false;

    public void init() {
        synchronized (lock) {
            if (isInited()) {
                return;
            }
            isInited = true;
            init();
        }
    }

    public void destory() {
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

    protected void doInit() {
    }

    protected void doDestory() {
    }

}
