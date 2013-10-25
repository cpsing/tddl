package com.taobao.tddl.config.diamond;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * diamond 实现
 * 
 * @author shenxun
 */
public class DiamondConfigDataHandler implements ConfigDataHandler {

    private final Executor executor = Executors.newCachedThreadPool(new ThreadFactory() {

                                        @Override
                                        public Thread newThread(Runnable r) {
                                            Thread thd = new Thread(r);
                                            thd.setName("andor_diamond_subscriber_executor");
                                            return thd;
                                        }
                                    });

    public DiamondConfigDataHandler(DiamondManager diamondManager){
        this.diamondManager = diamondManager;
    }

    private final DiamondManager diamondManager;

    public String getData(long timeout) {
        return diamondManager.getAvailableConfigureInfomation(timeout);
    }

    public void addListener(final ConfigDataListener configDataListener) {

        diamondManager.setManagerListener(new ManagerListener() {

            public void receiveConfigInfo(String date) {
                configDataListener.onDataRecieved(date);
            }

            public Executor getExecutor() {
                return executor;
            }
        });
    }

}
