package com.taobao.tddl.common.utils.mbean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TddlMBeanServer {

    private static final Logger                                              log            = LoggerFactory.getLogger(TddlMBeanServer.class);
    private static final String                                              LogPrefix      = "[TddlMBeanServer]";
    private MBeanServer                                                      mbs            = null;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>> idMap          = new ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>>();
    private ReentrantLock                                                    lock           = new ReentrantLock();

    private static ConcurrentHashMap<String, ObjectName>                     beanNameHolder = new ConcurrentHashMap<String, ObjectName>();

    public static boolean                                                    shutDownMBean  = true;

    private static class Holder {

        private static final TddlMBeanServer instance = new TddlMBeanServer();
    }

    private TddlMBeanServer(){
        // 创建MBServer
        String hostName = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            hostName = addr.getHostName();
        } catch (IOException e) {
            log.error(LogPrefix + "Get HostName Error", e);
            hostName = "localhost";
        }

        String host = System.getProperty("hostName", hostName);
        try {
            boolean useJmx = Boolean.parseBoolean(System.getProperty("tddl.useJMX", "true"));
            if (useJmx) {
                mbs = ManagementFactory.getPlatformMBeanServer();
                int port = Integer.parseInt(System.getProperty("tddl.rmi.port", "6679"));
                String rmiName = System.getProperty("tddl.rmi.name", "tddlJmxServer");
                Registry reg = null;
                try {
                    reg = LocateRegistry.getRegistry(port);
                    reg.list();
                } catch (Exception e) {
                    reg = null;
                }
                if (null == reg) {
                    reg = LocateRegistry.createRegistry(port);
                }
                reg.list();
                String serverURL = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/" + rmiName;
                JMXServiceURL url = new JMXServiceURL(serverURL);
                final JMXConnectorServer connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url,
                    null,
                    mbs);
                connectorServer.start();
                Runtime.getRuntime().addShutdownHook(new Thread() {

                    public void run() {
                        try {
                            System.err.println("JMXConnector stop");
                            connectorServer.stop();
                        } catch (IOException e) {
                            log.error(LogPrefix + e);
                        }
                    }
                });
                log.warn(LogPrefix + "jmx url: " + serverURL);
            }
        } catch (Exception e) {
            log.error(LogPrefix + "create MBServer error", e);
        }
    }

    public static void registerMBean(Object o, String name) {
        if (!shutDownMBean) {
            Holder.instance.registerMBean0(o, name);
        }
    }

    public static void registerMBeanWithId(Object o, String id) {
        if (!shutDownMBean) {
            Holder.instance.registerMBeanWithId0(o, id);
        }
    }

    public static void registerMBeanWithIdPrefix(Object o, String idPrefix) {
        if (!shutDownMBean) {
            Holder.instance.registerMBeanWithIdPrefix0(o, idPrefix);
        }
    }

    private void registerMBean0(Object o, String name) {
        // 注册MBean
        if (beanServerExist()) {
            String beanName = o.getClass().getPackage().getName() + ":type=" + o.getClass().getSimpleName()
                              + (null == name ? (",id=" + o.hashCode()) : (",name=" + name + "-" + o.hashCode()));
            realRegisterMBean(o, beanName);
        }
    }

    private void registerMBeanWithId0(Object o, String id) {
        // 注册MBean
        if (null == id || id.length() == 0) {
            throw new IllegalArgumentException("must set id");
        }
        if (beanServerExist()) {
            String name = o.getClass().getPackage().getName() + ":type=" + o.getClass().getSimpleName() + ",id=" + id;
            realRegisterMBean(o, name);
        }
    }

    private String getId(String name, String idPrefix) {
        ConcurrentHashMap<String, AtomicLong> subMap = idMap.get(name);
        if (null == subMap) {
            lock.lock();
            try {
                subMap = idMap.get(name);
                if (null == subMap) {
                    subMap = new ConcurrentHashMap<String, AtomicLong>();
                    idMap.put(name, subMap);
                }
            } finally {
                lock.unlock();
            }
        }

        AtomicLong indexValue = subMap.get(idPrefix);
        if (null == indexValue) {
            lock.lock();
            try {
                indexValue = subMap.get(idPrefix);
                if (null == indexValue) {
                    indexValue = new AtomicLong(0);
                    subMap.put(idPrefix, indexValue);
                }
            } finally {
                lock.unlock();
            }
        }
        long value = indexValue.incrementAndGet();
        String result = idPrefix + "-" + value;
        return result;
    }

    private void registerMBeanWithIdPrefix0(Object o, String idPrefix) {
        // 注册MBean
        if (beanServerExist()) {
            if (null == idPrefix || idPrefix.length() == 0) {
                idPrefix = "default";
            }
            idPrefix = idPrefix.replace(":", "-");

            String id = this.getId(o.getClass().getName(), idPrefix);
            String name = o.getClass().getPackage().getName() + ":type=" + o.getClass().getSimpleName() + ",id=" + id;

            realRegisterMBean(o, name);
        }
    }

    private void realRegisterMBean(Object o, String name) {
        try {
            ObjectName objectName = new ObjectName(name);
            realRegisterMBean(o, objectName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private boolean beanServerExist() {
        return mbs != null;
    }

    private synchronized void realRegisterMBean(Object o, ObjectName objectName) throws InstanceAlreadyExistsException,
                                                                                MBeanRegistrationException,
                                                                                NotCompliantMBeanException {
        ObjectName old = beanNameHolder.putIfAbsent(objectName.getCanonicalName(), objectName);
        if (old == null) {
            mbs.registerMBean(o, objectName);
            return;
        }
        log.error("same mbean try to register to BeanServer, reject! bean id is : " + objectName.getCanonicalName());
    }

    public void unRegisterAllSelfMBean() throws MBeanRegistrationException, InstanceNotFoundException {
        for (ObjectName objectName : beanNameHolder.values()) {
            if (beanServerExist()) mbs.unregisterMBean(objectName);
        }
    }

    public static void removeAllMBean() throws MBeanRegistrationException, InstanceNotFoundException {
        Holder.instance.unRegisterAllSelfMBean();
    }

}
