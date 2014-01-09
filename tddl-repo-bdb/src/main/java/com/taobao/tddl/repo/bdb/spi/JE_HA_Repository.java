package com.taobao.tddl.repo.bdb.spi;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_HA_Repository extends JE_Repository {

    public static final Log logger = LogFactory.getLog(JE_HA_Repository.class.getName());

    // public JE_HA_Repository(CommandExecutorFactory commandExecutorFactory) {
    // super(commandExecutorFactory);
    // }

    // public JE_HA_Repository(ServerConfig config,CommandExecutorFactory
    // commandExecutorFactory) {
    // super(config, commandExecutorFactory);
    // }

    @Override
    public void doInit() {
        File repo_dir = new File(config.getRepoDir());
        repo_dir.delete();// 删除空目录

        ReplicationConfig repConfig = new ReplicationConfig();
        repConfig.setGroupName(config.getGroupName());
        repConfig.setNodeName(config.getNodeName());
        repConfig.setNodePriority(config.getPriority());
        repConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        repConfig.setReplicaAckTimeout(5, TimeUnit.SECONDS);
        // TODO shenxun:无效
        repConfig.setConfigParam(ReplicationConfig.CONSISTENCY_POLICY, "NoConsistencyRequiredPolicy");
        // 设置为1小时
        repConfig.setConfigParam(ReplicationConfig.REP_STREAM_TIMEOUT, "1 H");
        String nodes = config.getGroupNodes();
        // nodeName1:ip:port,
        Map<String, String> nodesMap = new HashMap<String, String>();
        String firstNode = null;
        String[] ss = nodes.split(",");
        for (String s : ss) {
            String[] _s = s.split(":");
            if (_s.length == 3) {
                nodesMap.put(_s[0], _s[1] + ":" + _s[2]);
                if (firstNode == null) {
                    firstNode = _s[0];
                }
            }
        }
        String helperHosts = "";
        if (config.getNodeName().equals(firstNode) && !repo_dir.exists()) {
            StringBuilder sb = new StringBuilder();
            sb.append("I am master Node . My node name is ").append(config.getNodeName());
            sb.append(" ip/port is ").append(nodesMap.get(config.getNodeName()));
            sb.append(", while first node is ").append(firstNode);
            logger.warn(sb.toString());
            helperHosts = nodesMap.get(firstNode);
        } else {

            for (Entry<String, String> entry : nodesMap.entrySet()) {
                helperHosts += entry.getValue() + ",";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("I am not master node , My node name is (")
                .append(config.getNodeName())
                .append("-->")
                .append(nodesMap.get(config.getNodeName()))
                .append(")")
                .append(", first node is (");
            sb.append(firstNode).append("-->").append(nodesMap.get(config.getNodeName())).append(")");
            sb.append(" . helper hosts :").append(helperHosts);
            logger.warn(sb.toString());
        }
        repConfig.setHelperHosts(helperHosts);

        repConfig.setNodeHostPort(nodesMap.get(config.getNodeName()));

        EnvironmentConfig envConfig = new EnvironmentConfig();
        commonConfig(envConfig, config);
        envConfig.setAllowCreate(true);
        String[] _durability = config.getDurability();
        this.durability = new Durability(SyncPolicy.valueOf(_durability[0]),
            SyncPolicy.valueOf(_durability[1]),
            ReplicaAckPolicy.valueOf(_durability[2]));
        envConfig.setCachePercent(config.getCachePercent());
        if (config.isTransactional()) {
            envConfig.setTransactional(config.isTransactional());
            envConfig.setTxnTimeout(config.getTxnTimeout(), TimeUnit.SECONDS);
            envConfig.setDurability(durability);
        }

        if (!repo_dir.exists()) {
            repo_dir.mkdirs();
        }

        int REP_HANDLE_RETRY_MAX = 100;
        for (int i = 0; i < REP_HANDLE_RETRY_MAX; i++) {
            try {
                logger.info(repConfig.getHelperHosts());
                env = new ReplicatedEnvironment(repo_dir,
                    repConfig,
                    envConfig,
                    new NoConsistencyRequiredPolicy(),
                    QuorumPolicy.SIMPLE_MAJORITY);
                logger.info("nodeName:" + config.getNodeName() + ", ReplicatedEnvironment init successful.");
                logger.info("state:" + ((ReplicatedEnvironment) env).getState());
                break;
            } catch (UnknownMasterException ume) {
                try {
                    logger.info("UnknownMasterException,sleep 5s.");
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException ex) {
                    logger.warn("", ex);
                }
                continue;
            } catch (InsufficientLogException ile) {
                /* A network restore is required, make the necessary calls */
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig conf = new NetworkRestoreConfig();
                conf.setRetainLogFiles(false); // delete obsolete log files.
                restore.execute(ile, conf);
                // retry
                continue;
            } /*
               * catch (EnvironmentFailureException efe) { A network restore is
               * required, make the necessary calls
               * System.err.println("catch EnvironmentFailureException"); if
               * (efe.getCause() instanceof InsufficientLogException) {
               * System.err.println("efe is InsufficientLogException");
               * NetworkRestore restore = new NetworkRestore();
               * NetworkRestoreConfig conf = new NetworkRestoreConfig();
               * conf.setRetainLogFiles(true); // delete obsolete log files.
               * restore.execute((InsufficientLogException) efe.getCause(),
               * conf); } else { System.out.println("execute backup... "); for
               * (Entry<String, String> entry : nodesMap.entrySet()) {
               * if(entry.getKey().equals(config.getNodeName())){ continue; }
               * InetSocketAddress addr = new
               * InetSocketAddress(entry.getValue().split(":")[0],
               * Integer.parseInt(entry.getValue().split(":")[1]));
               * System.out.println(addr); FileManager fm = new
               * FileManager(env.getEnvironmentImpl(), repo_dir, false);
               * NetworkBackup backup = new NetworkBackup(addr, repo_dir,
               * NameIdPair.NOCHECK, true, fm); try { backup.execute(); } catch
               * (Exception ex) { ex.printStackTrace(); } }
               * System.out.println("backup end"); } // retry continue; }
               */
            catch (Throwable e) {
                System.err.println("error " + e + ". class" + e.getClass());
            }
        }
        if (env == null) {
            throw new IllegalStateException("getEnvironment: reached max retries");
        }

        cef = new CommandHandlerFactoryBDBImpl();
        cursorFactoryBDBImp = new CursorFactoryBDBImp();

    }

    @Override
    public boolean isWriteAble() {
        return ((ReplicatedEnvironment) env).getState().isMaster();
    }

    public int cleanLog() {
        return env.cleanLog();
    }
}
