//
///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.taobao.ustore.repo.je;
//
//import com.sleepycat_ustore_5034.je.*;
//import java.io.File;
//import java.util.Arrays;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//<<<<<<< .working
//=======
//import com.sleepycat_ustore_5055.je.CacheMode;
//import com.sleepycat_ustore_5055.je.Database;
//import com.sleepycat_ustore_5055.je.DatabaseConfig;
//import com.sleepycat_ustore_5055.je.DatabaseEntry;
//import com.sleepycat_ustore_5055.je.Environment;
//import com.sleepycat_ustore_5055.je.EnvironmentConfig;
//>>>>>>> .merge-right.r2107
//import com.taobao.ustore.common.config.schema.internal.ColumnMeta;
//import com.taobao.ustore.common.config.schema.internal.IndexMeta;
//import com.taobao.ustore.common.config.schema.internal.IndexType;
//import com.taobao.ustore.common.config.schema.internal.TableSchema;
//import com.taobao.ustore.common.inner.CodecFactory;
//import com.taobao.ustore.common.inner.DataEncoder;
//import com.taobao.ustore.common.inner.bean.CloneableRecord;
//import com.taobao.ustore.common.inner.bean.IColumn.DATA_TYPE;
//import com.taobao.ustore.spi.ServerConfig;
//import com.taobao.ustore.spi.Table;
//
///**
// *
// * @author jianxing <jianxing.qx@taobao.com> 
// */
//public class TestBDB {
//    public static void main(String[] args) {
//        File f = new File("/tmp/bdb/test34");
//        if(!f.exists()){
//            f.mkdirs();
//        }
//        EnvironmentConfig envConfig = new EnvironmentConfig();
//        envConfig.setAllowCreate(true);
//        envConfig.setTransactional(true);
//        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "256");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_EVICT_BYTES, (1024*1024*2)+"");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_NODES_PER_SCAN, "10");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY, "false");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_FORCED_YIELD, "true");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS, Runtime.getRuntime().availableProcessors()+"");
//        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, Runtime.getRuntime().availableProcessors()+"");
//        envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, 1024*1024*200+"");
//        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION, "true");
//        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, Runtime.getRuntime().availableProcessors()+"");
//        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, 1024*8+"");
//        envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE, 1024*1024+"");
//        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, 1024*1024*200+"");
//        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE, "1024");
//        envConfig.setConfigParam(EnvironmentConfig.LOG_USE_WRITE_QUEUE, "true");
//        envConfig.setConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE, 1024*1024+"");
//        //envConfig.setConfigParam(EnvironmentConfig.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION, "false");
//        envConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE,1024*8+"");
//        envConfig.setConfigParam(EnvironmentConfig.LOCK_TIMEOUT,500*1000+"");
//        envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT,"true");
//                //envConfig.setCachePercent(5);
//        envConfig.setCacheSize(1024*1024*20);
//        //envConfig.setCacheSize(1024*1024*40);
//       //envConfig.setCachePercent(50);
//        Environment env = new Environment(f, envConfig);
//        DatabaseConfig conf = new DatabaseConfig();
//        
//        //conf.setDeferredWrite(true);
//        conf.setCacheMode(CacheMode.DEFAULT);
//        conf.setAllowCreate(true);
//        conf.setSortedDuplicates(true);
//        
//        //conf.setBtreeComparator(new ByteComparator());
//        Database db = env.openDatabase(null, "test1", conf);
//        //env.evictMemory();
//        /*SecondaryConfig sconf = new SecondaryConfig();
//        sconf.setDeferredWrite(false);
//        sconf.setAllowCreate(true);
//        
//        sconf.setSortedDuplicates(true);
//        //sconf.setCacheMode(CacheMode.EVICT_LN);
//        sconf.setKeyCreator(new SecondaryKeyCreator() {
//
//            @Override
//            public boolean createSecondaryKey(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
//                result.setData(data.getData());
//                return true;
//            }
//        });
//        SecondaryDatabase sdb = env.openSecondaryDatabase(null, "test1.second", db, sconf);
//        */
//        DatabaseEntry key = new DatabaseEntry();
//        key.setData(new byte[1]);
//        DatabaseEntry value = new DatabaseEntry();
//        value.setData(new byte[1]);
//        long time = System.currentTimeMillis();
//        db.put(null, key, value);
//        TransactionConfig tc = new TransactionConfig();
//        tc.setReadCommitted(true);
//        Transaction t = env.beginTransaction(null, tc);
//        Cursor c = db.openCursor(t, CursorConfig.READ_COMMITTED);
//        c.getSearchKey(key, value, LockMode.READ_COMMITTED);
//        Transaction t1 = env.beginTransaction(null, tc);
//        db.put(t1, key, value);
//        System.out.println("end");
//        
//        //Cursor c = db.openCursor(null, CursorConfig.DEFAULT);
//        //c.setCacheMode(CacheMode.MAKE_COLD);
//        /*int max = 10000*7;
//        for(int i=1;i<=max;i++){     
//            //c.put(key, value);
//            byte[] bytes = new byte[4];
//            DataEncoder.encode(i, bytes, 0);
//            //value.setData(bytes);
//            db.put(null, key, value);
//            //db.delete(null, key);
//            if(i % 1000 == 0){
//                System.out.println(i);
//                //db.sync();
//            }
//        }
//        System.out.println(db.count());
//        
//        //c.close();
//        db.close();
//        //sdb.close();
//        env.close();
//        System.out.println((System.currentTimeMillis() - time)/1000);*/
//        //env.close();
//    }
//    
//    
//    
////    public static void main1(String[] args) throws Exception {
////        //System.setProperty(null, null)
////            String tableName = "test";
////            TableSchema s = new TableSchema(tableName);
////            ColumnMeta id = new ColumnMeta(tableName,"id", DATA_TYPE.LONG_VAL);
////            ColumnMeta name = new ColumnMeta(tableName,"name", DATA_TYPE.STRING_VAL);
////            IndexMeta primaryIndex = new IndexMeta(tableName, new ColumnMeta[]{id}, IndexType.BTREE, new ColumnMeta[]{name}, 0,false);
////            s.setPrimaryIndex(primaryIndex);
////            IndexMeta secondaryIndex = new IndexMeta(tableName, new ColumnMeta[]{name}, IndexType.BTREE, new ColumnMeta[]{id}, 0,false);
////            s.addSecondaryIndex(secondaryIndex);
////            ServerConfig conf = new ServerConfig();
////            conf.setRepoDir("/tmp/bdb/test62");
////            
////            conf.setCommitSync(false);
////            conf.setTransactional(true);
////            JE_Repository repo = new JE_Repository(conf); 
////            Table table = repo.getTable(s);
////            CloneableRecord _id = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(id)).newEmptyRecord();
////            CloneableRecord _name = CodecFactory.getInstance(CodecFactory.AVRO).getCodec(Arrays.asList(name)).newEmptyRecord();
////            long max = 10000*5;
////            long time = System.currentTimeMillis();
////            CountDownLatch cdl = new CountDownLatch(5);
////            //Transaction txn = repo.beginTransaction();
////            ExecutorService exec = Executors.newCachedThreadPool();
////            exec.submit(new R(table,0l,max,_id,_name,cdl));
////            exec.submit(new R(table,max,max*2,_id,_name,cdl));
////            exec.submit(new R(table,max*2,max*3,_id,_name,cdl));
////            exec.submit(new R(table,max*3,max*4,_id,_name,cdl));
////            exec.submit(new R(table,max*5,max*6,_id,_name,cdl));
////            //txn.commit();
////            cdl.await();
////            //Thread.sleep(1000*60);
////            System.out.println((System.currentTimeMillis() - time)/1000);
////            //repo.close();
////            System.exit(0);
////    }
////    
////    
////    public  static class R implements Runnable{
////
////        Table table;
////        long start;
////        long end;
////        CloneableRecord _id;
////        CloneableRecord _name;
////        CountDownLatch cdl;
////        
////        public R(Table table,long start,long end,CloneableRecord _id,CloneableRecord _name,CountDownLatch cdl){
////            this.table = table;
////            this.start = start;
////            this.end = end;
////            this.cdl = cdl;
////            this._id = _id;
////            this._name = _name;
////        }
////        
////        int error_count;
////        
////        @Override
////        public void run() {
////            for(long i=start;i<end;i++){
////                try {
////                    _id.put("id", i);
////                    _name.put("name", "xxx"+i);
////                    table.put(null, _id, _name);
////                
////                } catch (Exception ex) {
////                    error_count++;
////                    Logger.getLogger(TestBDB.class.getName()).log(Level.SEVERE, null, ex);
////                }
////                if(i % 1000 == 0){
////                    System.out.println(i);
////                }
////            }
////            System.out.println("error_count:"+error_count);
////            cdl.countDown();
////        }
////        
////    }
//    
//    
//}
