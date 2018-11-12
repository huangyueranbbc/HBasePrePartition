import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author huangyueran
 */
public class HBasePrePartition {

    // HBase 基本 API
    private Connection connection; // HTablePool被弃用 因为线程不安全。使用Connection代替，只要保证全局是同一个Connection就可以。
    private Admin admin;
    private Table table;

    TableName tableName = TableName.valueOf("phone_2018812");

    @Before
    public void begin() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Configuration conf = new Configuration();
        // 指定 Zookeeper集群
        conf.set("hbase.zookeeper.quorum", "master");

        connection = ConnectionFactory.createConnection(conf);

        table = connection.getTable(tableName);

        admin = connection.getAdmin();
    }

    @After
    public void end() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * @throws IOException
     * @category 创建表
     */
    @Test
    public void createTable() throws IOException {
        // 判断表是否存在 如果存在 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor desc = new HTableDescriptor(tableName); // 创建表描述

        // 创建列族 1-3个列族最佳
        HColumnDescriptor family = new HColumnDescriptor("cf1");
        family.setBlockCacheEnabled(true); // 打开读缓存
        family.setInMemory(true); // 打开写缓存
        family.setMaxVersions(1); // 最大版本数

        desc.addFamily(family);

        admin.createTable(desc);
    }

    /**
     * 预分区 创建表
     */
    @Test
    public void createTableBySplitKeys() {
        try {
            TableName tableName = TableName.valueOf("testyufenqu");
            List<String> columnFamily = new ArrayList();
            columnFamily.add("info");

            if (admin.tableExists(tableName)) {
                System.out.println("table has exist!" + tableName);
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String cf : columnFamily) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                byte[][] splitKeys = getSplitKeys();
                admin.createTable(tableDescriptor, splitKeys);//指定splitkeys
                System.out.println("===Create Table " + tableName
                        + " Success!columnFamily:" + columnFamily.toString()
                        + "===");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @throws IOException
     * @category 预分区插入数据
     */
    @Test
    public void insertByRegion() throws IOException {
        TableName tableName = TableName.valueOf("testyufenqu");
        Table table1 = connection.getTable(tableName);
        table1.put(batchPut());
    }

    private static List<Put> batchPut() {
        List<Put> list = new ArrayList<Put>();
        for (int i = 1; i <= 10000; i++) {
            byte[] rowkey = Bytes.toBytes(getRandomNumber() + "-" + System.currentTimeMillis() + "-" + i);
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zs" + i));
            list.add(put);
        }
        return list;
    }

    private static String getRandomNumber() {
        String ranStr = Math.random() + "";
        int pointIndex = ranStr.indexOf(".");
        return ranStr.substring(pointIndex + 1, pointIndex + 3);
    }

    private byte[][] getSplitKeys() {
        String[] keys = new String[]{"10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|"};
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    /**
     * @throws IOException
     * @category 插入数据
     */
    @Test
    public void insert() throws IOException {
        byte[] rowkey = ("18271683973_" + System.currentTimeMillis()).getBytes();
        Put puts = new Put(rowkey);
        puts.addColumn("cf1".getBytes(), "dest".getBytes(), "13805557773".getBytes());
        puts.addColumn("cf1".getBytes(), "type".getBytes(), "1".getBytes());
        puts.addColumn("cf1".getBytes(), "time".getBytes(), "2015-09-09 16:55:29".getBytes());
        table.put(puts);
    }


    /**
     * 异步往指定表添加数据 批量插入
     *
     * @return long                返回执行时间
     * @throws IOException
     */
    @Test
    public void put() throws Exception {
        String tablename = "t_cdr";

        for(int i=0;i<10000000;i++){
            List<Put> puts = getPuts();
            // 批量插入异常监听
            final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        System.out.println("Failed to sent put " + e.getRow(i) + ".");
                    }
                }

            };
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                    .listener(listener);
            params.writeBufferSize(5 * 1024 * 1024);

            final BufferedMutator mutator = connection.getBufferedMutator(params);
            try {
                mutator.mutate(puts);
                mutator.flush();
            } finally {
                mutator.close();
            }
        }

    }

    private List<Put> getPuts() {
        List<Put> puts = new ArrayList<Put>(); // 装入集合一起插入记录

        for (int i = 0; i < 10; i++) {
            String rowkey;
            String phoneNum = getPhoneNum("183");

            // 100条通话记录
            for (int j = 0; j < 100; j++) {
                String phoneDate = getData("2016");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                try {
                    long dateLong = sdf.parse(phoneDate).getTime();
                    // 降序
                    rowkey = phoneNum + (Long.MAX_VALUE - dateLong);

                    Put put = new Put(rowkey.getBytes());
                    put.add("cf1".getBytes(), "type".getBytes(), (new Random().nextInt(2) + "").getBytes());
                    put.add("cf1".getBytes(), "time".getBytes(), phoneDate.getBytes());
                    put.add("cf1".getBytes(), "dest".getBytes(), getPhoneNum("159").getBytes());

                    puts.add(put);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
        }

        return puts;
    }

    /**
     * 随机生成手机号
     *
     * @param prefix
     * @return
     */
    private String getPhoneNum(String prefix) {
        return prefix + String.format("%08d", new Random().nextInt(99999999));
    }

    /**
     * 随机生成时间
     *
     * @param year
     * @return
     */
    private String getData(String year) {
        Random r = new Random();
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[]{r.nextInt(12) + 1, r.nextInt(28) + 1, r.nextInt(60), r.nextInt(60), r.nextInt(60)});
    }


    /**
     * @throws IOException
     * @category 查询
     */
    @Test
    public void get() throws IOException {
        String rowkey = "18271683973_1499228897396";
        Get get = new Get(rowkey.getBytes());
        // TODO 搜索筛选查询
        get.addColumn("cf1".getBytes(), "type".getBytes());
        get.addColumn("cf1".getBytes(), "dest".getBytes());
        get.addColumn("cf1".getBytes(), "time".getBytes());

        Result result = table.get(get);

        Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
        System.out.println(new String(c1.getValue()));
        Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
        System.out.println(new String(c2.getValue()));
        Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
        System.out.println(new String(c3.getValue()));

    }

}
