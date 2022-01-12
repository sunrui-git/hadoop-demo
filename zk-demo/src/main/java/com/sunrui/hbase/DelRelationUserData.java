package com.sunrui.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class DelRelationUserData {
    private HBaseConfiguration configuration = null;
    private Connection connection = null;
    // 后面会重复使用table
    private Table table = null;

    @Before
    public void init() throws IOException {

        configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "linux121,linux122");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("user"));
    }

    /**
     * 生成用户数据
     *
     * @throws IOException
     */
    @Test
    public void createUserData() throws IOException {

        // 创建空表格
        Table table = getNewTable(connection);

        // 生成随机数据
        generateData(table);
        System.out.println("生成随机数据成功");

        // 打印生成的表
        scanTable(table);
    }

    /**
     * 删除好友，手动输入uid
     *
     * @throws IOException
     */
    @Test
    public void deleteFriend() throws IOException {
        // 用户uid
        String userId = "uid2";
        // 好友id
        String friendId = "uid6";

        Delete fromUser = new Delete(Bytes.toBytes(userId));
        fromUser.addColumn(Bytes.toBytes("friends"), Bytes.toBytes(friendId));
        Delete fromFriend = new Delete(Bytes.toBytes(friendId));
        fromFriend.addColumn(Bytes.toBytes("friends"), Bytes.toBytes(userId));

        table.delete(fromUser);
        table.delete(fromFriend);

        System.out.printf("移除 %s 和 %s 的好友关系成功\n", userId, friendId);

        scanTable(table);
    }


    @Test
    public void showTable() throws IOException {
        scanTable(table);
    }

    /**
     * 遍历并打印表格数据
     *
     * @param table
     * @throws IOException
     */
    public void scanTable(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(row + ":" + family + ", " + column + ", " + value);
            }
        }
    }

    /**
     * 生成随机用户数据
     *
     * @param table
     * @throws IOException
     */
    public void generateData(Table table) throws IOException {
        // 用户数
        int number = 9;
        // 主动添加好友的基数
        int friendScale = 2;

        // 准备数据
        Map<String, Set<String>> userMatrix = new HashMap<String, Set<String>>();
        for (int i = 1; i < 1 + number; i++) {
            userMatrix.put("uid" + i, new HashSet<String>());
        }

        Random random = new Random();
        for (int i = 1; i < 1 + number; i++) {

            for (int j = 0; j < friendScale; j++) {
                int index = random.nextInt(number) + 1;
                if (index != i) {
                    userMatrix.get("uid" + i).add("uid" + index);
                    userMatrix.get("uid" + index).add("uid" + i);
                }
            }
        }

        List<Put> puts = new ArrayList<Put>();
        for (Map.Entry<String, Set<String>> entry : userMatrix.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());

            Put put = new Put(Bytes.toBytes(entry.getKey()));
            for (String v : entry.getValue()) {
                put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes(v), Bytes.toBytes(v));
            }
            puts.add(put);
        }

        table.put(puts);
    }

    /**
     * 生成空表格
     *
     * @param connection
     * @return
     * @throws IOException
     */
    public Table getNewTable(Connection connection) throws IOException {

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        TableName tableName = TableName.valueOf("user");

        // 创建表格，如果存在就先删除
        if (admin.tableExists(tableName)) {

            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        hTableDescriptor.addFamily(new HColumnDescriptor("friends"));

        admin.createTable(hTableDescriptor);

        admin.close();

        return connection.getTable(tableName);
    }

    @After
    public void destroy() {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
