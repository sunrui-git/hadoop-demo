package com.sunrui.zk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author sunrui
 * @description 基于Zookeeper实现简易版配置中心
 * @date 2022/1/4
 */
@SpringBootApplication
public class Client {
    private static String mysqlConfig = "/mysqlConfig";
    private static ZkClient zkClient = new ZkClient("linux121:2181,linux:122:2181");
    private static HikariDataSource hikariDataSource;

    public static void main(String[] args) throws InterruptedException {
        configHikariSource();
        connectZk();

        Thread.sleep(Integer.MAX_VALUE);
    }
    public static void connectZk(){

        // 自定义序列化类
        zkClient.setZkSerializer(new ZkSerializer() {
            public byte[] serialize(Object o) throws ZkMarshallingError {
                return String.valueOf(o).getBytes();
            }

            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                return new String(bytes);
            }
        });
        // 监听数据变化
        zkClient.subscribeDataChanges(mysqlConfig, new IZkDataListener() {
            public void handleDataChange(String path, Object data) throws Exception {
                System.out.println(path + "data change" + data);
                // 关闭连接池
                hikariDataSource.close();
                configHikariSource();
            }

            public void handleDataDeleted(String path) throws Exception {
                System.out.println(path + " is deleted!!");
                // 关闭连接池
                hikariDataSource.close();
            }
        });
    }
    /**
     * 配置数据库连接池
     */
    private static void configHikariSource(){

        // 从 zookeeper 中获取配置信息
        JDBCConfig myConfig = getJDBCConfig();

        // 更新 hikari 配置
        updateHikariConfig(myConfig);

        System.out.println(myConfig.toString());
        System.out.println(hikariDataSource.toString());
    }

    // 更新连接信息
    private static void updateHikariConfig(JDBCConfig myConfig) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(myConfig.getUrl());
        config.setUsername(myConfig.getUsername());
        config.setPassword(myConfig.getPassword());
        config.addDataSourceProperty( "driverClassName" , myConfig.getDriver());
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        hikariDataSource = new HikariDataSource(config);
    }

    // 从zookeeper 获取连接信息
    private static JDBCConfig getJDBCConfig() {

        Object data = zkClient.readData(mysqlConfig);
        ObjectMapper mapper = new ObjectMapper();

        try {
            JDBCConfig myConfig = mapper.readValue(data.toString(), JDBCConfig.class);

            System.out.println(myConfig.toString());

            return myConfig;

        } catch (Exception e) {
            return new JDBCConfig();
        }
    }
}
