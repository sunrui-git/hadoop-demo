package com.sunrui.zk;

import lombok.Data;
import lombok.ToString;

/**
 * @author sunrui
 * @description
 * @date 2022/1/4
 */
@Data
@ToString
public class JDBCConfig {

    private String url;

    private String driver = "com.mysql.jdbc.Driver";

    private String username;

    private String password;
}
