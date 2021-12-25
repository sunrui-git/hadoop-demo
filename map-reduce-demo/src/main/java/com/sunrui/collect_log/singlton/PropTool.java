package com.sunrui.collect_log.singlton;

import com.sunrui.collect_log.collect.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

/**
 * @author sunrui
 * @description
 * @date 2021/11/17
 */
public class PropTool {

    private static volatile Properties prop = null;

    public static Properties getProp() throws IOException {

        if(prop == null){
            synchronized ("lock"){
                if(prop == null){
                    prop = new Properties();
                    prop.load(LogCollectorTask.class.getClassLoader().getResourceAsStream("collector.properties"));
                }
            }
        }
        return prop;
    }
}
