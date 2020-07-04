package com.mphrx.datalake.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@ConfigurationProperties(prefix = "mysql")
@ConstructorBinding
public class MysqlConf {
    private final String url;
    private final String driver;
    private final String username;
    private final String password;

    public MysqlConf(String url, String driver, String username, String password) {
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getDriver() {
        return driver;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }



    public Properties getMysqlProperties(){
        Properties prop = new Properties();
        prop.setProperty("driver", driver);
        prop.setProperty("user", username);
        prop.setProperty("password", password);

        return prop;
    }
}
