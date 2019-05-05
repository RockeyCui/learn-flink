package com.bnc.cirrostream.metadata.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;

/**
 * @author RockeyCui
 */
public class H2ConnFactoryUtil {
    private static HikariDataSource dataSource;
    private static HikariConfig hikariConfig = new HikariConfig();

    static {
        hikariConfig.setJdbcUrl("jdbc:h2:tcp://39.107.224.153:19200/~/rock");
        hikariConfig.setDriverClassName("org.h2.jdbcx.JdbcDataSource");
        hikariConfig.setUsername("rock");
        hikariConfig.setPassword("rock123");
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(hikariConfig);
    }


    public static Connection getConn() throws Exception {
        return dataSource.getConnection();
    }

    public static String haha(String a, int b) {
        return "1";
    }

}
