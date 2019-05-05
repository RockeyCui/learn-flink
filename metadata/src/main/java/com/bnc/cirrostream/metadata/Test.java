package com.bnc.cirrostream.metadata;

import com.bnc.cirrostream.metadata.util.H2ConnFactoryUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author RockeyCui
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Connection conn = H2ConnFactoryUtil.getConn();
        Statement statement = conn.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from user_info");

        while (resultSet.next()) {
            String name = resultSet.getString("name");
            System.out.println(name);
        }
    }
}
