package cn.cs.scu;

import cn.cs.scu.conf.ConfigurationManager;
import cn.cs.scu.constants.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Wanghan on 2017/3/15.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class testMysql {
    public static void main(String[] args) {

        String sql = "SELECT * FROM ad";

        String driver = ConfigurationManager.getString(Constants.JDBC_DRIVER);

        String url = ConfigurationManager.getString(Constants.JDBC_URL);

        String host = ConfigurationManager.getString(Constants.JDBC_HOST);

        String user = ConfigurationManager.getString(Constants.JDBC_USER);

        String pwd = ConfigurationManager.getString(Constants.JDBC_PASSWORD);

        Connection conn = null;

        Statement stmt = null;
        try {
            Class.forName(driver).newInstance();
            conn = (Connection) DriverManager.getConnection(url, user, pwd);
            stmt = (Statement) conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
//            System.out.println(rs.getFetchSize());
            while (rs.next()){
                System.out.println(rs.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
