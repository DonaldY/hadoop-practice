package com.donaldy.impala.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author donald
 * @date 2020/08/26
 */
public class ImpalaTest {
    public static void main(String[] args) throws Exception {

        // 定义连接impala的驱动和连接url
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String driverUrl = "jdbc:hive2://172.16.64.122:21050/mydb;auth=noSasl";

        // 查询的sql语句
        String querySql = "select * from t1";

        // 获取连接
        Class.forName(driver);

        // 通过 Drivermanager 获取连接
        final Connection connection = DriverManager.getConnection(driverUrl);
        final PreparedStatement ps = connection.prepareStatement(querySql);

        // 执行查询
        final ResultSet resultSet = ps.executeQuery();

        // 解析返回结果
        // 获取到每条数据的列数
        final int columnCount = resultSet.getMetaData().getColumnCount();
        // 遍历结果集
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                final String string = resultSet.getString(i);
                System.out.print(string + "\t");
            }
            System.out.println();
        }

        //关闭资源
        ps.close();
        connection.close();
    }
}
