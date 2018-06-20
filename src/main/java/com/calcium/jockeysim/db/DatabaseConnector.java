package com.calcium.jockeysim.db;

import java.sql.*;

/**
 * Created by calvi on 6/19/2018.
 */
public class DatabaseConnector {

//    private String sqlitePath;
    private Connection conn;


    public DatabaseConnector(String sqlitePath){
//        this.sqlitePath = sqlitePath;

        String url = "jdbc:sqlite:"+sqlitePath;
        try {
            conn = DriverManager.getConnection(url);
            System.out.println("Connection Created.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection(){
        return conn;
    }

    public ResultSet execute(String sql) throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        return rs;
    }


}
