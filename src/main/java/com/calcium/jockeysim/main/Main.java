package com.calcium.jockeysim.main;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by calvi on 6/19/2018.
 */
public class Main {
    public static void main(String[] args){
        Connection conn = null;


        try {
            String url = "jdbc:sqlite:D:\\Downloads\\SQLiteDatabaseBrowserPortable\\Data\\SoccBetSim.db";
            conn = DriverManager.getConnection(url);

            System.out.println("Connected");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if(conn != null){

            String sql = "SELECT * FROM USER";

            try(Connection connect = conn;
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
                ){

                ResultSetMetaData md = rs.getMetaData();
                Integer count = md.getColumnCount();
                List<String> columnNames = new ArrayList<>();
                List<Integer> columnTypes = new ArrayList<>();
                for(int i=1; i<count+1; i++){
                    columnNames.add(md.getColumnLabel(i));
                    columnTypes.add(md.getColumnType(i));
                }
                System.out.println(columnNames.size());

                while(rs.next()){

                    for(String column: columnNames){
                        System.out.println(column);
                        System.out.println(rs.getString(column));
                    }

                }
            }catch (SQLException e){
                e.printStackTrace();
            }

        }

    }
}
