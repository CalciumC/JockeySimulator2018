package com.calcium.jockeysim.main;

import com.calcium.jockeysim.handler.ResultSetSerializer;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by calvi on 6/19/2018.
 */
public class Main {
    public static void main(String[] args){
        Connection conn = null;

        SimpleModule module = new SimpleModule();
        module.addSerializer(new ResultSetSerializer());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);

        StringWriter writer = new StringWriter();

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
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.putPOJO("results", rs);
                    objectMapper.writeValue(writer, objectNode);

                    System.out.println(writer.toString());

//                ResultSetMetaData md = rs.getMetaData();
//                Integer count = md.getColumnCount();
//                List<String> columnNames = new ArrayList<>();
//                List<Integer> columnTypes = new ArrayList<>();
//                for(int i=1; i<count+1; i++){
//                    columnNames.add(md.getColumnLabel(i));
//                    columnTypes.add(md.getColumnType(i));
//                }
//                System.out.println(columnNames.size());
//
//                while(rs.next()){
//
//                    for(String column: columnNames){
//                        System.out.println(column);
//                        System.out.println(rs.getString(column));
//                    }
//
//                }
            }catch (SQLException e){
                e.printStackTrace();
            } catch (JsonGenerationException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
