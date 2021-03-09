package edu.uci.ics.cse223;

import java.sql.*;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class MainDemo
{
    public static void main( String[] args ) throws SQLException {
        System.out.println( "Hello World!" );
        String url = "jdbc:postgresql://db1.caezowu6vchx.us-east-1.rds.amazonaws.com/";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","postgres223");
        props.setProperty("ssl","false");
        Connection conn = DriverManager.getConnection(url, props);

//        String url = "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true";
//        Connection conn = DriverManager.getConnection(url);

        PreparedStatement result = conn.prepareStatement("SELECT COUNT(*) FROM thermometerobservation;");
        result.execute();
        ResultSet rs = result.getResultSet();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
    }
}
