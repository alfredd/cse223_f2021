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
        String url = "jdbc:postgresql://db4.caezowu6vchx.us-east-1.rds.amazonaws.com/";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","postgres223");
        props.setProperty("ssl","false");
        props.setProperty("max_prepared_transactions", "1");
        Connection conn = DriverManager.getConnection(url, props);

//        String url = "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true";
//        Connection conn = DriverManager.getConnection(url);

        PreparedStatement result = conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS public;" +
                "GRANT ALL ON SCHEMA public TO postgres;" +
                "GRANT ALL ON SCHEMA public TO public;");
        result.execute();
        /*ResultSet rs = result.getResultSet();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }*/
    }
}
