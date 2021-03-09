package edu.uci.ics.cse223;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DB {

    private final Connection conn;

    public DB (int hostID) throws IOException, SQLException {
        ConfigurationManager cm = new ConfigurationManager();
        String dbUrl = cm.getDBUrl(hostID);
        String dbUsername = cm.getDBUsername();
        String dbPassword = cm.getDBPassword();
        String url = "jdbc:postgresql://"+dbUrl;
        Properties props = new Properties();
        props.setProperty("user",dbUsername);
        props.setProperty("password",dbPassword);
        props.setProperty("ssl","false");
        conn = DriverManager.getConnection(url, props);
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
