package edu.uci.ics.cse223;

import jdk.jfr.internal.Logger;

import java.io.IOException;
import java.sql.*;
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
        conn.setAutoCommit(Boolean.FALSE);
        createDatabaseTables();
    }

    private void createDatabaseTables() {
        String[] SQL_CREATE_TABLES = {
                "CREATE TABLE IF NOT EXISTS WeMoObservation (" +
                        "  id varchar(255) NOT NULL," +
                        "  currentMilliWatts integer DEFAULT NULL," +
                        "  onTodaySeconds integer DEFAULT NULL," +
                        "  timeStamp timestamp NOT NULL," +
                        "  sensor_id varchar(255) DEFAULT NULL," +
                        "  PRIMARY KEY (id))",
                "CREATE TABLE IF NOT EXISTS WiFiAPObservation (" +
                        "  id varchar(255) NOT NULL," +
                        "  clientId varchar(255) DEFAULT NULL," +
                        "  timeStamp timestamp NOT NULL," +
                        "  sensor_id varchar(255) DEFAULT NULL," +
                        "  PRIMARY KEY (id))",
                "CREATE TABLE IF NOT EXISTS ThermometerObservation (" +
                        "  id varchar(255) NOT NULL," +
                        "  temperature integer DEFAULT NULL," +
                        "  timeStamp timestamp NOT NULL," +
                        "  sensor_id varchar(255) DEFAULT NULL," +
                        "  PRIMARY KEY (id))",
                "CREATE TABLE IF NOT EXISTS RedoLog (" +
                        " txnId varchar(25) NOT NULL," +
                        " txnStatement varchar(255) NOT NULL)",
                "CREATE TABLE IF NOT EXISTS ProtocolLog (" +
                        " txnId varchar(25) NOT NULL, " +
                        " cohortId varchar(25) NOT NULL," +
                        " status varchar(25) NOT NULL)",
                "CREATE TABLE IF NOT EXISTS CohortLog (" +
                        " txnId varchar(25) NOT NULL, " +
                        " status varchar(25) NOT NULL)"
        };
        for(String s : SQL_CREATE_TABLES) {
            createTable(s);
        }
    }

    private void createTable(String SQL_CREATE_TABLE) {
        try {
            Statement statement = conn.createStatement();
            statement.execute(SQL_CREATE_TABLE);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean prepareSQL(Twopc.SQL request) {
        boolean status = false;
        Statement statement;
        try {
            statement = conn.createStatement();
            status = statement.execute("prepare transaction " + request.getId());
        } catch(SQLException e) {
            return status;
        }
        if(status) {
            PreparedStatement pStatement;
            for (int i = 0; i < request.getStatementCount(); i++) {
                try {
                    pStatement = conn.prepareStatement(request.getStatement(i));
                    status = pStatement.execute();
                } catch (SQLException e) {
                    status = false;
                    break;
                }
            }
        }

        return status;
    }

    public boolean commitSQL(Twopc.SQL request) {
        boolean status = false;
        Statement statement;
        try {
            statement = conn.createStatement();
            status = statement.execute("commit prepared " + request.getId());
        } catch(SQLException e) {
            return status;
        }

        return status;
    }

    public boolean abortSQL(Twopc.SQL request) {
        boolean status = false;
        Statement statement;
        try {
            statement = conn.createStatement();
            status = statement.execute("rollback prepared " + request.getId());
        } catch(SQLException e) {
            return status;
        }

        return status;
    }
}

//class testDB {
//    public static void main(String[] args) {
//        String sql = "INSERT INTO thermometerobservation VALUES ('54fd1b36-84a1-4848-8bcf-cb165b2af698', 80, '2017-11-08 00:00:00', '30cced27_6cd1_4d82_9894_bddbb71a4402')";
//        Twopc.SQL request = Twopc.SQL.newBuilder().addStatement(sql).setId("1").build();
//    }
//}
