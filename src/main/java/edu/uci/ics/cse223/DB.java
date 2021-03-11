package edu.uci.ics.cse223;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DB {

    private final Connection conn;

    public DB(int hostID) throws IOException, SQLException {
        ConfigurationManager cm = new ConfigurationManager();
        String dbUrl = cm.getDBUrl(hostID);
        String dbUsername = cm.getDBUsername();
        String dbPassword = cm.getDBPassword();
        String url = "jdbc:postgresql://" + dbUrl;
        Properties props = new Properties();
        props.setProperty("user", dbUsername);
        props.setProperty("password", dbPassword);
        props.setProperty("ssl", "false");
        props.setProperty("isolation", "SERIALIZABLE");
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
                        " cohortId integer NOT NULL," +
                        " status varchar(25) NOT NULL)",
                "CREATE TABLE IF NOT EXISTS CohortLog (" +
                        " txnId varchar(25) NOT NULL, " +
                        " status varchar(25) NOT NULL)"
        };
        for (String s : SQL_CREATE_TABLES) {
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
        Statement statement = null;

        List<String> st = request.getStatementList();
        StringBuilder strBui = new StringBuilder("BEGIN; prepare transaction '");
        strBui.append(request.getId()).append("' ; ").append(String.join(";", st));
//        insertRedoLog(request.getId(), strBui.toString());

        try {
            statement = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (statement != null) {
                statement.execute(strBui.toString());
                status = true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        if (!status) {
            try {
                statement.execute("ROLLBACK PREPARED '" + request.getId() + "';");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return status;
    }

    public boolean commitSQL(Twopc.SQL request) {
        boolean status = false;
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeQuery("commit prepared '" + request.getId() + "'");
            status = true;
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }

        return status;
    }

    public boolean abortSQL(Twopc.SQL request) {
        boolean status = false;
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.execute("rollback prepared '" + request.getId() + "'");
            status = true;
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }

        return status;
    }

    public boolean insertRedoLog(String txnId, String txnQuery) {
        boolean status = false;
        String query = "insert into RedoLog (txnId, txnStatement) values (?, ?)";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            pStatement.setString(2, txnQuery);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public boolean deleteRedoLog(String txnId) {
        boolean status = false;
        String query = "delete from RedoLog where txnId=?";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public boolean insertProtocolLog(String txnId, Integer coId, String st) {
        boolean status = false;
        String query = "insert into ProtocolLog (txnId, cohortId, status) values (?, ?, ?)";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            pStatement.setInt(2, coId);
            pStatement.setString(3, st);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public boolean deleteProtocolLog(String txnId) {
        boolean status = false;
        String query = "delete from ProtocolLog where txnId=?";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public int updateProtocolLog(String txnId, Integer coId, String stat) {
        String query = "update ProtocolLog set status=? where txnId=? and cohortId=?";
        int status = 0;
        try {
            PreparedStatement pStmt = conn.prepareStatement(query);
            pStmt.setString(1, stat);
            pStmt.setString(2, txnId);
            pStmt.setInt(3, coId);
            status = pStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        return status;
    }

    public String getProtocolLogStatus(String txnId, String cohortId) { // txnId, cohortId
        String status = "";
        String query = "select status from ProtocolLog where txnId=? and cohortId=?";
        PreparedStatement prepStmt = null;
        try {
            prepStmt = conn.prepareStatement(query);
            prepStmt.setString(1, txnId);
            prepStmt.setString(2, cohortId);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                status = rs.getString(0);
                break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
        return status;
    }

    public List<String> getProtocolLogByTxnIDs(String status) { // txnId, cohortId
        List<String> res = new ArrayList<>();
        String query = "select txnId from ProtocolLog where status=?";
        PreparedStatement prepStmt = null;
        try {
            prepStmt = conn.prepareStatement(query);
            prepStmt.setString(1, status);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                res.add(rs.getString(0));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return res;
        }
        return res;
    }

    public boolean insertCohortLog(String txnId, String st) {
        boolean status = false;
        String query = "insert into CohortLog (txnId, status) values (?, ?)";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            pStatement.setString(2, st);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public boolean deleteCohortLog(String txnId) {
        boolean status = false;
        String query = "delete from CohortLog where txnId=?";
        try {
            PreparedStatement pStatement = conn.prepareStatement(query);
            pStatement.setString(1, txnId);
            status = pStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            status = false;
        }
        return status;
    }

    public int updateCohortLog(String txnId, String stat) {
        String query = "update CohortLog set status=? where txnId=?";
        int status = 0;
        try {
            PreparedStatement pStmt = conn.prepareStatement(query);
            pStmt.setString(1, stat);
            pStmt.setString(2, txnId);
            status = pStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        return status;
    }

}
