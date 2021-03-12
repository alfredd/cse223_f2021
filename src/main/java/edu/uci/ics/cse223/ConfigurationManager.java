package edu.uci.ics.cse223;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class ConfigurationManager {

    private final Properties p;

    public ConfigurationManager() throws IOException {
        p = new Properties();
        p.load(new FileReader(new File(System.getProperty("user.dir") + "/config.properties")));
    }

    public String getDBUrl(int hostID) {
        return p.getProperty("db"+hostID);
    }

    public Integer getHostPort(int hostID) {
        return Integer.decode(p.getProperty(""+hostID));
    }


    public String getDBUsername() {
        return p.getProperty("user");
    }

    public String getDBPassword() {
        return p.getProperty("password");
    }

    public int getMaxTxnStatements() {
        return Integer.decode(p.getProperty("max_txn"));
    }

}
