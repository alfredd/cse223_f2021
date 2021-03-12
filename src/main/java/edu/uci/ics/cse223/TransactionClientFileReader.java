package edu.uci.ics.cse223;

import java.io.*;
import java.util.*;

public class TransactionClientFileReader {
    private ConfigurationManager configManager;
    private String fileURL;
    private BufferedReader reader;
    private StringBuilder query;
    private String line;
    private HashSet<String> uniqueSensorIds;
    private TransactionClient client;

    public TransactionClientFileReader(String fileURL) {
        this.fileURL = fileURL;
        query = new StringBuilder("");
        uniqueSensorIds = new HashSet<>();
        line = "";
        try {
            configManager = new ConfigurationManager();
            client = new TransactionClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseFileAndExecute() {
        openFile();
        getUniqueSensorId();
        for (String sensorId : uniqueSensorIds) {
            createTransaction(sensorId);
        }
    }

    // INSERT INTO thermometerobservation VALUES ('54fd1b36-84a1-4848-8bcf-cb165b2af698', 80, '2017-11-08 00:00:00', '1');
    private void createTransaction(String sensorId) {
        System.out.println(sensorId);
        int hashVal = 0;
        String[] tmpStmts;
        String tmpDate = "", tmpStmt = "";
        Hashtable<Integer, String> hashTable = new Hashtable<>();
        List<Twopc.HashedQuery> hashedQueries = new ArrayList<>();
        openFile();
        try {
            line = reader.readLine();
            int cnt = 0, txnId = 0;
            while (line != null) {
                if (line.toLowerCase().contains("insert") && line.contains("\'" + sensorId + "\'")) {
                    tmpStmts = line.split(",");
                    tmpDate = tmpStmts[tmpStmts.length - 2];
                    tmpDate = tmpDate.substring(tmpDate.indexOf("\'") + 1, tmpDate.lastIndexOf("\'"));
                    hashVal = calculateHash(sensorId, tmpDate);
                    if (hashTable.containsKey(hashVal)) {
                        tmpStmt = hashTable.get(hashVal);
                        tmpStmt = tmpStmt + ";" + line;
                        hashTable.replace(hashVal, tmpStmt);
                    } else {
                        hashTable.put(hashVal, line);
                    }
                    cnt++;
                }
                if (cnt == configManager.getMaxTxnStatements()) {
                    txnId++;
//                    System.out.println(hashTable.toString());

                    hashTable.forEach((k, v) -> {
                        hashedQueries.add(
                                Twopc.HashedQuery.newBuilder()
                                        .setHash(k)
                                        .setStatement(v)
                                        .build()
                        );
                    });

                    Twopc.Transaction transaction = Twopc.Transaction.newBuilder()
                            .addAllStatement(hashedQueries)
                            .setId("txn-" + txnId + "-" + sensorId).build();

//                    System.out.println(transaction.toString());
//                    System.out.println("-------------------------");

                    client.executeTransaction(transaction);

                    cnt = 0;
                    tmpStmt = "";
                    tmpDate = "";
                    hashTable.clear();
                    hashedQueries.clear();
                    tmpStmts = null;
                    hashVal = 0;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openFile() {
        try {
            reader = new BufferedReader(new FileReader(
                    new File(fileURL))
            );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void getUniqueSensorId() {
        String[] tmpArray;
        String tmpSensorID = "";
        try {
            line = reader.readLine();
            while (line != null) {
                if (line.toLowerCase().contains("insert")) {
                    tmpArray = line.split(",");
                    tmpSensorID = tmpArray[tmpArray.length - 1];
                    tmpSensorID = tmpSensorID.substring(tmpSensorID.indexOf("\'") + 1, tmpSensorID.lastIndexOf("\'"));
                    uniqueSensorIds.add(tmpSensorID);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int calculateHash(String sensorId, String timeStamp) {
        int a = 0;
        a = Objects.hash(sensorId, timeStamp);
        return ((a < 0 ? (-1 * a) : a) % 3) + 1;
    }
}

//class DEMO {
//    public static void main(String[] args) {
//        TransactionClientFileReader t = new TransactionClientFileReader(
//                new File(System.getProperty("user.dir") + "/testqueries.sql").getPath()
//        );
//        t.parseFileAndExecute();
//    }
//}