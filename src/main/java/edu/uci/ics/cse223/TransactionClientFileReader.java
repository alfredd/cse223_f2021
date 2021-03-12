package edu.uci.ics.cse223;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class TransactionClientFileReader {
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
            client = new TransactionClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseFileAndExecute() {
        openFile();
        getUniqueSensorId();
        for(String sensorId : uniqueSensorIds) {
            createTransaction(sensorId);
        }
    }

    private void createTransaction(String sensorId) {
//        List<Twopc.HashedQuery> hashedQueries =
        try {
            line = reader.readLine();
            int cnt = 0, txnId = 0;
            while (line != null) {
                if (line.toLowerCase().contains("insert")) {
                    query.append(line).append(";");
                    cnt++;
                }
                line = reader.readLine();
                if (cnt == 10) {
                    txnId++;
//                    Twopc.SQL transaction = Twopc.SQL.newBuilder().setId("t" + String.valueOf(txnId))
//                            .addStatement(query.toString())
//                            .build();
//                    client.executeTransaction(transaction);
                    cnt = 0;
                    query.delete(0, query.length());
                }

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
        String [] tmpArray;
        String tmpSensorID = "";
        try {
            line = reader.readLine();
            while (line != null) {
                if (line.toLowerCase().contains("insert")) {
                    tmpArray = line.split(",");
                    tmpSensorID = tmpArray[tmpArray.length - 1];
                    tmpSensorID = tmpSensorID.substring(tmpSensorID.indexOf("\'")+1, tmpSensorID.lastIndexOf("\'"));
                    uniqueSensorIds.add(tmpSensorID);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int calculateHash(String sensorId, String timeStamp) {
        return Objects.hash(sensorId, timeStamp);
    }
}
//INSERT INTO thermometerobservation VALUES ('54fd1b36-84a1-4848-8bcf-cb165b2af698', 80, '2017-11-08 00:00:00', '30cced27_6cd1_4d82_9894_bddbb71a4402');
//INSERT INTO thermometerobservation VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');

class DEMO {
    public static void main(String[] args) {
        TransactionClientFileReader t = new TransactionClientFileReader(
                new File(System.getProperty("user.dir") + "/observations.sql").getPath()
        );
        t.parseFileAndExecute();
    }
}


//        BufferedReader reader;
//        String line = "";
//        StringBuilder query = new StringBuilder("");
//        try {
//            TransactionClient client = new TransactionClient();
//            reader = new BufferedReader(new FileReader(
//                    new File(args[0]))
//            );
//            line = reader.readLine();
//            int cnt = 0, txnId = 0;
//            while (line != null) {
//                if (line.toLowerCase().contains("insert")) {
//                    query.append(line).append(";");
//                    cnt++;
//                }
//                line = reader.readLine();
//                if (cnt == 10) {
//                    txnId++;
////                    Twopc.SQL transaction = Twopc.SQL.newBuilder().setId("t" + String.valueOf(txnId))
////                            .addStatement(query.toString())
////                            .build();
////                    client.executeTransaction(transaction);
//                    cnt = 0;
//                    query.delete(0, query.length());
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }