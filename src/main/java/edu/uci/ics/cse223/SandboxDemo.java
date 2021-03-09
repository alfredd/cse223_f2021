package edu.uci.ics.cse223;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SandboxDemo {

    public static void main(String[] args) throws IOException {
        System.out.println(System.getProperty("user.dir"));
        Properties p = new Properties();
        p.load(new FileReader(new File(System.getProperty("user.dir")+"/config.properties")));
        System.out.println(p.keySet());
    }
}
