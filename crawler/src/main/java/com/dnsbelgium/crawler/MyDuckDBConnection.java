package com.dnsbelgium.crawler;

import org.duckdb.DuckDBDriver;
import java.sql.Connection;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class MyDuckDBConnection {

    public static Connection connect(String dbPath) {
        try {
            File dbFile = new File(dbPath);
            File parentDir = dbFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }

            System.out.println("Connecting to DuckDB at: " + dbFile.getAbsolutePath());

            Connection conn = new DuckDBDriver().connect("jdbc:duckdb:" + dbPath, null);
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void replicateDatabase(String originalDbPath, String replicatedDbPath) {
        try {
            File originalDbFile = new File(originalDbPath);
            File replicatedDbFile = new File(replicatedDbPath);

            if (!originalDbFile.exists()) {
                System.err.println("Original database file does not exist: " + originalDbPath);
                return;
            }

            // replicatie hier
            Files.copy(originalDbFile.toPath(), replicatedDbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            System.out.println("Replicated database to: " + replicatedDbFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}