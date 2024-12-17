package com.dnsbelgium.crawler;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

public class DuckDBTableCreator {

    public static void createLinksTable(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            String createTableSQL = "CREATE TABLE IF NOT EXISTS links (" +
                                    "url TEXT PRIMARY KEY, " +
                                    "title TEXT, " +
                                    "type TEXT, " +
                                    "response_time DOUBLE)";
            stmt.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createLinksTableInBothDatabases(String originalDbPath, String replicatedDbPath) {
        Connection originalConn = null;
        Connection replicatedConn = null;
        try {
            originalConn = MyDuckDBConnection.connect(originalDbPath);
            replicatedConn = MyDuckDBConnection.connect(replicatedDbPath);
            createLinksTable(originalConn);
            createLinksTable(replicatedConn);
        } finally {
            if (originalConn != null) {
                try {
                    originalConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (replicatedConn != null) {
                try {
                    replicatedConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}