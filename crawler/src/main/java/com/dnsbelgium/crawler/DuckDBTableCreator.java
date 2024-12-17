package com.dnsbelgium.crawler;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

/**
 * Utility class that provides methods to create the "links" table in a DuckDB database.
 * 
 * This class contains methods to create the table schema in a specified database and to ensure
 * the table is created in both the original and replicated databases.
 */
public class DuckDBTableCreator {

    /**
     * Creates the "links" table in the given database connection if it does not already exist.
     * 
     * The "links" table stores information about the crawled URLs, including the URL itself, 
     * the title of the page, the type of link (e.g., internal or external), and the response time.
     *
     * @param conn the active database connection to create the table in
     */
    public static void createLinksTable(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            // SQL statement to create the "links" table if it does not already exist
            String createTableSQL = "CREATE TABLE IF NOT EXISTS links (" +
                                    "url TEXT PRIMARY KEY, " +
                                    "title TEXT, " +
                                    "type TEXT, " +
                                    "response_time DOUBLE)";
            // Execute the SQL query to create the table
            stmt.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            // Print stack trace for debugging in case of a SQL exception
            e.printStackTrace();
        }
    }

    /**
     * Creates the "links" table in both the original and replicated databases.
     * 
     * This method establishes connections to both the original and replicated DuckDB databases, 
     * and creates the "links" table in both databases by calling {@link #createLinksTable(Connection)}.
     *
     * @param originalDbPath the file path to the original DuckDB database
     * @param replicatedDbPath the file path to the replicated DuckDB database
     */
    public static void createLinksTableInBothDatabases(String originalDbPath, String replicatedDbPath) {
        Connection originalConn = null;
        Connection replicatedConn = null;
        try {
            // Establish connections to both the original and replicated databases
            originalConn = MyDuckDBConnection.connect(originalDbPath);
            replicatedConn = MyDuckDBConnection.connect(replicatedDbPath);
            
            // Create the "links" table in both databases
            createLinksTable(originalConn);
            createLinksTable(replicatedConn);
        } finally {
            // Ensure both database connections are closed after the operations
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
