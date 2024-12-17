package com.dnsbelgium.crawler;

import org.duckdb.DuckDBDriver;
import java.sql.Connection;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Utility class to handle DuckDB database connections and operations such as connecting to a database
 * and replicating the database files.
 * 
 * This class provides methods to connect to a DuckDB database using JDBC and to replicate a DuckDB database
 * file by copying its contents to a new location.
 */
public class MyDuckDBConnection {

    /**
     * Establishes a connection to a DuckDB database located at the specified path.
     * 
     * This method checks if the parent directory of the database file exists, and if not, it creates the necessary directories.
     * Afterward, it attempts to establish a connection to the DuckDB database and returns the connection object.
     *
     * @param dbPath the path to the DuckDB database file
     * @return a {@link Connection} object to the database, or {@code null} if the connection fails
     */
    public static Connection connect(String dbPath) {
        try {
            // Create a File object for the database file and ensure the parent directory exists
            File dbFile = new File(dbPath);
            File parentDir = dbFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();  // Create parent directories if they don't exist
            }

            System.out.println("Connecting to DuckDB at: " + dbFile.getAbsolutePath());

            // Attempt to establish a connection to the DuckDB database
            Connection conn = new DuckDBDriver().connect("jdbc:duckdb:" + dbPath, null);
            return conn;
        } catch (Exception e) {
            // Print the exception stack trace if an error occurs while connecting
            e.printStackTrace();
        }
        return null;  // Return null if the connection could not be established
    }

    /**
     * Replicates the DuckDB database from the original database file to a new replicated database file.
     * 
     * This method copies the content of the original database file to the replicated database file.
     * If the original database does not exist, an error message is printed, and the operation is skipped.
     *
     * @param originalDbPath the path to the original DuckDB database file
     * @param replicatedDbPath the path to the replicated DuckDB database file
     */
    public static void replicateDatabase(String originalDbPath, String replicatedDbPath) {
        try {
            // Create File objects for both the original and replicated database files
            File originalDbFile = new File(originalDbPath);
            File replicatedDbFile = new File(replicatedDbPath);

            // Check if the original database file exists
            if (!originalDbFile.exists()) {
                System.err.println("Original database file does not exist: " + originalDbPath);
                return;  // Exit the method if the original database does not exist
            }

            // Replicate the database by copying the original file to the new location
            Files.copy(originalDbFile.toPath(), replicatedDbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            System.out.println("Replicated database to: " + replicatedDbFile.getAbsolutePath());
        } catch (IOException e) {
            // Print the exception stack trace if an error occurs while replicating the database
            e.printStackTrace();
        }
    }
}
