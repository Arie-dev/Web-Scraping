package com.dnsbelgium.crawler;

import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;

/**
 * Utility class that provides analytics functions for the "links" table in the database.
 * 
 * This class contains methods to perform basic analytics on the crawled link data, 
 * such as counting link types and calculating the average response time.
 */
public class DuckDBAnalytics {

    /**
     * Counts the number of links for each type (e.g., "internal" or "external") 
     * in the "links" table and prints the results.
     * 
     * This method groups the links by their type and calculates the count for each group.
     *
     * @param conn the active database connection to execute the query
     */
    public static void countLinkTypes(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            // SQL query to group by "type" and count the number of records in each group
            String query = "SELECT type, COUNT(*) FROM links GROUP BY type";
            ResultSet rs = stmt.executeQuery(query);

            // Iterate through the result set and print the count for each link type
            while (rs.next()) {
                String type = rs.getString("type");
                int count = rs.getInt(2);
                System.out.println(type + ": " + count);
            }
        } catch (SQLException e) {
            // Print the stack trace in case of an exception
            e.printStackTrace();
        }
    }

    /**
     * Calculates the average response time for all links in the "links" table 
     * and prints the result in milliseconds.
     * 
     * This method uses the SQL AVG function to compute the average of the "response_time" column.
     *
     * @param conn the active database connection to execute the query
     */
    public static void calculateAverageResponseTime(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            // SQL query to calculate the average response time
            String query = "SELECT AVG(response_time) FROM links";
            ResultSet rs = stmt.executeQuery(query);

            // Print the average response time if the query returns a result
            if (rs.next()) {
                System.out.println("Average Response Time: " + rs.getDouble(1) + " ms");
            }
        } catch (SQLException e) {
            // Print the stack trace in case of an exception
            e.printStackTrace();
        }
    }
}
