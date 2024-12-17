package com.dnsbelgium.crawler;

import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.Connection;

public class DuckDBAnalytics {

    public static void countLinkTypes(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            String query = "SELECT type, COUNT(*) FROM links GROUP BY type";
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String type = rs.getString("type");
                int count = rs.getInt(2);
                System.out.println(type + ": " + count);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void calculateAverageResponseTime(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            String query = "SELECT AVG(response_time) FROM links";
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                System.out.println("Average Response Time: " + rs.getDouble(1) + " ms");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
