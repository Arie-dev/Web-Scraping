package com.dnsbelgium.crawler;

import org.springframework.stereotype.Service;
import java.util.List;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * Service class that provides methods to fetch crawled link data from the database.
 * 
 * This class interacts with the DuckDB database and retrieves all the records from the "links" table.
 * It is used by the controller layer to display the link data to the user interface.
 */
@Service
public class CrawlerService {

    /**
     * Path to the replicated DuckDB database file where the crawled link data is stored.
     */
    private static final String REPLICATED_DB_PATH = "./db/crawler_db_copy.duckdb";

    /**
     * Fetches all links stored in the "links" table of the replicated DuckDB database.
     * 
     * This method establishes a connection to the database, executes a SELECT query to fetch
     * all rows from the "links" table, and maps the result set to a list of {@link LinkData} objects.
     *
     * @return a list of {@link LinkData} objects representing all the links in the database
     */
    public List<LinkData> getLinks() {
        // List to store the link data retrieved from the database
        List<LinkData> links = new ArrayList<>();
        
        // Establish a connection, execute the query, and process the results
        try (Connection conn = MyDuckDBConnection.connect(REPLICATED_DB_PATH);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM links")) {

            // Iterate through the result set and map each row to a LinkData object
            while (rs.next()) {
                LinkData link = new LinkData();
                link.setUrl(rs.getString("url"));
                link.setTitle(rs.getString("title"));
                link.setType(rs.getString("type"));
                link.setResponseTime(rs.getDouble("response_time"));
                links.add(link);
            }
        } catch (Exception e) {
            // Print stack trace for debugging in case of an exception
            e.printStackTrace();
        }
        
        // Return the list of links
        return links;
    }
}
