package com.dnsbelgium.crawler;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Crawler class that crawls a specified website starting from a given URL, collects links from articles, 
 * and stores relevant data in a database.
 */
public class Crawler {
    // The starting domain for the crawl
    private static final String starting_domain = "https://en.wikipedia.org/wiki/Data_engineering";

    private Connection conn;
    private Set<String> visitedLinks;

    /**
     * Constructor for the Crawler class.
     * 
     * @param conn the database connection
     */
    public Crawler(Connection conn) {
        this.conn = conn;
        this.visitedLinks = new HashSet<>();
    }

    /**
     * Starts crawling the given URL up to a specified depth.
     * For each URL, it fetches the page, extracts links, and stores link data in the database.
     * 
     * @param URL the URL to start crawling from
     * @param maxDepth the maximum depth of recursion for crawling
     */
    public void getPageLinks(String URL, int maxDepth) {
        Queue<UrlDepthPair> queue = new LinkedList<>();
        queue.add(new UrlDepthPair(URL, 0));

        // Process URLs in the queue
        while (!queue.isEmpty()) {
            UrlDepthPair current = queue.poll();
            String currentUrl = current.url;
            int currentDepth = current.depth;

            // Skip URLs that exceed the maximum crawl depth
            if (currentDepth >= maxDepth) {
                System.err.println("Reached max depth of " + currentDepth + ", skipping URL: " + currentUrl);
                continue;
            }

            // Skip previously visited URLs
            if (!visitedLinks.contains(currentUrl)) {
                visitedLinks.add(currentUrl);
                System.out.println("Visiting URL: " + currentUrl + " at depth: " + currentDepth);

                // Skip unsupported URLs
                if (!(currentUrl.startsWith("http://") || currentUrl.startsWith("https://"))) {
                    System.out.println("Skipping unsupported URL: " + currentUrl);
                    continue;
                }

                // Skip URLs that contain login or registration pages
                if (currentUrl.contains("login") || currentUrl.contains("signin") || currentUrl.contains("register")) {
                    System.out.println("Skipping login or unwanted URL: " + currentUrl);
                    continue;
                }

                // Skip URLs that are not part of the English Wikipedia
                if (!currentUrl.contains("en.wikipedia.org")) {
                    System.out.println("Skipping non-English URL: " + currentUrl);
                    continue;
                }

                try {
                    // Measure the response time of the page
                    long startTime = System.nanoTime();

                    // Fetch the document
                    Document document = Jsoup.connect(currentUrl)
                            .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0")
                            .get();

                    // Calculate response time
                    long responseTime = (System.nanoTime() - startTime) / 1000000;

                    // Extract the title and type of the page
                    String title = document.title();
                    String type = currentUrl.contains(starting_domain) ? "internal" : "external";

                    // Insert the link data into the database if not already present
                    if (!isUrlInDatabase(currentUrl)) {
                        insertLinkData(currentUrl, title, type, responseTime);
                    }

                    // Select and process links from the page
                    Elements linksOnPage = document.select("div.mw-parser-output a[href]");
                    for (Element page : linksOnPage) {
                        String linkUrl = page.attr("abs:href");

                        // Skip non-article links
                        if (!linkUrl.contains("en.wikipedia.org/wiki/")) {
                            System.out.println("Skipping non-article URL: " + linkUrl);
                            continue;
                        }

                        System.err.println("URL is BEING processed RIGHT NOW: " + currentUrl + " -> " + linkUrl);
                        queue.add(new UrlDepthPair(linkUrl, currentDepth + 1));
                    }
                } catch (HttpStatusException e) {
                    System.err.println("HTTP error fetching URL: " + currentUrl + " - " + e.getMessage());
                } catch (UnsupportedMimeTypeException e) {
                    System.err.println("Unsupported MIME type: " + currentUrl + " - " + e.getMessage());
                } catch (IOException e) {
                    System.err.println("Error fetching URL: " + currentUrl + " - " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("Error processing URL: " + currentUrl + " - " + e.getMessage());
                }
            } else {
                System.out.println("Already visited URL: " + currentUrl);
            }
        }
    }

    /**
     * Checks if the URL already exists in the database.
     * 
     * @param url the URL to check
     * @return true if the URL is already in the database, false otherwise
     */
    private boolean isUrlInDatabase(String url) {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM links WHERE url = ?")) {
            pstmt.setString(1, url);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Inserts the link data into the database.
     * 
     * @param url the URL to insert
     * @param title the title of the page
     * @param type the type of the link (internal or external)
     * @param responseTime the response time of the page in milliseconds
     */
    private void insertLinkData(String url, String title, String type, long responseTime) {
        try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO links (url, title, type, response_time) VALUES (?, ?, ?, ?)")) {
            pstmt.setString(1, url);
            pstmt.setString(2, title);
            pstmt.setString(3, type);
            pstmt.setLong(4, responseTime);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method to run the crawler.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        String originalDbPath = "./db/crawler_db.duckdb";
        String replicatedDbPath = "./db/crawler_db_copy.duckdb";
        Connection conn = null;
        try {
            // Establish connection to the database
            conn = MyDuckDBConnection.connect(originalDbPath);
            DuckDBTableCreator.createLinksTable(conn);

            // Create a new crawler and start crawling
            Crawler crawler = new Crawler(conn);
            crawler.getPageLinks(starting_domain, 2);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // Replicate the database after crawling
            MyDuckDBConnection.replicateDatabase(originalDbPath, replicatedDbPath);
            DuckDBTableCreator.createLinksTableInBothDatabases(originalDbPath, replicatedDbPath);
        }
    }

    /**
     * A helper class to store URLs and their corresponding depth level during the crawling process.
     */
    private static class UrlDepthPair {
        String url;
        int depth;

        UrlDepthPair(String url, int depth) {
            this.url = url;
            this.depth = depth;
        }
    }
}
