package com.dnsbelgium.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class Crawler {

    private Connection conn;
    private Set<String> visitedLinks;

    public Crawler(Connection conn) {
        this.conn = conn;
        this.visitedLinks = new HashSet<>();
    }

    public void getPageLinks(String URL, int depth) {
        if (depth >= 20) { 
            System.err.println("Reached max depth of 5, skipping URL: " + URL);
            return; 
        }
        if (!visitedLinks.contains(URL)) {
            visitedLinks.add(URL);

            if (!(URL.startsWith("http://") || URL.startsWith("https://"))) {
                System.out.println("Skipping unsupported URL: " + URL);
                return; 
            }

            try {
                long startTime = System.nanoTime();

                Document document = Jsoup.connect(URL)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
                        .get();

                long responseTime = (System.nanoTime() - startTime) / 1000000;

                String title = document.title();
                String type = URL.contains("yourdomain.com") ? "internal" : "external";

                if (!isUrlInDatabase(URL)) {
                    insertLinkData(URL, title, type, responseTime);
                }

                Elements linksOnPage = document.select("a[href]");
                for (Element page : linksOnPage) {
                    String linkUrl = page.attr("abs:href");
                    getPageLinks(linkUrl, depth + 1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

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

    public static void main(String[] args) {
        String originalDbPath = "./db/crawler_db.duckdb";
        String replicatedDbPath = "./db/crawler_db_copy.duckdb";
        Connection conn = null;
        try {
            conn = MyDuckDBConnection.connect(originalDbPath);
            DuckDBTableCreator.createLinksTable(conn);
            Crawler crawler = new Crawler(conn);
            crawler.getPageLinks("https://www.dnsbelgium.be/", 0);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            MyDuckDBConnection.replicateDatabase(originalDbPath, replicatedDbPath);
            DuckDBTableCreator.createLinksTableInBothDatabases(originalDbPath, replicatedDbPath);
        }
    }
}