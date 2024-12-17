package com.dnsbelgium.crawler;

import org.springframework.stereotype.Service;
import java.util.List;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

@Service
public class CrawlerService {

    private static final String REPLICATED_DB_PATH = "./db/crawler_db_copy.duckdb";

    public List<LinkData> getLinks() {
        List<LinkData> links = new ArrayList<>();
        try (Connection conn = MyDuckDBConnection.connect(REPLICATED_DB_PATH);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM links")) {

            while (rs.next()) {
                LinkData link = new LinkData();
                link.setUrl(rs.getString("url"));
                link.setTitle(rs.getString("title"));
                link.setType(rs.getString("type"));
                link.setResponseTime(rs.getDouble("response_time"));
                links.add(link);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return links;
    }
}