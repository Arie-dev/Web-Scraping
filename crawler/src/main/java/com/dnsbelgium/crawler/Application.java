package com.dnsbelgium.crawler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class Application {

    private static final String ORIGINAL_DB_PATH = "./db/crawler_db.duckdb";
    private static final String REPLICATED_DB_PATH = "./db/crawler_db_copy.duckdb";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Scheduled(fixedRate = 5000)
    public void replicateDatabase() {
        MyDuckDBConnection.replicateDatabase(ORIGINAL_DB_PATH, REPLICATED_DB_PATH);
        DuckDBTableCreator.createLinksTableInBothDatabases(ORIGINAL_DB_PATH, REPLICATED_DB_PATH);
    }
}