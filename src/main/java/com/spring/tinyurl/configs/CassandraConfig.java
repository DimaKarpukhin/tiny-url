package com.spring.tinyurl.configs;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URISyntaxException;
import java.nio.file.Paths;

@Configuration
public class CassandraConfig {
    private static final String SECURE_CONNECT = "/secure-connect-tinyurl.zip";
    private static final String USERNAME = "dimaDk";
    private static final String PASSWORD = "tinyurl2516";
    private static final String KEYSPACE = "codding";

    @Bean("cassandraSession")
    public CqlSession getSession () throws URISyntaxException {
        return CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(
                        getClass().getResource(SECURE_CONNECT).toURI()))
                .withAuthCredentials(USERNAME, PASSWORD)
                .withKeyspace(KEYSPACE)
                .build();
    }
}