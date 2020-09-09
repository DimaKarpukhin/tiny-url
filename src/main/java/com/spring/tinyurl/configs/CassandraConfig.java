package com.spring.tinyurl.configs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.spring.tinyurl.entities.UserClicks;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable;

@Configuration
public class CassandraConfig {
    private static final String SECURE_CONNECT = "/secure-connect-tinyurl.zip";
    private static final String USERNAME = "dimaDk";
    private static final String PASSWORD = "tinyurl2516";
    private static final String KEYSPACE = "codding";

    @Bean("cassandraSession")
    public CqlSession getSession () throws URISyntaxException {
        // Create the CqlSession object:
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(getClass().getResource(SECURE_CONNECT).toURI()))
                .withAuthCredentials(USERNAME, PASSWORD)
                .withKeyspace(KEYSPACE)
                .build()) {


//            // Select the release_version from the system.local table:
//            ResultSet rs = session.execute("select release_version from system.local");
//            Row row = rs.one();
//            //Print the results of the CQL query to the console:
//            if (row != null) {
//                System.out.println(">>> [" + row.getString("release_version") + "] <<<");
//            } else {
//                System.out.println(">>> [an error occurred in: CassandraConfig.getSession()] <<<");
//            }

//            //DROP
//            Drop dropTable = dropTable("users_clicks").ifExists();
//            session.execute(dropTable.build());
//
//            //CREATE
//            CreateTableWithOptions createTable = SchemaBuilder.createTable(
//                    "codding", "users_clicks").ifNotExists()
//                    .withPartitionKey("id", DataTypes.TEXT)
//                    .withColumn("name", DataTypes.TEXT)
//                    .withColumn("clicks", DataTypes.INT);
//            System.out.println("[getSession]: " + session.toString() + createTable.build().toString());
//            session.execute(createTable.build());
//
//            //INSERT
//            Insert insert = QueryBuilder.insertInto("users_clicks")
//                    .value("id", literal("2222222"))
//                    .value("name", literal("Dima"))
//                    .value("clicks", literal(7)).ifNotExists();
//            session.execute(insert.build());
//
//            Insert insert2 = QueryBuilder.insertInto("users_clicks")
//                    .value("id", literal("333333333"))
//                    .value("name", literal("Artem"))
//                    .value("clicks", literal(10)).ifNotExists();
//            session.execute(insert2.build());
//
//            //GET ALL
//            Select select3 = selectFrom("users_clicks").all();
//            ResultSet rss =  session.execute(select3.build());
//            List<UserClicks> usersClicks = rss.all().stream().map(row1 -> new UserClicks(
//                    row1.getString("id"),
//                    row1.getString("name"),
//                    row1.getInt("clicks")))
//                    .collect(Collectors.toList());
//            for (UserClicks user: usersClicks) {
//                System.out.println(">>> " + user.toString() + " <<<");
//            }
//
//            //UPDATE
//            Select select5 = selectFrom("users_clicks").all()
//                    .whereColumn("id").isEqualTo(literal("333333333"));
//            Row r2 = session.execute(select5.build()).one();
//            UserClicks u = UserClicks.parse(r2);
//            System.out.println(">>> " + r2.getFormattedContents() + " <<<\n>>> " + u.toString() + " <<<");
//
//            Update update = update("users_clicks")
//                    .setColumn("clicks", add(literal(u.getClicks()),literal(7)))
//                    .whereColumn("id").isEqualTo(literal(u.getId()));
//            session.execute(update.build());
//            Select select4 = selectFrom("users_clicks").all()
//                    .whereColumn("id").isEqualTo(literal(u.getId()));
//            System.out.println(">>> " + session.execute(select4.build()).one().getFormattedContents() + " <<<");

            return session;
        }
    }
}