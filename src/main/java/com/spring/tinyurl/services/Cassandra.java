package com.spring.tinyurl.services;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.*;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.spring.tinyurl.configs.CassandraConfig;
import com.spring.tinyurl.entities.UserClicks;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable;

@Component
public class Cassandra {
    private static final String USERS_CLICKS_TABLE = "users_clicks";
    private static final String ID_COLUMN = "id";
    private static final String NAME_COLUMN = "name";
    private static final String CLICKS_COLUMN = "clicks";

    @Autowired
    private CqlSession cassandraSession;

    public  void insertUserClicks(UserClicks user) {
        insertUserClicks(user.getId(), user.getName(), user.getClicks());
    }

    public void insertUserClicks(String userId, String userName) {
        insertUserClicks(userId, userName, 0);
    }

    public void insertUserClicks(String userId, String userName, int clicks) {
        cassandraSession.execute(
                insertInto(USERS_CLICKS_TABLE)
                        .value(ID_COLUMN, literal(userId))
                        .value(NAME_COLUMN, literal(userName))
                        .value(CLICKS_COLUMN, literal(clicks))
                        .ifNotExists()
                        .build());
    }

    public void incrementUserClicks(UserClicks user) {
        incrementUserClicks(user.getId());
    }

    public void incrementUserClicks(String userId) {
        incrementUserClicks(userId, 1);
    }

    public void incrementUserClicks(String userId, int increaseVal) {
        int userClicks = getUserClicks(userId).getClicks();
        cassandraSession.execute(
                update(USERS_CLICKS_TABLE)
                        .setColumn(CLICKS_COLUMN, add(
                                literal(userClicks),
                                literal(increaseVal)))
                        .whereColumn(ID_COLUMN).isEqualTo(literal(userId))
                        .build());
    }

    public List<UserClicks> getClicksSummery() {
        return cassandraSession.execute(
                selectFrom(USERS_CLICKS_TABLE).all()
                        .build())
                .all().stream().map(row -> new UserClicks(
                        row.getString(ID_COLUMN),
                        row.getString(NAME_COLUMN),
                        row.getInt(CLICKS_COLUMN)))
                .collect(Collectors.toList());
    }

    public UserClicks getUserClicks(String userId) {
        Row userClicksRow =  cassandraSession.execute(
                selectFrom(USERS_CLICKS_TABLE).all()
                        .whereColumn(ID_COLUMN).isEqualTo(literal(userId))
                        .build())
                .one();

        return UserClicks.parse(userClicksRow);
    }

    public void dropUsersClicksTable() {
        drop(USERS_CLICKS_TABLE);
    }

    public void drop(String tableName) {
        cassandraSession.execute(
                dropTable(tableName)
                        .ifExists()
                        .build());
    }

    @PostConstruct
    public void createUsersClicksTable() {
        //        cassandraSession.execute("create table IF NOT EXISTS " +
        //                "clicks.user_clicks( username text, tiny text, url text, click_time timestamp, PRIMARY KEY((username, tiny),click_time) )  WITH CLUSTERING ORDER BY (click_time DESC)");

        cassandraSession.execute(
                SchemaBuilder.createTable(
                        "codding", USERS_CLICKS_TABLE).ifNotExists()
                        .withPartitionKey(ID_COLUMN, DataTypes.TEXT)
                        .withColumn(NAME_COLUMN, DataTypes.TEXT)
                        .withColumn(CLICKS_COLUMN, DataTypes.INT)
                        .build());
    }
}
