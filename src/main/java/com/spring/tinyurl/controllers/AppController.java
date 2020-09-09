package com.spring.tinyurl.controllers;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.client.model.*;
import com.spring.tinyurl.entities.NewTinyRequest;
import com.spring.tinyurl.entities.User;
import com.spring.tinyurl.entities.UserClicks;
import com.spring.tinyurl.services.Cassandra;
import com.spring.tinyurl.services.Redis;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.PostConstruct;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;
import static reactor.core.publisher.Mono.first;

/**
 * Spring Boot Hello案例
 * <p>
 * Created by bysocket on 26/09/2017.
 */
@RestController
@RequestMapping(value = "")
public class AppController {

    public static final int MAX_ATTEMPTS = 3;
    private static final int TINY_LENGTH = 7;

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private Redis redis;
    @Autowired
    private Cassandra cassandra;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${app.baseurl}")
    String baseurl;

    private Random random = new Random();

    @RequestMapping(value = "/clicks", method = RequestMethod.GET)
    public List<UserClicks> getClicksSummery() {
        return cassandra.getClicksSummery();
    }

    @RequestMapping(value = "/users", method = RequestMethod.GET)
    public List<User> getAllUsers() {
        return mongoTemplate.findAll(User.class, "users");
    }

    @RequestMapping(value = "/newUser", method = RequestMethod.POST)
    public String createUser(@RequestParam String id, @RequestParam String name) {
        User user = new User(id,name);
        mongoTemplate.insert(user,"users");
        //cassandra.createUsersClicksTable();
        cassandra.insertUserClicks(id, name);

        return "OK";
    }

    @RequestMapping(value = "/newTinyUrl", method = RequestMethod.POST)
    public String createTinyUrl(@RequestBody NewTinyRequest request) throws JsonProcessingException {
        String result = "failed";
        String tinyUrl, userId;
        request.setLongUrl(addHttpsIfNotPresent(request.getLongUrl()));
        userId = request.getUserId();
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
           tinyUrl = generateTinyUrl();
           if (redis.set(tinyUrl, objectMapper.writeValueAsString(request))) {
               if (userId != null) {
                   Query query = Query.query(Criteria.where("_id").is(userId));
                   Update update = new Update().set("shorts."  + tinyUrl, new HashMap() );
                   mongoTemplate.updateFirst(query, update, "users");
               }
               result = baseurl + tinyUrl + "/";
               break;
           }
        }

        return result;
    }

    @RequestMapping(value = "/{tiny}/", method = RequestMethod.GET)
    public ModelAndView redirect(@PathVariable String tiny) throws JsonProcessingException {
        NewTinyRequest tinyRequest = objectMapper.readValue(
                redis.get(tiny).toString(), NewTinyRequest.class);
        String userId = tinyRequest.getUserId();
        if ( userId != null) {
            Query query = Query.query(Criteria.where("_id").is(userId));
            Update update = new Update().inc("shorts."  + tiny + ".clicks." + getCurMonth(), 1 );
            mongoTemplate.updateFirst(query, update, "users");
            cassandra.incrementUserClicks(userId);
        }

        return new ModelAndView("redirect:" + tinyRequest.getLongUrl());
    }

    private String getCurMonth() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM");
        Date date = new Date();

        return formatter.format(date);
    }

    private String addHttpsIfNotPresent(@RequestParam String longUrl) {
        return  !longUrl.startsWith("http")? "https://" + longUrl: longUrl;
    }

    private String generateTinyUrl() {
        String charPool = "ABCDEFHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < TINY_LENGTH; i++) {
            res.append(charPool.charAt(random.nextInt(charPool.length())));
        }

        return res.toString();
    }

}