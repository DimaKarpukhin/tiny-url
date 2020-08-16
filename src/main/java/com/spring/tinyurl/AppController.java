package com.spring.tinyurl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.tinyurl.json.NewTinyRequest;
import com.spring.tinyurl.json.User;
import com.spring.tinyurl.util.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Spring Boot Hello案例
 * <p>
 * Created by bysocket on 26/09/2017.
 */
@RestController
@RequestMapping(value = "")
public class AppController {

    public static final int MAX_RETRIES = 3;
    private static final int TINY_LENGTH = 7;

    @Autowired
    private MongoTemplate m_MongoTemplate;

    @Autowired
    private RedisUtil m_RedisRepository;

    @Autowired
    private ObjectMapper m_ObjectMapper;

    @Value("${app.baseurl}")
    String baseurl;

    Random m_Random = new Random();

    @RequestMapping(value = "", method = RequestMethod.GET)
    public String SayHello()
    {
        return "Hello!";
    }


    @RequestMapping(value = "/app/user", method = RequestMethod.GET)
    public List<User> GetAllUsers() throws JsonProcessingException
    {
        return m_MongoTemplate.findAll(User.class, "users");
    }

    @RequestMapping(value = "/app/user", method = RequestMethod.POST)
    public String CreateUser(@RequestBody User user)
    {
        m_MongoTemplate.insert(user,"users");

        return "OK";
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    public String CreateNewTinyUrl(@RequestBody NewTinyRequest request) throws JsonProcessingException
    {

        System.out.println("Debug1");
        request.setLongUrl(addHttpsIfNotPresent(request.getLongUrl()));
        for (int i = 0; i < MAX_RETRIES; i++)
        {
           String candidate = generateTinyUrl();

           if (m_RedisRepository.set(candidate, m_ObjectMapper.writeValueAsString(request)))
           {
               if (request.getUser() != null)
               {
                   Query query = Query.query(Criteria.where("_id").is(request.getUser()));
                   Update update = new Update().set("shorts."  + candidate, new HashMap() );
                   m_MongoTemplate.updateFirst(query, update, "users");
               }
               return baseurl + candidate + "/";
           }
        }

        return "failed";
    }

    @RequestMapping(value = "/{tiny}/", method = RequestMethod.GET)
    public ModelAndView Redirect(@PathVariable String tiny) throws JsonProcessingException
    {
        System.out.println("Debug2");
        NewTinyRequest redirectTo = m_ObjectMapper.readValue(m_RedisRepository.get(tiny).toString(), NewTinyRequest.class);

        if (redirectTo.getUser() != null)
        {
            Query query = Query.query(Criteria.where("_id").is(redirectTo.getUser()));
            Update update = new Update().inc("shorts."  + tiny + ".clicks." + getCurMonth(), 1 );
            m_MongoTemplate.updateFirst(query, update, "users");
        }
        System.out.println(redirectTo);

        return new ModelAndView("redirect:" + redirectTo.getLongUrl());
    }

    private String getCurMonth()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/yyyy");
        Date date = new Date();

        System.out.println(">>>>>>>" + formatter.format(date));

        return formatter.format(date);
    }


    private String addHttpsIfNotPresent(@RequestParam String longUrl)
    {
        if (!longUrl.startsWith("http"))
        {
            longUrl = "https://" + longUrl;
        }

        return longUrl;
    }

    private String generateTinyUrl()
    {
        String charpool = "ABCDEFHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        String res = "";
        for (int i = 0; i < TINY_LENGTH; i++) {
            res += charpool.charAt(m_Random.nextInt(charpool.length()));
        }

        return res;
    }

}