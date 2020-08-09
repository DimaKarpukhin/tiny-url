package com.spring.tinyurl;
import com.spring.tinyurl.util.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spring Boot Hello案例
 * <p>
 * Created by bysocket on 26/09/2017.
 */
@RestController
@RequestMapping(value = "")
public class AppController {

    public static final int MAX_RETRIES = 3;
    @Autowired
    RedisUtil redisRepository;
    @Value("${app.baseurl}")
    String baseurl;

    private static final int TINY_LENGTH = 7;
    Random random = new Random();

    @RequestMapping(value = "", method = RequestMethod.GET)
    public String sayHello() {
        return "Hello!";
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    public String createNewTiny(@RequestParam String longUrl) {

        System.out.println("Debug1");
        longUrl = addHttpsIfNotPresent(longUrl);
        for (int i = 0; i < MAX_RETRIES; i++) {
           String candidate = generateTinyUrl();

           if (redisRepository.set(candidate, longUrl))
           {
               return baseurl + candidate + "/";
           }
        }

        return "failed";
    }


    @RequestMapping(value = "/{tiny}/", method = RequestMethod.GET)
    public ModelAndView redirect(@PathVariable String tiny) {
        System.out.println("Debug2");
        String redirectTo = redisRepository.get(tiny).toString();
        System.out.println(redirectTo);
        return new ModelAndView("redirect:" + redirectTo);
    }

    private String addHttpsIfNotPresent(@RequestParam String longUrl) {
        if (!longUrl.startsWith("http"))
        {
            longUrl = "https://" + longUrl;
        }
        return longUrl;
    }

    private String generateTinyUrl() {
        String charpool = "ABCDEFHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        String res = "";
        for (int i = 0; i < TINY_LENGTH; i++) {
            res += charpool.charAt(random.nextInt(charpool.length()));
        }
        return res;
    }

}