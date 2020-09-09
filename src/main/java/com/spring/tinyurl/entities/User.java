package com.spring.tinyurl.entities;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;
import java.util.Objects;

/**
 * Holds information for an account.
 */
@Document(collection = "users")
public class User {

    @Id
    private String id;
    private String name;
    private int allUrlClicks;
    private Map<String, ShortUrl> shorts;

    public void setName(String name) {
        this.name = name;
    }

    public User() {}

    public User(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getClicksAmount() {
        return allUrlClicks;
    }

    public int incrementClicks() {
        return incrementClicks(1);
    }
    public int incrementClicks(int value) {
        return allUrlClicks += value;
    }

    public int decrementClicks() {
        return decrementClicks(1);
    }
    public int decrementClicks(int value) {
        return allUrlClicks -= value;
    }

    public Map<String, ShortUrl> getShorts() {
        return shorts;
    }

    public void setShorts(Map<String, ShortUrl> shorts) {
        this.shorts = shorts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' + ", " +
                "name='" + name + '\'' + ", " +
                "shorts=" + shorts + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(id, user.id) &&
                Objects.equals(shorts, user.shorts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, shorts);
    }
}