package com.spring.tinyurl.json;

import java.util.Objects;

public class NewTinyRequest {
    private String longUrl;
    private String user;

    @Override
    public String toString() {
        return "NewTinyRequest{" +
                "longUrl='" + longUrl + '\'' +
                ", user='" + user + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewTinyRequest that = (NewTinyRequest) o;
        return Objects.equals(longUrl, that.longUrl) &&
                Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {

        return Objects.hash(longUrl, user);
    }

    public String getLongUrl() {

        return longUrl;
    }

    public String getUser() {
        return user;
    }

    public void setLongUrl(String longUrl) {
        this.longUrl = longUrl;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
