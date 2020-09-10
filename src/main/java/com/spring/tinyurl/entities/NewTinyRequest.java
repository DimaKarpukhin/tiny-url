package com.spring.tinyurl.entities;

import java.util.Objects;

public class NewTinyRequest
{
    private String longUrl;
    private String userId;

    public NewTinyRequest(String longUrl, String userId) {
        this.longUrl = longUrl;
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "NewTinyRequest{" +
                "longUrl='" + longUrl + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewTinyRequest that = (NewTinyRequest) o;
        return Objects.equals(longUrl, that.longUrl) &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(longUrl, userId);
    }

    public String getLongUrl() {

        return longUrl;
    }

    public String getUserId() {
        return userId;
    }

    public void setLongUrl(String longUrl) {
        this.longUrl = longUrl;
    }
}
