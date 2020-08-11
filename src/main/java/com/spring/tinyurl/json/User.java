package com.spring.tinyurl.json;


import com.spring.tinyurl.json.ShortUrl;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds information for an account.
 */
@Document(collection = "users")
public class User {

    @Id
    private String _id;
    private Map<String, ShortUrl> shorts;

    public Map<String, ShortUrl> getShorts() {
        return shorts;
    }

    public void setShorts(Map<String, ShortUrl> shorts) {
        this.shorts = shorts;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    @Override
    public String toString() {
        return "User{" +
                "_id='" + _id + '\'' +
                ", shorts=" + shorts +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(_id, user._id) &&
                Objects.equals(shorts, user.shorts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_id, shorts);
    }
}