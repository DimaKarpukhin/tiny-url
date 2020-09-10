package com.spring.tinyurl.entities;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Objects;

public class UserClicks {
    private String id;
    private String name;
    private int clicks;

    public UserClicks() {}

    public UserClicks(String id, String name, int clicks) {
        this.id = id;
        this.name = name;
        this.clicks = clicks;
    }

    private static String trimQuotes(String string){
        return string.substring(
                string.indexOf("'") + 1,
                string.indexOf("'", 1));
    }

    @Override
    public String toString() {
        return "UserClicks{"
                + "id='" + id + '\''
                + ", name='" + name + '\''
                + ", clicks=" + clicks
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserClicks that = (UserClicks) o;
        return clicks == that.clicks
                && Objects.equals(id, that.id)
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, clicks);
    }

    public static UserClicks parse(Row userClicks){
        if(userClicks != null) {
            try {
                String[] columns = userClicks.getFormattedContents().split(",");
                String id = extractId(columns);
                String name = extractName(columns);
                int clicks = extractClicks(columns);
                return new UserClicks(id, name, clicks);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return null;
            }
        }else {
            return null;
        }
    }

    private static String extractId(String[] columns) {
        return trimQuotes(extractValue(columns, "id"));
    }

    private static String extractName(String[] columns) {
        return trimQuotes(extractValue(columns, "name"));
    }

    private static int extractClicks(String[] columns) {
        return Integer.parseInt(extractValue(columns, "clicks"));
    }

    private static String extractValue(String[] columns, String key){
        String result = null;
        for (String column : columns) {
            if (column.contains(key)) {
                result = column.split(":")[1].trim();
                break;
            }
        }

        return result;
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

    public void setName(String name) {
        this.name = name;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }
}
