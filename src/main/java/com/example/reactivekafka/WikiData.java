package com.example.reactivekafka;

public class WikiData {
    private Long id;
    private String type;
    private String title;
    private String title_url;
    private String comment;
    private String user;
    private String wiki;

    public WikiData() {
    }

    public WikiData(Long id, String type, String title, String title_url, String comment, String user, String wiki) {
        this.id = id;
        this.type = type;
        this.title = title;
        this.title_url = title_url;
        this.comment = comment;
        this.user = user;
        this.wiki = wiki;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle_url() {
        return title_url;
    }

    public void setTitle_url(String title_url) {
        this.title_url = title_url;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getWiki() {
        return wiki;
    }

    public void setWiki(String wiki) {
        this.wiki = wiki;
    }
}
