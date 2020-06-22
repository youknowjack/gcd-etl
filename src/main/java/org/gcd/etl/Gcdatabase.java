package org.gcd.etl;


public final class Gcdatabase {
    private String url;
    private String user;
    private String password;
    private GcdSchema gcdSchema;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public GcdSchema getGcdSchema() {
        return gcdSchema;
    }

    public void setGcdSchema(GcdSchema gcdSchema) {
        this.gcdSchema = gcdSchema;
    }

    @Override
    public String toString() {
        return "Gcdatabase{" + "url='" + url + '\'' + ", user='" + user + '\'' + ", password='" + password + '\'' + ", schema=" +
                gcdSchema + '}';
    }
}
