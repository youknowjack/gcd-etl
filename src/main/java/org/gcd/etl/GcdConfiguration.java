package org.gcd.etl;

public final class GcdConfiguration {
    private Gcdatabase gcdatabase;

    public Gcdatabase getGcdatabase() {
        return gcdatabase;
    }

    public void setGcdatabase(Gcdatabase gcdatabase) {
        this.gcdatabase = gcdatabase;
    }

    @Override
    public String toString() {
        return "GcdConfiguration{" + "gcdatabase=" + gcdatabase + '}';
    }
}
