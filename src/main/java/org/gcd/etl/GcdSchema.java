package org.gcd.etl;/**
 * @author jack
 */

/**
 * @author jack@indeed.com (Jack Humphrey)
 */
public final class GcdSchema {
    private boolean publicationType = true;
    private boolean volumeNotPrinted = true;
    private boolean seriesIsSingleton = true;
    private boolean storyFirstLine = true;
    private boolean storyCredit = true;

    public boolean isPublicationType() {
        return publicationType;
    }

    public void setPublicationType(boolean hasPublicationType) {
        this.publicationType = hasPublicationType;
    }

    public boolean isVolumeNotPrinted() {
        return volumeNotPrinted;
    }

    public void setVolumeNotPrinted(boolean volumeNotPrinted) {
        this.volumeNotPrinted = volumeNotPrinted;
    }

    public boolean isSeriesIsSingleton() {
        return seriesIsSingleton;
    }

    public void setSeriesIsSingleton(boolean seriesIsSingleton) {
        this.seriesIsSingleton = seriesIsSingleton;
    }

    public boolean isStoryFirstLine() {
        return storyFirstLine;
    }

    public void setStoryFirstLine(boolean storyFirstLine) {
        this.storyFirstLine = storyFirstLine;
    }

    public boolean isStoryCredit() {
        return storyCredit;
    }

    public void setStoryCredit(boolean storyCredit) {
        this.storyCredit = storyCredit;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "GcdSchema{...}";
    }
}
