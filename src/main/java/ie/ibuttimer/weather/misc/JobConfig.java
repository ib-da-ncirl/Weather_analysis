/*
 * The MIT License (MIT)
 * Copyright (c) 2020 Ian Buttimer
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ie.ibuttimer.weather.misc;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Job configuration object
 */
public class JobConfig {

    private Properties properties;
    private boolean wait;
    private boolean verbose;
    private String inPathRoot = "";
    private String outPathRoot = "";

    private JobConfig(Properties properties, String inPathRoot, String outPathRoot) {
        this(properties , true, inPathRoot, outPathRoot);
    }

    private JobConfig(Properties properties, boolean wait, String inPathRoot, String outPathRoot) {
        this(properties, wait, false, inPathRoot, outPathRoot);
    }

    private JobConfig(Properties properties, boolean wait, boolean verbose, String inPathRoot, String outPathRoot) {
        this.properties = properties;
        this.wait = wait;
        this.verbose = verbose;
        this.inPathRoot = inPathRoot;
        this.outPathRoot = outPathRoot;
    }

    public static JobConfig of(Properties properties, boolean wait, boolean verbose, String inPathRoot, String outPathRoot) {
        return new JobConfig(properties, wait, verbose, inPathRoot, outPathRoot);
    }

    public static JobConfig of(Properties properties, boolean wait, String inPathRoot, String outPathRoot) {
        return new JobConfig(properties, wait, inPathRoot, outPathRoot);
    }

    public static JobConfig of(Properties properties, String inPathRoot, String outPathRoot) {
        return new JobConfig(properties, inPathRoot, outPathRoot);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean isWait() {
        return wait;
    }

    public void setWait(boolean wait) {
        this.wait = wait;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public String getInPathRoot() {
        return inPathRoot;
    }

    public void setInPathRoot(String inPathRoot) {
        this.inPathRoot = inPathRoot;
    }

    public String getOutPathRoot() {
        return outPathRoot;
    }

    public void setOutPathRoot(String outPathRoot) {
        this.outPathRoot = outPathRoot;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getProperty(String key, int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, Integer.toString(defaultValue)));
    }

    public long getProperty(String key, long defaultValue) {
        return Long.parseLong(properties.getProperty(key, Long.toString(defaultValue)));
    }

    public float getProperty(String key, float defaultValue) {
        return Float.parseFloat(properties.getProperty(key, Float.toString(defaultValue)));
    }

    public double getProperty(String key, double defaultValue) {
        return Double.parseDouble(properties.getProperty(key, Double.toString(defaultValue)));
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public void setProperty(String key, int value) {
        setProperty(key, Integer.toString(value));
    }

    public void setProperty(String key, long value) {
        setProperty(key, Long.toString(value));
    }

    public void setProperty(String key, float value) {
        setProperty(key, Float.toString(value));
    }

    public void setProperty(String key, double value) {
        setProperty(key, Double.toString(value));
    }

    public LocalDateTime getProperty(String key, LocalDateTime defaultValue, DateTimeFormatter formatter) {
        LocalDateTime ldt = defaultValue;
        String dateTime = properties.getProperty(key, "");
        if (!StringUtils.isEmpty(dateTime)) {
            ldt = Utils.getDateTime(dateTime, formatter);
        }
        return ldt;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "properties=" + properties +
                ", wait=" + wait +
                ", verbose=" + verbose +
                ", inPathRoot='" + inPathRoot + '\'' +
                ", outPathRoot='" + outPathRoot + '\'' +
                '}';
    }
}
