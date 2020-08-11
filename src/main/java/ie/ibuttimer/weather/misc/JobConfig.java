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
        this.properties = properties;
        this.wait = wait;
        this.verbose = true;
        this.inPathRoot = inPathRoot;
        this.outPathRoot = outPathRoot;
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
}
