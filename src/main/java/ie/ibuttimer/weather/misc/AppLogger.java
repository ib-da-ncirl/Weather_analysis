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

import org.apache.log4j.Logger;

public class AppLogger {

    private Logger logger;

    private AppLogger(Logger logger) {
        this.logger = logger;
    }

    public static AppLogger of(Logger logger) {
        return new AppLogger(logger);
    }

    public Logger logger() {
        return logger;
    }

    public void warn(Object message) {
        logger.warn("Warning: " + message);
    }

    public void warn(Object message, Throwable t) {
        logger.warn("Warning: " + message, t);
    }

    public void error(Object message) {
        logger.warn("Error: " + message);
    }

    public void error(Object message, Throwable t) {
        logger.warn("Error: " + message, t);
    }
}
