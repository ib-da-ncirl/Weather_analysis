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

package ie.ibuttimer.weather.analysis;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


/**
 * Basic stats calculation
 *
 * - Calculated mean and standard deviation based on
 *   https://learning.oreilly.com/library/view/Art+of+Computer+Programming,+Volume+2,+The:+Seminumerical+Algorithms/9780321635778/ch04.html#page_232
 */
public class StatsAccumulator {
    private long count;
    private double min;
    private double max;
    private double mean;
    private double variance;
    private long minTimestamp;
    private long maxTimestamp;
    private String tag;

    public StatsAccumulator() {
        reset();
    }

    public void reset() {
        this.count = 0;
        this.min = Double.MAX_VALUE;
        this.max = Long.MIN_VALUE;  // 0.0 is not greater than Double.MIN_VALUE, as Double.MIN_VALUE "is smallest positive nonzero value of type double"
        this.mean = 0.0;
        this.variance = 0.0;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
    }

    public void addValue(double value, long timestamp) {
        ++count;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        double delta = value - mean;
        mean += (delta / count);
        double delta2 = value - mean;
        variance += (delta * delta2);

        if (timestamp < minTimestamp) {
            minTimestamp = timestamp;
        }
        if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
        }
    }

    public long getCount() {
        return count;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        return variance;
    }

    public double getSetDev() {
        return Math.sqrt(variance);
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public String getMinTimestamp(DateTimeFormatter dateTimeFormatter) {
        return LocalDateTime.ofEpochSecond(minTimestamp, 0, ZoneOffset.UTC).format(dateTimeFormatter);
    }

    public String getMaxTimestamp(DateTimeFormatter dateTimeFormatter) {
        return LocalDateTime.ofEpochSecond(maxTimestamp, 0, ZoneOffset.UTC).format(dateTimeFormatter);
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String toString() {
        return "StatsAccumulator{" +
                "count=" + count +
                ", min=" + min +
                ", max=" + max +
                ", mean=" + mean +
                ", variance=" + variance +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", tag='" + tag + '\'' +
                '}';
    }
}
