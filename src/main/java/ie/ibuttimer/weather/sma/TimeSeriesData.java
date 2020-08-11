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

package ie.ibuttimer.weather.sma;

import ie.ibuttimer.weather.misc.Value;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * A class representing time series data as a timestamp and value
 *
 * This implementation is based on Chapter 6. "Moving Average" from
 * "Data Algorithms" by Dr. Mahmoud Parsian
 * Publisher: O'Reilly Media, Inc.
 * Release Date: July 2015
 * ISBN: 9781491906187
 * URL: https://learning.oreilly.com/library/view/data-algorithms/9781491906170/
 */
public class TimeSeriesData implements WritableComparable<TimeSeriesData> {

    private long timestamp;
    private Value value;

    public TimeSeriesData(long timestamp, Value value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public TimeSeriesData() {
        this.timestamp = 0L;
        this.value = Value.of();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    /**
     * Compare object based on timestamp
     * @param timeSeriesData    Object to compare to
     * @return
     */
    @Override
    public int compareTo(TimeSeriesData timeSeriesData) {
        int order = 0;
        if (this.timestamp < timeSeriesData.timestamp) {
            order = -1;
        } else if (this.timestamp > timeSeriesData.timestamp) {
            order = 1;
        }
        return order;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(timestamp);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.value.readFields(dataInput);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesData that = (TimeSeriesData) o;

        if (timestamp != that.timestamp) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public static class TimeSeriesDataComparator extends WritableComparator {
        public TimeSeriesDataComparator() {
            super(TimeSeriesData.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(TimeSeriesData.class, new TimeSeriesDataComparator());
    }

}
