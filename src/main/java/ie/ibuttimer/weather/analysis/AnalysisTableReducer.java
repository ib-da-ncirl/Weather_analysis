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

import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.TimeSeriesData;
import ie.ibuttimer.weather.misc.AppLogger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static ie.ibuttimer.weather.Constants.DATETIME_FMT;
import static ie.ibuttimer.weather.Constants.FAMILY_BYTES;

/**
 * Reducer to perform statistical analysis
 *
 * Calculated mean and standard deviation based on
 * https://learning.oreilly.com/library/view/Art+of+Computer+Programming,+Volume+2,+The:+Seminumerical+Algorithms/9780321635778/ch04.html#page_232
 */
public class AnalysisTableReducer extends TableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("AnalysisTableReducer"));

    public static final byte[] COUNT = "count".getBytes();
    public static final byte[] MIN = "min".getBytes();
    public static final byte[] MAX = "max".getBytes();
    public static final byte[] MEAN = "mean".getBytes();
    public static final byte[] VARIANCE = "variance".getBytes();
    public static final byte[] STD_DEV = "std_dev".getBytes();
    public static final byte[] MIN_TS = "min_ts".getBytes();
    public static final byte[] MAX_TS = "max_ts".getBytes();

    private long count;
    private double min;
    private double max;
    private double mean;
    private double variance;
    private long minTimestamp;
    private long maxTimestamp;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        this.count = 0;
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;
        this.mean = 0.0;
        this.variance = 0.0;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();

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

            long timestamp = v.getTimestamp();
            if (timestamp < minTimestamp) {
                minTimestamp = timestamp;
            }
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
            }
        });

        // add entry with column name as row id
        String name = key.getMainKey();
        double stdDev = Math.sqrt(variance);
        String minTs = LocalDateTime.ofEpochSecond(minTimestamp, 0, ZoneOffset.UTC).format(DATETIME_FMT);
        String maxTs = LocalDateTime.ofEpochSecond(maxTimestamp, 0, ZoneOffset.UTC).format(DATETIME_FMT);
        Put put = new Put(Bytes.toBytes(name))
            .addColumn(FAMILY_BYTES, COUNT, Bytes.toBytes(count))
            .addColumn(FAMILY_BYTES, MIN, Bytes.toBytes(min))
            .addColumn(FAMILY_BYTES, MAX, Bytes.toBytes(max))
            .addColumn(FAMILY_BYTES, MEAN, Bytes.toBytes(mean))
            .addColumn(FAMILY_BYTES, VARIANCE, Bytes.toBytes(variance))
            .addColumn(FAMILY_BYTES, STD_DEV, Bytes.toBytes(stdDev))
            .addColumn(FAMILY_BYTES, MIN_TS, Bytes.toBytes(minTs))
            .addColumn(FAMILY_BYTES, MAX_TS, Bytes.toBytes(maxTs));

        logger.logger().info(
                String.format("%s: count=%d  min=%f  max=%f  mean=%f  variance=%f  stdDev=%f  minTs=%s  maxTs=%s",
                        name, count, min, max, mean, variance, stdDev, minTs, maxTs));

        context.write(null, put);
    }
}
