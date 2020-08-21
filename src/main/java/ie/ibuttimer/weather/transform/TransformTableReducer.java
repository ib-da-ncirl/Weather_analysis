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

package ie.ibuttimer.weather.transform;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import ie.ibuttimer.weather.common.AbstractDriver;
import ie.ibuttimer.weather.common.AbstractTableReducer;
import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.TimeSeriesData;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;

/**
 * Reducer to perform statistical analysis
 *
 * - Calculated mean and standard deviation based on
 *   https://learning.oreilly.com/library/view/Art+of+Computer+Programming,+Volume+2,+The:+Seminumerical+Algorithms/9780321635778/ch04.html#page_232
 *
 * - Produces a lagged output
 * - Calculates the auto covariance
 * - Calculates the auto correlation
 */
public class TransformTableReducer extends AbstractTableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("TransformTableReducer"));

    private HashBasedTable<String, String, Double> stats;

    private List<Accumulator> accumulators;

    boolean zeroTransform;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        stats = AbstractDriver.decodeStats(conf);

        zeroTransform = conf.getBoolean(CFG_ZERO_TRANSFORM, false);

        // lag in hours; in the form '1', '1,2,3' or range '1-10'
        String lag = conf.get(CFG_TRANSFORM_LAG, "");
        accumulators = Lists.newArrayList(genLagged("0"));
        if (!StringUtils.isEmpty(lag)) {
            if (lag.contains(",")) {
                String[] splits = lag.split(",");
                for (int i = 0; i < splits.length; ++i) {
                    accumulators.add(genLagged(splits[i]));
                }
            } else if (lag.contains("-")) {
                String[] splits = lag.split("-");
                long start = Long.parseLong(splits[0]);
                long end = Long.parseLong(splits[1]);
                for (int i = (int)start; i <= (int)end; ++i) {
                    accumulators.add(genLagged(Integer.toString(i)));
                }
            } else {
                accumulators.add(0, genLagged(lag));
            }
        }
    }

    private static final int SEC_PER_HR = 60 * 60;

    private Accumulator genLagged(String hrLag) {
        long lagLen = Long.parseLong(hrLag) * SEC_PER_HR;  // hours -> sec
        return new Accumulator(lagLen);
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        double mean = stats.get(key.getMainKey(), MEAN);

        accumulators.forEach(a -> {
            a.tag = getTransformColumnName(key, (int)a.getId());
        });
        byte[] actualColumn = getTransformColumnName(key, 0).getBytes();

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();
            double useValue = value;
            if (zeroTransform) {
                useValue -= mean;
            }

            long timestamp = v.getTimestamp();
            String row = Utils.getRowName(key.getSubKey());
            Put put = new Put(Bytes.toBytes(row));

            double finalUseValue = useValue;
            accumulators.forEach(a -> {
                a.meanDist += Math.pow(finalUseValue, 2);   // sq(y - y_bar)
                a.lagged.addValue(timestamp, value)
                        .ifPresent(lv -> {
                            // zero transformed lag value
                            double zeroTransformLag = lv - mean;
                            put.addColumn(FAMILY_BYTES, a.tag.getBytes(), storeValueAsString(zeroTransformLag));

                            a.diffProd += (finalUseValue * zeroTransformLag);
                        });
                ++a.count;
            });

            write(context, put);
        });

        accumulators.forEach(a -> {
            // calc autocovariance
            // E(X Xt) - mean2
            double autocovariance = (a.diffProd / a.count) - Math.pow(mean, 2);
            // calc autocorrelation
            // E(X Xt) - mean2
            double autocorrelation = a.diffProd / a.meanDist;

            logger.logger().info(String.format("XXX %d XXX  autocovariance %.3f   autocorrelation %.3f",
                    a.getId(), autocovariance, autocorrelation));

            Put put = new Put(Bytes.toBytes(STATS_ROW_MARK + a.tag))
                    .addColumn(FAMILY_BYTES, AUTOCOVARIANCE.getBytes(), storeValueAsString(autocovariance))
                    .addColumn(FAMILY_BYTES, AUTOCORRELATION.getBytes(), storeValueAsString(autocorrelation));

            write(context, put);

        });
    }

    public static String getTransformRowName(CompositeKey key, int id) {
        return STATS_ROW_MARK + getTransformColumnName(key, id);
    }

    public static String getTransformColumnName(CompositeKey key, int id) {
        return key.getMainKey() + "_lag_" + id;
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        accumulators.forEach(a -> {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(a.lag/SEC_PER_HR);
        });
        return getClass().getSimpleName() + "@" + Integer.toHexString(this.hashCode()) +
                "{" +
                "lag=" + sb.toString() +
                '}';
    }

    private static class Accumulator {
        long lag;
        Lagged<Double> lagged;
        double diffProd;
        double meanDist;
        long count;
        String tag;

        public Accumulator(long lag) {
            this.lag = lag;
            this.lagged = new Lagged<>(lag);
            this.diffProd = 0;
            this.meanDist = 0;
            this.count = 0;
            this.tag = "";
        }

        long getId() {
            return lag/SEC_PER_HR;
        }
    }
}
