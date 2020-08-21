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

import com.google.common.collect.Lists;
import ie.ibuttimer.weather.analysis.StatsAccumulator;
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
import java.util.*;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;
import static ie.ibuttimer.weather.misc.Utils.buildTag;

/**
 * Reducer to perform differencing
 *
 * - Calculates the auto correlation
 */
public class DifferencingTableReducer extends AbstractTableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("DifferencingTableReducer"));

    private int differencing;
    private int seasonal;
    private List<Cache> cacheList;
    private StatsAccumulator[] statsAccumulators;
    private String diffTypeName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        seasonal = 0;
        differencing = 0;
        String diffSetting = conf.get(CFG_TRANSFORM_DIFFERENCING, "");
        if (!StringUtils.isEmpty(diffSetting)) {
            String[] splits = diffSetting.split(",");
            boolean ok = (splits.length == 2);
            if (ok) {
                this.diffTypeName = splits[0];
                int value = Integer.parseInt(splits[1]);
                if (splits[0].equalsIgnoreCase(SEASON)) {
                    seasonal = value;   // seasonal width
                    differencing = 1;   // just 1 diff run
                } else if (splits[0].equalsIgnoreCase(STEP)) {
                    differencing = value;   // multiple diff run
                    seasonal = 1;           // width of 1
                } else {
                    ok = false;
                }
            }
            if (!ok) {
                throw new IllegalArgumentException("Unrecognised " + CFG_TRANSFORM_DIFFERENCING + "argument: " + diffSetting);
            }
        }

        cacheList = Lists.newArrayList(new Cache(0, 0));    // 1st is just pass-through
        if (differencing <= 0 && seasonal <= 0) {
            statsAccumulators = new StatsAccumulator[1];    // stats for pass-through
        } else {
            statsAccumulators = new StatsAccumulator[differencing + 1]; // lags + pass-through
            for (int i = 0; i < statsAccumulators.length; ++i) {
                if (i < differencing) {
                    cacheList.add(new Cache(i * seasonal, seasonal));
                }
                statsAccumulators[i] = new StatsAccumulator();
            }
        }
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        final int[] stepCount = {0};

        String name = key.getMainKey();

        cacheList.forEach(c -> {
            // id is cache len; 0,1,..
            c.tag = getDifferenceColumnName(key, diffTypeName, c.prev.length);

            statsAccumulators[c.prev.length].setTag(c.tag);
        });

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();
            long timestamp = v.getTimestamp();

            String row = Utils.getRowName(key.getSubKey());
            Put put = new Put(Bytes.toBytes(row));

            final double[] diffVal = new double[] {value};
            cacheList.forEach(c -> {
                c.addValue(stepCount[0], diffVal[0])
                    .ifPresent(d -> {
                        put.addColumn(FAMILY_BYTES, c.tag.getBytes(), storeValueAsString(d));
                        diffVal[0] = d;

                        statsAccumulators[c.prev.length].addValue(d, timestamp);
                    });
            });

            write(context, put);

            ++stepCount[0];
        });

        Arrays.asList(statsAccumulators).forEach(a -> {
            logger.logger().info(
                    String.format("%s: %s - count=%d  min=%f  max=%f  mean=%f  variance=%f  stdDev=%f  minTs=%s  maxTs=%s",
                            name, a.getTag(), a.getCount(), a.getMin(), a.getMax(),
                            a.getMean(), a.getVariance(), a.getSetDev(),
                            a.getMinTimestamp(DATETIME_FMT), a.getMaxTimestamp(DATETIME_FMT)));

            // mark stats rows with STATS_ROW_MARK
            Put put = new Put(Bytes.toBytes(STATS_ROW_MARK + a.getTag()))
                    .addColumn(FAMILY_BYTES, COUNT.getBytes(), storeValueAsString(a.getCount()))
                    .addColumn(FAMILY_BYTES, MIN.getBytes(), storeValueAsString(a.getMin()))
                    .addColumn(FAMILY_BYTES, MAX.getBytes(), storeValueAsString(a.getMax()))
                    .addColumn(FAMILY_BYTES, MEAN.getBytes(), storeValueAsString(a.getMean()))
                    .addColumn(FAMILY_BYTES, VARIANCE.getBytes(), storeValueAsString(a.getVariance()))
                    .addColumn(FAMILY_BYTES, STD_DEV.getBytes(), storeValueAsString(a.getSetDev()))
                    .addColumn(FAMILY_BYTES, MIN_TS.getBytes(), storeValueAsString(a.getMinTimestamp(DATETIME_FMT)))
                    .addColumn(FAMILY_BYTES, MAX_TS.getBytes(), storeValueAsString(a.getMaxTimestamp(DATETIME_FMT)));

            write(context, put);
        });

    }

    public static String getDifferenceRowName(CompositeKey key, String diffType, int id) {
        return STATS_ROW_MARK + getDifferenceColumnName(key, diffType, id);
    }

    public static String getDifferenceColumnName(CompositeKey key, String diffType, int id) {
        return buildTag(Arrays.asList(key.getMainKey(), diffType, Integer.toString(id)));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(this.hashCode()) +
                "{" +
                "differencing=" + differencing +
                '}';
    }

    private static class Cache {
        int trigger;
        double[] prev;
        int saveIdx;
        int readIdx;
        boolean open;
        String tag;

        public Cache(int trigger, int depth) {
            this.trigger = trigger;
            this.prev = new double[depth];
            this.saveIdx = -1;
            this.readIdx = 0;
            this.open = false;
            this.tag = "";
        }

        int next_idx() {
            if (open) {
                readIdx = (readIdx + 1) % prev.length;
            }
            saveIdx = (saveIdx + 1) % prev.length;
            return saveIdx;
        }

        Optional<Double> addValue(int step, double value) {
            Optional<Double> difference;
            if (prev.length == 0) {
                // pass-through
                difference = Optional.of(value);
            } else {
                difference = Optional.empty();
                if (step == trigger) {
                    // triggered so just save value
                    prev[next_idx()] = value;
                } else if (step > trigger) {
                    if (!open) {
                        // open if cache full
                        open = (saveIdx == prev.length - 1);
                    }
                    if (open) {
                        difference = Optional.of(value - prev[readIdx]);
                    }
                    prev[next_idx()] = value;
                }
            }
            return difference;
        }
    }
}
