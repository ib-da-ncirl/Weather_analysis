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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.analysis.AnalysisTableReducer.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;

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
    private List<Cache> cache;
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
            if (splits.length == 2) {
                this.diffTypeName = splits[0];
                int value = Integer.parseInt(splits[1]);
                if (splits[0].equalsIgnoreCase("season")) {
                    seasonal = value;
                    differencing = 1;
                } else {
                    differencing = value;
                    seasonal = 1;
                }
            } else {
                throw new IllegalArgumentException("Unrecognised " + CFG_TRANSFORM_DIFFERENCING + "argument: " + diffSetting);
            }
        }
        if (differencing <= 0 && seasonal <= 0) {
            cache = Arrays.asList();
            statsAccumulators = new StatsAccumulator[0];
        } else {
            cache = Arrays.asList(new Cache[differencing]);
            statsAccumulators = new StatsAccumulator[differencing];
            for (int i = 0; i < differencing; ++i) {
                cache.set(i, new Cache(i * seasonal, seasonal));
                statsAccumulators[i] = new StatsAccumulator();
            }
        }
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        final int[] stepCount = {0};

        String name = key.getMainKey();

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();
            long timestamp = v.getTimestamp();

            String row = Utils.getRowName(key.getSubKey());
            Put put = new Put(Bytes.toBytes(row))
                    // zero transformed value
                    .addColumn(FAMILY_BYTES, key.getMainKey().getBytes(), storeValueAsString(value));

            final double[] diffVal = new double[] {value};
            cache.forEach(c -> {
                c.addValue(stepCount[0], diffVal[0])
                    .ifPresent(d -> {
                        // id is trigger + 1 for differencing or cache len for seasonal
                        int id = c.prev.length == 1 ? (c.trigger + 1) : c.prev.length;
                        String tag = key.getMainKey() + "_" + diffTypeName + "_" + id;
                        put.addColumn(FAMILY_BYTES, tag.getBytes(), storeValueAsString(d));
                        diffVal[0] = d;

                        statsAccumulators[c.trigger].setTag(tag);
                        statsAccumulators[c.trigger].addValue(d, timestamp);
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

            Put put = new Put(Bytes.toBytes(a.getTag()))
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

        public Cache(int trigger, int depth) {
            this.trigger = trigger;
            this.prev = new double[depth];
            this.saveIdx = -1;
            this.readIdx = 0;
            this.open = false;
        }

        int next_idx() {
            if (open) {
                readIdx = (readIdx + 1) % prev.length;
            }
            saveIdx = (saveIdx + 1) % prev.length;
            return saveIdx;
        }

        Optional<Double> addValue(int step, double value) {
            Optional<Double> difference = Optional.empty();
            if (step == trigger) {
                prev[next_idx()] = value;
            } else if (step > trigger) {
                if (!open) {
                    open = (saveIdx == prev.length - 1);
                }
                if (open) {
                    difference = Optional.of(value - prev[readIdx]);
                }
                prev[next_idx()] = value;
            }
            return difference;
        }
    }
}
