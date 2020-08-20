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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;

/**
 * Reducer to perform statistical analysis
 *
 * Calculated mean and standard deviation based on
 * https://learning.oreilly.com/library/view/Art+of+Computer+Programming,+Volume+2,+The:+Seminumerical+Algorithms/9780321635778/ch04.html#page_232
 */
public class AnalysisTableReducer extends TableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("AnalysisTableReducer"));

    public static byte[] columnNameBytes(String name, int index) {
        String colName = name;
        if (index >= 0) {
            colName += "_" + index;
        }
        return colName.getBytes();
    }

    private int num_strata;
    private int strata_width;
    private StatsAccumulator[] accumulators;
    private int[] widths;
    private int current_strata;
    private StatsAccumulator overall;

    @Override
    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();

        this.num_strata = conf.getInt(CFG_NUM_STRATA, DFLT_NUM_STRATA);
        this.strata_width = conf.getInt(CFG_STRATA_WIDTH, DFLT_STRATA_WIDTH);
        this.accumulators = new StatsAccumulator[this.num_strata];
        for (int i = 0; i < this.num_strata; ++i) {
            this.accumulators[i] = new StatsAccumulator();
        }
        this.widths = new int[this.num_strata];
        Arrays.fill(widths, 0);
        this.current_strata = 0;
        this.overall = new StatsAccumulator();
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();
            long timestamp = v.getTimestamp();

            overall.addValue(value, timestamp);

            if (num_strata > 1) {
                accumulators[current_strata].addValue(value, timestamp);

                ++widths[current_strata];
                if (widths[current_strata] == strata_width) {
                    widths[current_strata] = 0;
                    current_strata = (current_strata + 1) % num_strata;
                }
            }
        });

        // add entry with column name as row id
        String name = key.getMainKey();
        write(context, overall, -1, name);
        if (num_strata > 1) {
            for (int i = 0; i < num_strata; ++i) {
                write(context, accumulators[i], i, name);
            }
        }
    }

    private void write(Context context, StatsAccumulator accumulator, int index, String name) throws IOException, InterruptedException {
        String minTs = accumulator.getMinTimestamp(DATETIME_FMT);
        String maxTs = accumulator.getMaxTimestamp(DATETIME_FMT);
        Put put = new Put(Bytes.toBytes(name))
                .addColumn(FAMILY_BYTES, columnNameBytes(COUNT, index), storeValueAsString(accumulator.getCount()))
                .addColumn(FAMILY_BYTES, columnNameBytes(MIN, index), storeValueAsString(accumulator.getMin()))
                .addColumn(FAMILY_BYTES, columnNameBytes(MAX, index), storeValueAsString(accumulator.getMax()))
                .addColumn(FAMILY_BYTES, columnNameBytes(MEAN, index), storeValueAsString(accumulator.getMean()))
                .addColumn(FAMILY_BYTES, columnNameBytes(VARIANCE, index), storeValueAsString(accumulator.getVariance()))
                .addColumn(FAMILY_BYTES, columnNameBytes(STD_DEV, index), storeValueAsString(accumulator.getSetDev()))
                .addColumn(FAMILY_BYTES, columnNameBytes(MIN_TS, index), storeValueAsString(minTs))
                .addColumn(FAMILY_BYTES, columnNameBytes(MAX_TS, index), storeValueAsString(maxTs));

        String label;
        if (index < 0) {
            label = "overall";
        } else {
            label = "strata_" + index;
        }
        logger.logger().info(
                String.format("%s: %s - count=%d  min=%f  max=%f  mean=%f  variance=%f  stdDev=%f  minTs=%s  maxTs=%s",
                        name, label, accumulator.getCount(), accumulator.getMin(), accumulator.getMax(),
                        accumulator.getMean(), accumulator.getVariance(), accumulator.getSetDev(), minTs, maxTs));

        context.write(null, put);

    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(this.hashCode()) +
                "{" +
                "num_strata=" + num_strata +
                ", strata_width=" + strata_width +
                '}';
    }
}
