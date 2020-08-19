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

import com.google.common.collect.Maps;
import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.TimeSeriesData;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static ie.ibuttimer.weather.Constants.*;

/**
 * Reducer to perform statistical analysis
 *
 * Calculated mean and standard deviation based on
 * https://learning.oreilly.com/library/view/Art+of+Computer+Programming,+Volume+2,+The:+Seminumerical+Algorithms/9780321635778/ch04.html#page_232
 */
public class TransformTableReducer extends TableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("AnalysisTableReducer"));

    public static final String TRANSFORM_MEANS = "transform.means";

    private Map<String, Double> means = Maps.newHashMap();
    private long lag;
    private Deque<Double> laggedValues = new ArrayDeque<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String meanCfg = conf.get(TRANSFORM_MEANS, "");
        Arrays.asList(meanCfg.split(";")).forEach(entry -> {
            String[] cell = entry.split(",");
            means.put(cell[0], Double.parseDouble(cell[2]));
        });

        lag = conf.getLong(CFG_TRANSFORM_LAG, 0) * 60 * 60; // lag in sec
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        final long[] first = {0};

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();
            double mean = means.getOrDefault(key.getMainKey(), 0.0);
            value -= mean;

            long timestamp = v.getTimestamp();
            Optional<Double> lagValue = Optional.empty();
            if (lag > 0) {
                if (first[0] == 0) {
                    first[0] = timestamp;
                }
                if (timestamp - lag >= first[0]) {
                    lagValue = Optional.of(laggedValues.removeFirst());
                }
                laggedValues.addLast(value);
            }

            String row = Utils.getRowName(key.getSubKey());
            Put put = new Put(Bytes.toBytes(row))
                    .addColumn(FAMILY_BYTES, key.getMainKey().getBytes(), Bytes.toBytes(value));
            lagValue.ifPresent(lv -> put.addColumn(FAMILY_BYTES,
                    (key.getMainKey() + "_lag").getBytes(), Bytes.toBytes(lv)));

            try {
                context.write(null, put);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

    }
}
