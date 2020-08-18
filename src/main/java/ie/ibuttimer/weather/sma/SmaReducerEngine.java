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

import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.TimeSeriesData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.Constants.DFLT_DATETIME_FMT;

/**
 * Engine to perform common reduce function for Simple Moving Average functionality
 */
public class SmaReducerEngine<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private int windowSize;
    private DateTimeFormatter dateTimeFmt;
    private ISmaReduceOutput<KEYIN, VALUEIN, KEYOUT, VALUEOUT> output;

    private long count;
    private double sum;

    public SmaReducerEngine(int windowSize, DateTimeFormatter dateTimeFmt,
                            ISmaReduceOutput<KEYIN, VALUEIN, KEYOUT, VALUEOUT> output) {
        init(windowSize, dateTimeFmt, output);
    }

    private void init(int windowSize, DateTimeFormatter dateTimeFmt,
                      ISmaReduceOutput<KEYIN, VALUEIN, KEYOUT, VALUEOUT> output) {
        this.windowSize = windowSize;
        this.dateTimeFmt = dateTimeFmt;
        this.output = output;
        this.count = 0;
        this.sum = 0.0;
    }

    public SmaReducerEngine(Configuration conf, ISmaReduceOutput<KEYIN, VALUEIN, KEYOUT, VALUEOUT> output) {
        init(conf.getInt(CFG_MA_WINDOW_SIZE, DFLT_MA_WINDOW_SIZE),
                new DateTimeFormatterBuilder().
                    appendPattern(conf.get(CFG_DATETIME_FMT, DFLT_DATETIME_FMT)).toFormatter(),
                output);
    }

    public void reduce(CompositeKey key, Iterable<TimeSeriesData> values,
                       Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) {

        MovingAverage movingAverage = new MovingAverage(windowSize);

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            long timestamp = v.getTimestamp();
            String dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).format(dateTimeFmt);

            double value = v.getValue().doubleValue();

            ++count;
            sum += value;

            // update moving average
            movingAverage.addNewNumber(value);

            double movingAvg = movingAverage.getMovingAverage();
            double error = value - movingAvg;

            try {
                output.reduce(key, dateTime, value, movingAvg, error, context);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public long getCount() {
        return count;
    }

    public double getMean() {
        return sum/count;
    }

    /**
     * Callback interface
     * @param <KEYIN>
     * @param <VALUEIN>
     * @param <KEYOUT>
     * @param <VALUEOUT>
     */
    public interface ISmaReduceOutput<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        void reduce(CompositeKey key, String dateTime, double value, double movingAvg, double error,
                    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
                throws IOException, InterruptedException;
    }
}
