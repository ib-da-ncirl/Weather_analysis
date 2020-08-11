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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static ie.ibuttimer.weather.Constants.*;

/**
 * Reducer to perform Simple Moving Average functionality
 */
public class SMA_Reducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    private int windowSize;
    private DateTimeFormatter dateTimeFmt;

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        windowSize = conf.getInt(CFG_MA_WINDOW_SIZE, DFLT_MA_WINDOW_SIZE);

        dateTimeFmt = new DateTimeFormatterBuilder().
                appendPattern(conf.get(CFG_DATETIME_FMT, DFLT_DATETIME_FMT)).toFormatter();
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) {

        MovingAverage movingAverage = new MovingAverage(windowSize);

        values.forEach(v -> {
                movingAverage.addNewNumber(v.getValue().doubleValue());
                long timestamp = v.getTimestamp();
                String dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).format(dateTimeFmt);

                outKey.set(key.getMainKey());
                outValue.set(String.format("%s: %5.2f", dateTime, movingAverage.getMovingAverage()));
            try {
                context.write(outKey, outValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
