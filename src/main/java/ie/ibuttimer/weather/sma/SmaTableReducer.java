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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static ie.ibuttimer.weather.Constants.*;

/**
 * Reducer to perform Simple Moving Average functionality
 */
public class SmaTableReducer extends TableReducer<CompositeKey, TimeSeriesData, Text>
                            implements SmaReducerEngine.ISmaReduceOutput<CompositeKey, TimeSeriesData, Text, Mutation> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private SmaReducerEngine<CompositeKey, TimeSeriesData, Text, Mutation> engine;

    public static final byte[] ACTUAL = "actual".getBytes();
    public static final byte[] MOVING_AVG = "moving_avg".getBytes();
    public static final byte[] ERROR = "error".getBytes();
    public static final byte[] SQ_ERROR = "sq_error".getBytes();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        this.engine = new SmaReducerEngine<>(context.getConfiguration(),  this);
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) {

        engine.reduce(key, values, context);
    }

    @Override
    public void reduce(CompositeKey key, String dateTime, double value, double movingAvg, double error, Context context)
                                                                    throws IOException, InterruptedException {
        String row = "r-" +
                LocalDateTime.ofEpochSecond(key.getSubKey(), 0, ZoneOffset.UTC).format(YYYYMMDDHH_FMT);
        Put put = new Put(Bytes.toBytes(row))
            .addColumn(FAMILY_BYTES, ACTUAL, Bytes.toBytes(value))
            .addColumn(FAMILY_BYTES, MOVING_AVG, Bytes.toBytes(movingAvg))
            .addColumn(FAMILY_BYTES, ERROR, Bytes.toBytes(error))
            .addColumn(FAMILY_BYTES, SQ_ERROR, Bytes.toBytes(Math.pow(error, 2)));

        context.write(null, put);
    }
}
