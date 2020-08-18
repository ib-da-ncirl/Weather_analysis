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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer to perform Simple Moving Average functionality
 */
public class SmaFileReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text>
                            implements SmaReducerEngine.ISmaReduceOutput<CompositeKey, TimeSeriesData, Text, Text> {

    private SmaReducerEngine<CompositeKey, TimeSeriesData, Text, Text> engine;

    private final Text outKey = new Text();
    private final Text outValue = new Text();


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
        outKey.set(key.getMainKey());
        outValue.set(String.format("%s: actual: %5.2f  moving avg: %5.2f  error %5.2f  sq error %5.2f",
                dateTime, value, movingAvg, error, Math.pow(error, 2)));

        context.write(outKey, outValue);
    }
}
