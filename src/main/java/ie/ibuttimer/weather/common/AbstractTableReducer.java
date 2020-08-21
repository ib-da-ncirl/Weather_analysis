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

package ie.ibuttimer.weather.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;

public abstract class AbstractTableReducer<KEYIN, VALUEIN, KEYOUT> extends TableReducer<KEYIN, VALUEIN, KEYOUT> {

    protected void write(Context context, Put put) {
        try {
            context.write(null, put);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void addModelMetrics(Context context, String key, ErrorTracker errorTracker, int numParams, String params) {
        double mse = errorTracker.getMSE();
        double maape = errorTracker.getMAAPE();
        Put put = new Put(Bytes.toBytes(STATS_ROW_MARK + key))
                .addColumn(FAMILY_BYTES, MSE.getBytes(), storeValueAsString(mse))
                .addColumn(FAMILY_BYTES, MAAPE.getBytes(), storeValueAsString(maape))
                .addColumn(FAMILY_BYTES, AIC_MSE.getBytes(), storeValueAsString(errorTracker.getAIC(numParams, mse)))
                .addColumn(FAMILY_BYTES, AIC_MAAPE.getBytes(), storeValueAsString(errorTracker.getAIC(numParams, maape)))
                .addColumn(FAMILY_BYTES, PARAMS.getBytes(), storeValueAsString(params));

        write(context, put);
    }
}
