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

package ie.ibuttimer.weather.arima;

import ie.ibuttimer.weather.Constants;
import ie.ibuttimer.weather.common.AbstractTableReducer;
import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.ErrorTracker;
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
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
public class ArimaTableReducer extends AbstractTableReducer<CompositeKey, TimeSeriesData, Text> {

    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("ArimaTableReducer"));


    private List<Term> arTerms;     // auto-regressive terms, i.e. those applied to past values
    private List<Term> maTerms;     // moving average terms, i.e. those applied to past errors

    private Deque<Double> valueWindow;
    private Deque<Double> errorWindow;

    private double constant;

    private ErrorTracker errorTracker;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String coefficients = conf.get(CFG_ARIMA_P);
        arTerms = Arrays.asList(getCoefficients(coefficients));

        coefficients = conf.get(CFG_ARIMA_Q);
        if (coefficients.equalsIgnoreCase("none")) {
            maTerms = Collections.emptyList();
        } else {
            maTerms = Arrays.asList(getCoefficients(coefficients));
        }

        constant = conf.getDouble(CFG_ARIMA_C, 0.0);

        valueWindow = new ArrayDeque<>();
        errorWindow = new ArrayDeque<>();

        errorTracker = new ErrorTracker();
    }

    private Term[] getCoefficients(String coefficients) {
        Term[] terms;
        if (StringUtils.isEmpty(coefficients)) {
            terms = new Term[0];
        } else {
            String[] splits = coefficients.split(",");
            terms = new Term[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                terms[i] = new Term(Double.parseDouble(splits[i]), i);
            }
        }
        return terms;
    }


    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        AtomicLong count = new AtomicLong();

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();

            valueWindow.addFirst(value);

            // YUCK not very efficient but quick and dirty
            Double[] valueArray = valueWindow.toArray(new Double[arTerms.size()]);
            Double[] errorArray = errorWindow.toArray(new Double[maTerms.size()]);

            double prediction = constant +
                    arTerms.stream().mapToDouble(ar -> ar.apply(valueArray, count.get())).sum();
            double errors;
            if (count.get() == 0) {
                errors = 0;
            } else {
                errors = maTerms.stream().mapToDouble(ar -> ar.apply(errorArray, count.get())).sum();
            }
            // using convention ma coefficients are negative
            prediction -= errors;

            double error = value - prediction;
            errorTracker.addError(value, error);

            if ((valueWindow.size() > 0) && (valueWindow.size() == arTerms.size())) {
                valueWindow.removeLast();
            }
            if ((errorWindow.size() > 0) && (errorWindow.size() == maTerms.size())) {
                errorWindow.removeLast();
            }
            errorWindow.addFirst(error);

            String row = Utils.getRowName(key.getSubKey());
            Put put = new Put(Bytes.toBytes(row))
                    .addColumn(FAMILY_BYTES, ACTUAL, storeValueAsString(value))
                    .addColumn(FAMILY_BYTES, PREDICTION, storeValueAsString(prediction))
                    .addColumn(FAMILY_BYTES, ERROR, storeValueAsString(error))
                    .addColumn(FAMILY_BYTES, SQ_ERROR, storeValueAsString(Math.pow(error, 2)));

            write(context, put);

            count.incrementAndGet();
        });

        Put put = new Put(Bytes.toBytes(STATS_ROW_MARK + key.getMainKey()))
                .addColumn(FAMILY_BYTES, MSE, storeValueAsString(errorTracker.getMSE()))
                .addColumn(FAMILY_BYTES, MAAPE, storeValueAsString(errorTracker.getMAAPE()));

        write(context, put);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("arTerms=");
        arTerms.forEach(a -> {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(a.coefficient);
        });
        sb.append(",maTerms=");
        maTerms.forEach(a -> {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(a.coefficient);
        });
        return getClass().getSimpleName() + "@" + Integer.toHexString(this.hashCode()) +
                "{" +
                "constant=" + constant +
                "," + sb.toString() +
                '}';
    }

    private static class Term {
        double coefficient;
        int index;
        String tag;

        public Term(double coefficient, int index) {
            this.coefficient = coefficient;
            this.index = index;
            this.tag = "";
        }

        double apply(Double[] values, long count) {
            double val = 0.0;
            if (count >= index) {
                val = values[index] * coefficient;
            }
            return val;
        }
    }
}
