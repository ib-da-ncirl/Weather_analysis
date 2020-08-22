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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.Hbase.storeValueAsString;

/**
 * Reducer to perform ARIMA
 *
 * - Produces predicted output
 * - Calculates the MSE & MAAPE
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

    private StringBuffer modelParams;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        modelParams = new StringBuffer();

        String coefficients = conf.get(CFG_ARIMA_P);
        arTerms = Arrays.asList(getCoefficients(coefficients));
        modelParams.append(CFG_ARIMA_P).append("=").append(sanitiseParam(coefficients));

        coefficients = conf.get(CFG_ARIMA_Q);
        modelParams.append(" : ").append(CFG_ARIMA_Q).append("=").append(sanitiseParam(coefficients));
        if (coefficients.equalsIgnoreCase("none")) {
            maTerms = Collections.emptyList();
        } else {
            maTerms = Arrays.asList(getCoefficients(coefficients));
        }

        if (maTerms.size() > arTerms.size()) {
            throw new IllegalArgumentException("Number of error coefficients exceed those of lag");
        }

        constant = conf.getDouble(CFG_ARIMA_C, 0.0);
        modelParams.append(" : ").append(CFG_ARIMA_C).append("=").append(constant);

        // from other steps
        modelParams.append(" : ")
                .append(CFG_ARIMA_D).append("=").append(conf.get(CFG_ARIMA_D))
                .append(" : ").append(CFG_TRANSFORM_DIFFERENCING).append("=").append(sanitiseParam(conf.get(CFG_TRANSFORM_DIFFERENCING, "")))
                .append(" : ").append(CFG_ZERO_TRANSFORM).append("=").append(conf.getBoolean(CFG_ZERO_TRANSFORM, false));

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

    private String sanitiseParam(String param) {
        return param.replaceAll(",", ";");
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        AtomicLong count = new AtomicLong();
        boolean errorTerms = (maTerms.size() > 0);

        values.forEach(v -> {

            // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)

            double value = v.getValue().doubleValue();

            if (count.get() >= 1) {  // enough values to start predicting?

                // YUCK not very efficient but quick and dirty
                Double[] valueArray = makeArray(valueWindow, arTerms.size());
                Double[] errorArray = makeArray(errorWindow, maTerms.size());

                double prediction = constant +
                        arTerms.stream().mapToDouble(ar ->
                                // count - 1 so predictions start once have one previous
                                ar.apply(valueArray, count.get() - 1)).sum();
                double errors = maTerms.stream().mapToDouble(ar ->
                            // count - 1 so predictions start once have one previous
                            ar.apply(errorArray, count.get() - 1)).sum();
                // using convention ma coefficients are subtracted
                prediction -= errors;

                double error = value - prediction;
                errorTracker.addError(value, error);
                if (errorTerms) {
                    makeSpace(errorWindow, errorArray.length);
                    errorWindow.addFirst(error);
                }

                makeSpace(valueWindow, valueArray.length);

                String row = Utils.getRowName(key.getSubKey());
                Put put = new Put(Bytes.toBytes(row))
                        .addColumn(FAMILY_BYTES, ACTUAL, storeValueAsString(value))
                        .addColumn(FAMILY_BYTES, PREDICTION, storeValueAsString(prediction))
                        .addColumn(FAMILY_BYTES, ERROR, storeValueAsString(error))
                        .addColumn(FAMILY_BYTES, SQ_ERROR, storeValueAsString(Math.pow(error, 2)));

                write(context, put);
            }

            valueWindow.addFirst(value);

            count.incrementAndGet();
        });

        addModelMetrics(context, key.getMainKey(), errorTracker,
                arTerms.size() + maTerms.size() + (constant == 0 ? 0 : 1), modelParams.toString());
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

    private void makeSpace(Deque<Double> deque, int max) {
        int count = deque.size();
        if ((count > 0) && (count == max)) {
            deque.removeLast();
        }
    }

    private Double[] makeArray(Deque<Double> deque, int size) {
        Double[] array = deque.toArray(new Double[size]);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null) {
                array[i] = 0.0;
            }
        }
        return array;
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

        @Override
        public String toString() {
            return "Term{" +
                    "coefficient=" + coefficient +
                    ", index=" + index +
                    ", tag='" + tag + '\'' +
                    '}';
        }
    }
}
