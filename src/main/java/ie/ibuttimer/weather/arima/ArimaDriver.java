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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import ie.ibuttimer.weather.common.*;
import ie.ibuttimer.weather.hbase.Hbase;
import ie.ibuttimer.weather.hbase.TypeMap;
import ie.ibuttimer.weather.misc.*;
import ie.ibuttimer.weather.transform.DifferencingDriver;
import ie.ibuttimer.weather.transform.TransformDriver;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static ie.ibuttimer.weather.Constants.*;

public class ArimaDriver extends AbstractDriver implements IDriver {

    protected ArimaDriver(AppLogger logger) {
        super(logger);
    }

    public static ArimaDriver of(AppLogger logger) {
        return new ArimaDriver(logger);
    }

    @Override
    public int runJob(Configuration config, JobConfig jobCfg) throws IOException, ClassNotFoundException, InterruptedException {

        Pair<Integer, Map<String, String>> properties =
                getRequiredStringProperties(jobCfg,
                        Lists.newArrayList(CFG_ARIMA_IN_TABLE, CFG_ARIMA_DIFFERENCING_TABLE, CFG_ARIMA_LAGS_TABLE,
                                CFG_ARIMA_OUT_TABLE));

        int resultCode = properties.getKey();

        if (resultCode == STATUS_SUCCESS) {

            Map<String, String> map = properties.getRight();

            String arimaD = jobCfg.getProperty(CFG_ARIMA_D);    // differencing

            List<String> lags = Arrays.asList(jobCfg.getProperty(CFG_ARIMA_P).split(","));

            String diffTable = jobCfg.getProperty(CFG_ARIMA_DIFFERENCING_TABLE);
            String lagTable = jobCfg.getProperty(CFG_ARIMA_LAGS_TABLE);

            boolean in_progress = true;
            for (int step = 0; in_progress && step < 3; ++step) {
                Hbase hbase = null;

                switch (step) {
                    case 0:
                        // perform differencing
                        // update config with differencing info for arima
                        jobCfg.setProperty(CFG_DIFFERENCING_IN_TABLE, jobCfg.getProperty(CFG_ARIMA_IN_TABLE));
                        jobCfg.setProperty(CFG_DIFFERENCING_OUT_TABLE, diffTable);
                        jobCfg.setProperty(CFG_TRANSFORM_DIFFERENCING, "step," + arimaD);

                        resultCode = DifferencingDriver.of(logger).runJob(config, jobCfg);
                        in_progress = (resultCode == STATUS_SUCCESS);

                        // dewpt_3904_step_0 etc.
                        break;

                    case 1:
                        // perform lagging
                        AtomicReference<String> diffColumn = new AtomicReference<>();
                        try {
                            hbase = hbaseConnection(jobCfg);

                            // add stats
                            HashBasedTable<String, String, Value> stats = loadStats(hbase, jobCfg, diffTable,
                                    STATS_ROW_MARK_REGEX, Arrays.asList(MEAN, VARIANCE));
                            addStatsToConfig(stats, config);

                            // identify target column
                            stats.rowKeySet().stream()
                                    // filter for 'xxxx_1234_step_D' where D is the last differencing step
                                    .filter(r -> r.matches(".*_"+arimaD+"$"))
                                    .findFirst()
                                    .ifPresent(r -> diffColumn.set(r.substring(STATS_ROW_MARK.length())));
                        } finally {
                            if (hbase != null) {
                                hbase.closeConnection();
                            }
                        }

                        // update config with lag info for arima
                        jobCfg.setProperty(CFG_COLUMN_LIST, diffColumn.get());
                        jobCfg.setProperty(CFG_KEY_TYPE_MAP, TypeMap.encode(DataTypes.STRING, diffColumn.get()));

                        jobCfg.setProperty(CFG_TRANSFORM_LAG, Integer.toString(lags.size()));
                        jobCfg.setProperty(CFG_TRANSFORM_IN_TABLE, diffTable);
                        jobCfg.setProperty(CFG_TRANSFORM_OUT_TABLE, lagTable);

                        resultCode = TransformDriver.of(logger).setAddStats(false)
                                .runJob(config, jobCfg);
                        in_progress = (resultCode == STATUS_SUCCESS);

                        // dewpt_3904_step_1_lag_1 etc.
                        break;

                    case 2:
                        // perform arima
                        AtomicReference<String> arimaColumn = new AtomicReference<>();

                        // create output table if necessary
                        String outTable = map.get(CFG_ARIMA_OUT_TABLE);
                        try {
                            hbase = createTable(jobCfg, outTable);

                            // load stats
                            HashBasedTable<String, String, Value> stats = loadStats(hbase, jobCfg, lagTable,
                                    STATS_ROW_MARK_REGEX, Arrays.asList(AUTOCOVARIANCE, AUTOCORRELATION));

                            // identify target column
                            stats.rowKeySet().stream()
                                    // filter for 'xxxx_1234_step_D_lag_P' where D is the last differencing step & P is the last lag
                                    // just take 0 lag rest isn't required
                                    .filter(r -> r.matches(".*_"+arimaD+"_lag_0$"))
                                    .findFirst()
                                    .ifPresent(r -> arimaColumn.set(r.substring(STATS_ROW_MARK.length())));
                        } finally {
                            if (hbase != null) {
                                hbase.closeConnection();
                            }
                        }

                        // update config with lag info for arima
                        jobCfg.setProperty(CFG_COLUMN_LIST, arimaColumn.get());
                        jobCfg.setProperty(CFG_KEY_TYPE_MAP, TypeMap.encode(DataTypes.STRING, arimaColumn.get()));

                        Job job = initJob(config, jobCfg, "ARIMA");

                        TableMapReduceUtil.initTableMapperJob(
                                lagTable,             // input table
                                initScan(jobCfg),     // Scan instance to control CF and attribute selection
                                CKTSMapper.class,     // mapper class
                                CompositeKey.class,   // mapper output key
                                TimeSeriesData.class, // mapper output value
                                job);

                        TableMapReduceUtil.initTableReducerJob(
                                outTable,   // output table
                                ArimaTableReducer.class,   // reducer class
                                job);

                        resultCode = startJob(job, jobCfg);
                        break;
                }

            }

        }
        return resultCode;
    }



}
