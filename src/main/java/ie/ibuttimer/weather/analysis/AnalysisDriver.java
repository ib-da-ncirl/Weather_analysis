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

import com.google.common.collect.Lists;
import ie.ibuttimer.weather.common.AbstractDriver;
import ie.ibuttimer.weather.common.CKTSMapper;
import ie.ibuttimer.weather.common.CompositeKey;
import ie.ibuttimer.weather.common.TimeSeriesData;
import ie.ibuttimer.weather.hbase.Hbase;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.IDriver;
import ie.ibuttimer.weather.misc.JobConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.analysis.AnalysisTableReducer.columnNameBytes;

public class AnalysisDriver extends AbstractDriver implements IDriver {

    protected AnalysisDriver(AppLogger logger) {
        super(logger);
    }

    public static AnalysisDriver of(AppLogger logger) {
        return new AnalysisDriver(logger);
    }

    @Override
    public int runJob(Configuration config, JobConfig jobCfg) throws IOException, ClassNotFoundException, InterruptedException {

        Pair<Integer, Map<String, String>> properties =
                getRequiredStringProperties(jobCfg, Lists.newArrayList(CFG_ANALYSIS_IN_TABLE, CFG_ANALYSIS_OUT_TABLE));

        int resultCode = properties.getKey();

        if (resultCode == STATUS_SUCCESS) {

            Map<String, String> map = properties.getRight();

            Job job = initJob(config, jobCfg, "Analysis");

            TableMapReduceUtil.initTableMapperJob(
                    map.get(CFG_ANALYSIS_IN_TABLE), // input table
                    initScan(jobCfg),     // Scan instance to control CF and attribute selection
                    CKTSMapper.class,     // mapper class
                    CompositeKey.class,   // mapper output key
                    TimeSeriesData.class, // mapper output value
                    job);

            // create output table if necessary
            String analysisTable = map.get(CFG_ANALYSIS_OUT_TABLE);
            Hbase hbase = null;
            try {
                hbase = createTable(jobCfg, analysisTable);

                addStatsToConfig(hbase, jobCfg, analysisTable, config, Arrays.asList(MEAN, VARIANCE));

            } finally {
                if (hbase != null) {
                    hbase.closeConnection();
                }
            }

            TableMapReduceUtil.initTableReducerJob(
                    analysisTable,   // output table
                    AnalysisTableReducer.class,   // reducer class
                    job);

            resultCode = startJob(job, jobCfg);

            if (resultCode == STATUS_SUCCESS) {
                saveResults(jobCfg, analysisTable);
            }
        }
        return resultCode;
    }

    public static void saveResults(JobConfig jobCfg, String table) throws IOException {

        int numStrata = jobCfg.getProperty(CFG_NUM_STRATA, DFLT_NUM_STRATA);

        List<String> statColumns = Lists.newArrayList(COUNT, MIN, MAX, MEAN, VARIANCE, STD_DEV, MIN_TS, MAX_TS);
        for (int i = 0; i < numStrata; ++i) {
            int finalI = i;
            Arrays.asList(COUNT, MIN, MAX, MEAN, VARIANCE, STD_DEV, MIN_TS, MAX_TS).forEach(s -> {
                statColumns.add(new String(columnNameBytes(s, finalI)));
            });
        }

        saveDriverResults(jobCfg, table, statColumns, jobCfg.getProperty(CFG_ANALYSIS_PATH_ROOT, ""));
    }
}
