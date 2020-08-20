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

import com.google.common.collect.Lists;
import ie.ibuttimer.weather.common.*;
import ie.ibuttimer.weather.hbase.Hbase;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.IDriver;
import ie.ibuttimer.weather.misc.JobConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Map;

import static ie.ibuttimer.weather.Constants.*;

public class DifferencingDriver extends AbstractDriver implements IDriver {

    protected DifferencingDriver(AppLogger logger) {
        super(logger);
    }

    public static DifferencingDriver of(AppLogger logger) {
        return new DifferencingDriver(logger);
    }

    @Override
    public int runJob(Configuration config, JobConfig jobCfg) throws IOException, ClassNotFoundException, InterruptedException {

        Pair<Integer, Map<String, String>> properties =
                getRequiredStringProperties(jobCfg,
                        Lists.newArrayList(CFG_DIFFERENCING_IN_TABLE, CFG_DIFFERENCING_OUT_TABLE));

        int resultCode = properties.getKey();

        if (resultCode == STATUS_SUCCESS) {

            Map<String, String> map = properties.getRight();

            // create output table if necessary
            String outputTable = map.get(CFG_DIFFERENCING_OUT_TABLE);
            Hbase hbase = null;
            try {
                hbase = createTable(jobCfg, outputTable);
            } finally {
                if (hbase != null) {
                    hbase.closeConnection();
                }
            }

            Job job = initJob(config, jobCfg, "Differencing");

            String inputTable = map.get(CFG_DIFFERENCING_IN_TABLE);
            TableMapReduceUtil.initTableMapperJob(
                    inputTable,         // input table
                    initScan(jobCfg),     // Scan instance to control CF and attribute selection
                    CKTSMapper.class,     // mapper class
                    CompositeKey.class,   // mapper output key
                    TimeSeriesData.class, // mapper output value
                    job);

            TableMapReduceUtil.initTableReducerJob(
                    outputTable,   // output table
                    DifferencingTableReducer.class,   // reducer class
                    job);

            resultCode = startJob(job, jobCfg);
        }
        return resultCode;
    }

}
