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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.arima.ArimaDriver.saveResults;
import static ie.ibuttimer.weather.misc.Utils.rangeSpec;

public class SmaDriver extends AbstractDriver implements IDriver {

    protected SmaDriver(AppLogger logger) {
        super(logger);
    }

    public static SmaDriver of(AppLogger logger) {
        return new SmaDriver(logger);
    }

    @Override
    public int runJob(Configuration config, JobConfig jobCfg) throws IOException, ClassNotFoundException, InterruptedException {

        Pair<Integer, String> properties = getRequiredStringProperty(jobCfg, CFG_SMA_IN_TABLE);

        int resultCode = properties.getLeft();
        String outTable = jobCfg.getProperty(CFG_SMA_REDUCE_TABLE, DFLT_SMA_REDUCE_TABLE);

        if (resultCode == STATUS_SUCCESS) {

            for (int size :
                    rangeSpec(jobCfg.getProperty(CFG_MA_WINDOW_SIZE, Integer.toString(DFLT_MA_WINDOW_SIZE)))) {

                jobCfg.setProperty(CFG_MA_WINDOW_SIZE, size);

                Job job = initJob(config, jobCfg, "SMA");


                TableMapReduceUtil.initTableMapperJob(
                        properties.getRight(), // input table
                        initScan(jobCfg),     // Scan instance to control CF and attribute selection
                        CKTSMapper.class,     // mapper class
                        CompositeKey.class,   // mapper output key
                        TimeSeriesData.class, // mapper output value
                        job);
                String reduceMode = jobCfg.getProperty(CFG_SMA_REDUCE_MODE, DFLT_SMA_REDUCE_MODE);

                if (reduceMode.equalsIgnoreCase(SMA_FILE_REDUCE_MODE)) {
                    job.setReducerClass(SmaFileReducer.class);    // reducer class

                    FileOutputFormat.setOutputPath(job, new Path(jobCfg.getProperty(CFG_OUT_PATH_ROOT)));

                } else if (reduceMode.equalsIgnoreCase(SMA_TABLE_REDUCE_MODE)) {

                    Hbase hbase = null;
                    try {
                        hbase = deleteTables(jobCfg, Collections.singletonList(outTable));
                        hbase = createTable(jobCfg, outTable);
                    } finally {
                        if (hbase != null) {
                            hbase.closeConnection();
                        }
                    }

                    TableMapReduceUtil.initTableReducerJob(
                            outTable,                // output table
                            SmaTableReducer.class,   // reducer class
                            job);

                } else {
                    resultCode = STATUS_CONFIG_ERROR;
                }

                if (resultCode == STATUS_SUCCESS) {
                    resultCode = startJob(job, jobCfg);

                    if (resultCode == STATUS_SUCCESS) {
                        saveResults(jobCfg, outTable, jobCfg.getProperty(CFG_SMA_PATH_ROOT, ""), logger);
                    }
                }
                if (resultCode != STATUS_SUCCESS) {
                    break;
                }
            }
        }

        return resultCode;
    }
}
