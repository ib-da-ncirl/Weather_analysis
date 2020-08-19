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

import com.google.common.collect.HashBasedTable;
import ie.ibuttimer.weather.hbase.Hbase;
import ie.ibuttimer.weather.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.analysis.AnalysisTableReducer.MEAN;
import static ie.ibuttimer.weather.analysis.AnalysisTableReducer.VARIANCE;

public abstract class AbstractDriver implements IDriver {

    protected AppLogger logger;

    protected AbstractDriver(AppLogger logger) {
        this.logger = logger;
    }

    protected Job initJob(Configuration config, JobConfig jobCfg, String name) throws IOException {
        jobCfg.getProperties()
                .forEach((k, v) -> {
                    config.set((String)k, (String)v);
                });
        Job job = Job.getInstance(config, name);
        job.setJarByClass(getClass());     // class that contains mapper and reducer

        job.setNumReduceTasks(jobCfg.getProperty(CFG_NUM_REDUCERS, DFLT_NUM_REDUCERS));

        return job;
    }

    public enum EnableStartStop{ IGNORE, PROCESS }

    public static Scan initScan(JobConfig jobCfg, EnableStartStop enableStartStop) {

        LocalDateTime start = jobCfg.getProperty(CFG_START_DATETIME, LocalDateTime.MIN, DATETIME_FMT);
        LocalDateTime end = jobCfg.getProperty(CFG_STOP_DATETIME, LocalDateTime.MIN, DATETIME_FMT);
        Scan scan = new Scan()
                .setCaching(jobCfg.getProperty(CFG_SCAN_CACHING, DFLT_SCAN_CACHING))
                .setCacheBlocks(false);  // don't set to true for MR jobs
        if (enableStartStop == EnableStartStop.PROCESS) {
            if (start.isAfter(LocalDateTime.MIN)) {
                scan.withStartRow(Utils.getRowName(start.toEpochSecond(ZoneOffset.UTC)).getBytes());
            }
            if (end.isAfter(LocalDateTime.MIN)) {
                scan.withStopRow(Utils.getRowName(end.toEpochSecond(ZoneOffset.UTC)).getBytes());
            }
        }
        return scan;
    }

    public static Scan initScan(JobConfig jobCfg) {
        return initScan(jobCfg, EnableStartStop.PROCESS);
    }

    protected Pair<Integer, String> getRequiredStringProperty(JobConfig jobCfg, String name) {
        int resultCode = STATUS_SUCCESS;
        String property = jobCfg.getProperty(name, "");
        if (StringUtils.isEmpty(property)) {
            logger.warn(String.format("'%s' not configured", name));
            resultCode = STATUS_CONFIG_ERROR;
        }
        return Pair.of(resultCode, property);
    }

    protected Pair<Integer, Map<String, String>> getRequiredStringProperies(JobConfig jobCfg, List<String> names) {
        AtomicInteger resultCode = new AtomicInteger(STATUS_SUCCESS);
        Map<String, String> map = Maps.newHashMap();
        names.forEach(n -> {
            Pair<Integer, String> result = getRequiredStringProperty(jobCfg, n);
            if (result.getLeft() != STATUS_SUCCESS) {
                resultCode.set(result.getLeft());
            }
            map.put(n, result.getRight());
        });
        return Pair.of(resultCode.get(), map);
    }

    public HashBasedTable<String, String, Value> loadStats(Hbase hbase, JobConfig jobCfg, String tableName) throws IOException {
        Map<String, DataTypes> columns = com.google.common.collect.Maps.newHashMap();
        Arrays.asList(MEAN, VARIANCE).forEach(x -> columns.put(x, DataTypes.DOUBLE));
        return hbase.read(tableName, initScan(jobCfg, EnableStartStop.IGNORE), columns);
    }

    public void addStatsToConfig(Hbase hbase, JobConfig jobCfg, String tableName, Configuration config) throws IOException {
        HashBasedTable<String, String, Value> stats = loadStats(hbase, jobCfg, tableName);

        // transform means into 'row,col,mean;row,col,mean;..'
        StringBuffer sb = new StringBuffer();
        stats.rowKeySet().forEach(row -> {
            stats.columnKeySet().forEach(col -> {
                sb.append(row).append(',')          // row, i.e. variable name
                        .append(col).append(',')    // col, i.e. 'mean', 'variance'
                        .append(stats.get(row, col).doubleValue()).append(';'); // value
            });
        });
        config.set(REDUCER_STATS, sb.toString());
    }

    public static HashBasedTable<String, String, Double> decodeStats(Configuration conf) {
        HashBasedTable<String, String, Double> stats = HashBasedTable.create();

        String meanCfg = conf.get(REDUCER_STATS, "");
        Arrays.asList(meanCfg.split(";")).forEach(entry -> {
            String[] cell = entry.split(",");
            stats.put(cell[0], cell[1], Double.parseDouble(cell[2]));
        });
        return stats;
    }

}
