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

import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.IDriver;
import ie.ibuttimer.weather.misc.JobConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.weather.Constants.*;

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

    protected Scan initScan(JobConfig jobCfg) {
        return new Scan()
                .setCaching(jobCfg.getProperty(CFG_SCAN_CACHING, DFLT_SCAN_CACHING))
                .setCacheBlocks(false);  // don't set to true for MR jobs
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

}
