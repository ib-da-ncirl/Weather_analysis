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

import ie.ibuttimer.weather.Constants;
import ie.ibuttimer.weather.WeatherAnalysis;
import ie.ibuttimer.weather.misc.JobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static ie.ibuttimer.weather.Constants.*;

public class SMA_Driver {

    private WeatherAnalysis app;

    public SMA_Driver(WeatherAnalysis app) {
        this.app = app;
    }

    public static SMA_Driver of(WeatherAnalysis app) {
        return new SMA_Driver(app);
    }

    public int runJob(JobConfig jobCfg) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = HBaseConfiguration.create();
        jobCfg.getProperties()
                .forEach((k, v) -> {
                    config.set((String)k, (String)v);
                });
        Job job = Job.getInstance(config, "SMA");
        job.setJarByClass(getClass());     // class that contains mapper and reducer

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                Constants.WEATHER_TABLE,        // input table
                scan,               // Scan instance to control CF and attribute selection
                SMA_Mapper.class,     // mapper class
                CompositeKey.class,         // mapper output key
                TimeSeriesData.class,  // mapper output value
                job);
        job.setReducerClass(SMA_Reducer.class);    // reducer class

        job.setPartitionerClass(SMA_Partitioner.class);
        job.setGroupingComparatorClass(CompositeKeyGrouping.class); // comparator that controls which keys are grouped together for a single call to Reducer
        job.setSortComparatorClass(CompositeKeyComparator.class);   // comparator that controls how the keys are sorted before they are passed to the Reducer

        job.setNumReduceTasks(2);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/sma"));

        int resultCode;
        if (jobCfg.isWait()) {
            resultCode = job.waitForCompletion(jobCfg.isVerbose()) ? STATUS_SUCCESS : STATUS_FAIL;
        } else {
            job.submit();
            resultCode = STATUS_RUNNING;
        }
        return resultCode;
    }

}
