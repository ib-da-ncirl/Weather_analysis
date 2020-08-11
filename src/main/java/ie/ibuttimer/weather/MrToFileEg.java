package ie.ibuttimer.weather;

import ie.ibuttimer.weather.misc.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.Constants.FAMILY_BYTES;

public class MrToFileEg {

    public MrToFileEg() {



    }



    public void job() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config,"ExampleSummary");
        job.setJarByClass(getClass());     // class that contains mapper and reducer

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                Constants.WEATHER_TABLE,        // input table
                scan,               // Scan instance to control CF and attribute selection
                MyMapper.class,     // mapper class
                Text.class,         // mapper output key
                Value.class,  // mapper output value
                job);
        job.setReducerClass(MyFileReducer.class);    // reducer class
        job.setNumReduceTasks(1);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile"));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }


    public static class MyMapper extends TableMapper<Text, Value> {
        public static final String TEMP_3904 = stationRainColumn(3904);
        public static final byte[] TEMP_ATTR = stationRainColumnBytes(3904);
        public static final String TEMP_3904_SQ = TEMP_3904 + "_SQ";
        public static final byte[] TEMP_ATTR_SQ = TEMP_3904_SQ.getBytes();

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        /*
            hbase(main):004:0> get "weather_info", "r-2020063015"
            COLUMN                               CELL
             cf:date                             timestamp=1596534897470, value=2020-06-30 15:00:00
             cf:dewpt_3904                       timestamp=1596534897470, value=13.7
             cf:ind_rain_3904                    timestamp=1596534897470, value=0
             cf:ind_temp_3904                    timestamp=1596534897470, value=0
             cf:ind_wetb_3904                    timestamp=1596534897470, value=0
             cf:msl_3904                         timestamp=1596534897470, value=1002.3
             cf:rain_3904                        timestamp=1596534897470, value=0.0
             cf:rhum_3904                        timestamp=1596534897470, value=86
             cf:temp_3904                        timestamp=1596534897470, value=16.0
             cf:vappr_3904                       timestamp=1596534897470, value=15.7
             cf:wetb_3904                        timestamp=1596534897470, value=14.7
         */

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String rowId = new String(row.get());

            text.set(TEMP_ATTR);
            String val = new String(value.getValue(FAMILY_BYTES, TEMP_ATTR));
            context.write(text, Value.of(Float.parseFloat(val)));

            text.set(TEMP_ATTR_SQ);
            context.write(text, Value.of(Math.pow(Float.parseFloat(val), 2)));
        }
    }

    public static class MyFileReducer extends Reducer<Text, Value, Text, Text> {
        public static final byte[] TEMP_COUNT = "temp_count".getBytes();
        public static final byte[] TEMP_SUM = "temp_sum".getBytes();
        public static final byte[] TEMP_AVG = "temp_avg".getBytes();

        public void reduce(Text key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(MyMapper.TEMP_3904)) {

                Value summer = Value.of(0L);
                Value minimiser = Value.of(Integer.MAX_VALUE);
                Value maximiser = Value.of(Integer.MIN_VALUE);
                Value zeroCnt = Value.of(0L);
                Value squared = Value.of(0L);
                AtomicLong count = new AtomicLong();

                values.forEach( value -> {
                    summer.add(value);
                    maximiser.max(value);
                    minimiser.min(value);
                    value.asDouble(d -> {
                        if (d == 0.0) {
                            zeroCnt.add(1);
                        }
                    });
                    count.incrementAndGet();
                });

                context.write(key, new Text("Sum: " + summer.longValue()));
                context.write(key, new Text("Count: " + count.get()));
                context.write(key, new Text("Avg: " + summer.doubleValue() / count.get()));

            } else if (key.toString().equals(MyMapper.TEMP_3904_SQ)) {

                Value squared = Value.of(0L);
                AtomicLong count = new AtomicLong();

                values.forEach( value -> {
                    squared.add(value);
                    count.incrementAndGet();
                });

                context.write(key, new Text("Sum of squares: " + squared.longValue()));
                context.write(key, new Text("Square count: " + count.get()));
            }
        }
    }

}
