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

package ie.ibuttimer.weather;

import ie.ibuttimer.weather.misc.JobConfig;
import ie.ibuttimer.weather.misc.Utils;
import ie.ibuttimer.weather.misc.Value;
import ie.ibuttimer.weather.sma.SMA_Driver;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static ie.ibuttimer.weather.Constants.*;

public class WeatherAnalysis {

    Logger logger = Logger.getLogger("WeatherAnalysis");

    private TableName table1 = TableName.valueOf("Table1");

    public void test() throws IOException, ServiceException {


        Configuration config = HBaseConfiguration.create();


        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        HBaseAdmin.available(config);
//        HBaseAdmin.checkHBaseAvailable(config);


        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

//        HTableDescriptor desc = new HTableDescriptor(table1);
//        desc.addFamily(new HColumnDescriptor(family1));
//        admin.createTable(desc);

        admin.createTable(
            TableDescriptorBuilder.newBuilder(table1)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Constants.FAMILY.getBytes()).build())
                    .build()
        );


        connection.close();
    }
    public void tables() throws IOException, ServiceException {


        Configuration config = HBaseConfiguration.create();


        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        // Getting all the list of tables using HBaseAdmin object
//        HTableDescriptor[] tableDescriptor = admin.listTables();
        List<TableDescriptor> tableDescriptor = admin.listTableDescriptors();

        // printing all the table names.
//        Table1 [[B@172b013]
//        weather_info [[B@56673b2c]
        for (TableDescriptor table : tableDescriptor){
            System.out.println("" + table.getTableName() + " " + table.getColumnFamilyNames());
        }
        connection.close();
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
        TableMapReduceUtil.initTableReducerJob(
                table1.getNameAsString(),        // output table
                MyTableReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(1);   // at least one, adjust as required

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }


    public static class MyMapper extends TableMapper<Text, Value> {
        public static final byte[] CF = "cf".getBytes();
        public static final String TEMP_3904 = stationRainColumn(3904);
        public static final byte[] TEMP_ATTR = stationRainColumnBytes(3904);

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
            String val = new String(value.getValue(CF, TEMP_ATTR));
            context.write(text, Value.of(Float.parseFloat(val)));
        }
    }

    public static class MyTableReducer extends TableReducer<Text, Value, ImmutableBytesWritable> {
        public static final byte[] TEMP_COUNT = "temp_count".getBytes();
        public static final byte[] TEMP_SUM = "temp_sum".getBytes();
        public static final byte[] TEMP_AVG = "temp_avg".getBytes();

        public void reduce(Text key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            if (key.toString().equals(MyMapper.TEMP_3904)) {
                int i = 0;
                double sum = 0;
                for (Value val : values) {
                    ++i;
                    sum += val.floatValue();
                }
                put.addColumn(FAMILY_BYTES, TEMP_SUM, Bytes.toBytes(sum));
                put.addColumn(FAMILY_BYTES, TEMP_COUNT, Bytes.toBytes(i));
                put.addColumn(FAMILY_BYTES, TEMP_AVG, Bytes.toBytes(sum/i));
            }
            context.write(null, put);
        }
    }


    private static final String OPT_HELP = "h";
    private static final String OPT_CFG = "c";
    private static final String OPT_JOB = "j";
    private static final String OPT_WAIT = "w";
    private static final String OPT_NO_WAIT = "nw";
    private static final String OPT_LIST_JOBS = "l";
    private static final String OPT_MULTI_JOB = "m";
    private static final String OPT_IN_ROOT = "i";
    private static final String OPT_OUT_ROOT = "o";
    private static final Options options;

    static {
        options = new Options();
        options.addOption(OPT_HELP, false, "print this message");
        options.addOption(OPT_CFG, true, "configuration file(s), multiple files separated by '" +
                MULTIPLE_CFG_FILE_SEP + "'");
        options.addOption(OPT_JOB, true, "name of job to run");
        options.addOption(OPT_WAIT, false, "wait for job completion, [default]");
        options.addOption(OPT_NO_WAIT, false, "do not wait for job completion");
        options.addOption(OPT_LIST_JOBS, false, "list available jobs");
        options.addOption(OPT_MULTI_JOB, true, "process multiple jobs as per specified file");
        options.addOption(OPT_IN_ROOT, true, "input root folder");
        options.addOption(OPT_OUT_ROOT, true, "output root folder");
    }

   /* sample argument lists
        -j sma -c prod.properties;config.properties
//        -m <path to file>
     */

    private static final String JOB_SMA = "sma";
    private static final List<Triple<String, String, String>> jobList;
    private static final String jobListFmt;
    static {
        jobList = new ArrayList<>();
        jobList.add(Triple.of(JOB_SMA, "perform Simple Moving Average", "SMA Job"));

        OptionalInt width = jobList.stream().map(Triple::getLeft).mapToInt(String::length).max();
        StringBuffer sb = new StringBuffer("  %");
        width.ifPresent(w -> sb.append("-").append(w));
        sb.append("s : %s%n");
        jobListFmt = sb.toString();
    }

    private String inPathRoot = "";
    private String outPathRoot = "";

    public static void main(String[] args) {

        WeatherAnalysis app = new WeatherAnalysis();

        CommandLineParser parser = new BasicParser();
        int resultCode = STATUS_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_IN_ROOT)) {
                app.inPathRoot = cmd.getOptionValue(OPT_IN_ROOT);
            }
            if (cmd.hasOption(OPT_OUT_ROOT)) {
                app.outPathRoot = cmd.getOptionValue(OPT_OUT_ROOT);
            }

            if (cmd.hasOption(OPT_MULTI_JOB)) {
                String jobFile = cmd.getOptionValue(OPT_MULTI_JOB);
                if (StringUtils.isEmpty(jobFile)) {
                    resultCode = STATUS_CONFIG_ERROR;
                    System.out.format("No job file specified");
                    app.help();
                }

                File file = FileUtils.getFile(jobFile);
                List<String> contents = FileUtils.readLines(file, StandardCharsets.UTF_8);

                for (String jobSpec : contents) {
                    if (!jobSpec.trim().startsWith(COMMENT_PREFIX)) {
                        resultCode = app.processJob(jobSpec.split(" "));
                        if (resultCode != STATUS_SUCCESS) {
                            break;
                        }
                    }
                }
            } else {
                resultCode = app.processJob(args);
            }
        } catch (ParseException pe) {
            System.out.format("%s%n%n", pe.getMessage());
            app.help();
            resultCode = STATUS_FAIL;
        } catch (Exception e) {
            System.out.format("%s%n%n", e.getMessage());
            resultCode = STATUS_FAIL;
        }

        System.exit(resultCode);
    }


    private int processJob(String[] args) throws Exception {

        CommandLineParser parser = new BasicParser();
        int resultCode = STATUS_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_HELP)) {
                // print help
                help();
            } else if (cmd.hasOption(OPT_LIST_JOBS)) {
                // print job list
                jobList();
            } else {
                String resourceFile;
                if (cmd.hasOption(OPT_CFG)) {
                    // config file
                    resourceFile = cmd.getOptionValue(OPT_CFG);
                } else {
                    resourceFile = DFLT_CFG_FILE;
                }

                if (cmd.hasOption(OPT_JOB)) {
                    // read the config
                    Properties properties = getResources(resourceFile);

                    if (!StringUtils.isEmpty(inPathRoot)) {
                        properties.setProperty(CFG_IN_PATH_ROOT, inPathRoot);
                    } else {
                        inPathRoot = properties.getProperty(CFG_IN_PATH_ROOT);
                    }
                    if (!StringUtils.isEmpty(outPathRoot)) {
                        properties.setProperty(CFG_OUT_PATH_ROOT, outPathRoot);
                    } else {
                        outPathRoot = properties.getProperty(CFG_OUT_PATH_ROOT);
                    }

                    if (properties.isEmpty()) {
                        resultCode = STATUS_CONFIG_ERROR;
                        System.out.format("No configuration specified, properties empty%n%n");
                        help();
                    } else {
                        // run the job
                        JobConfig jobCfg = JobConfig.of(properties,
                                (!cmd.hasOption(OPT_NO_WAIT)), inPathRoot, outPathRoot);

                        logger.info(
                                String.format("Run mode: %s",
                                        jobCfg.getProperties().getProperty(CFG_MODE, "prod")));

                        if (Boolean.parseBoolean(jobCfg.getProperties().getProperty(CFG_CLR_LAST_RESULT, "false"))) {
                            File outPath = new File(jobCfg.getOutPathRoot());
                            logger.info(String.format("Deleting: %s", outPath));
                            FileUtils.deleteQuietly(outPath);
                        }

                        String name = cmd.getOptionValue(OPT_JOB);
                        jobList.stream()
                                .filter(e -> e.getLeft().equals(name))
                                .findFirst()
                                .ifPresent(t -> logger.info(Utils.getDialog("Running " + t.getRight())));



                        switch (name) {
                            case JOB_SMA:
                                resultCode = SMA_Driver.of(this).runJob(jobCfg);
                                break;
                            default:
                                System.out.format("Unknown job: %s%n%n", cmd.getOptionValue(OPT_JOB));
                                jobList();
                                resultCode = STATUS_CONFIG_ERROR;
                        }
                    }
                } else {
                    System.out.format("No arguments specified%n%n");
                    help();
                }
            }
        } catch (ParseException pe) {
            System.out.format("%s%n%n", pe.getMessage());
            help();
            resultCode = STATUS_FAIL;
        }

        return resultCode;
    }

    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WeatherAnalysis", options);
    }

    private void jobList() {
        System.out.println("Job List");
        jobList.forEach(job -> System.out.format(jobListFmt, job.getLeft(), job.getRight()));
    }

    /**
     * Load resources from the specified file(s). Multiple files are separated by ':'.
     * @param filename  Resource filename(s)
     * @return Properties
     */
    private Properties getResources(String filename) {
        Properties properties = new Properties();

        Arrays.asList(filename.split(MULTIPLE_CFG_FILE_SEP)).forEach(name -> {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(name)) {
                if (input != null) {
                    properties.load(input);
                } else {
                    System.out.println("Unable to load " + name);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        return properties;
    }

}
