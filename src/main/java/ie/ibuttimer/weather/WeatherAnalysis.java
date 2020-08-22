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

import com.google.common.collect.Lists;
import ie.ibuttimer.weather.analysis.AnalysisDriver;
import ie.ibuttimer.weather.arima.ArimaDriver;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.JobConfig;
import ie.ibuttimer.weather.misc.Utils;
import ie.ibuttimer.weather.sma.SmaDriver;
import ie.ibuttimer.weather.transform.DifferencingDriver;
import ie.ibuttimer.weather.transform.TransformDriver;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.misc.Utils.expandPath;

public class WeatherAnalysis extends Configured implements Tool {

    AppLogger logger = AppLogger.of(Logger.getLogger("WeatherAnalysis"));

    private static final String OPT_HELP = "h";
    private static final String OPT_ARG_FILE = "a";
    private static final String OPT_CFG = "c";
    private static final String OPT_JOB = "j";
    private static final String OPT_VERBOSE = "v";
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
        options.addOption(OPT_ARG_FILE, true, "file to read arguments from");
        options.addOption(OPT_CFG, true, "configuration file(s), multiple files separated by '" +
                MULTIPLE_CFG_FILE_SEP + "'");
        options.addOption(OPT_JOB, true, "name of job to run, or list separated by '" +
                MULTIPLE_CFG_FILE_SEP + "'");
        options.addOption(OPT_VERBOSE, false, "verbose mode");
        options.addOption(OPT_WAIT, false, "wait for job completion, [default]");
        options.addOption(OPT_NO_WAIT, false, "do not wait for job completion");
        options.addOption(OPT_LIST_JOBS, false, "list available jobs");
        options.addOption(OPT_MULTI_JOB, true, "process multiple jobs as per specified file");
        options.addOption(OPT_IN_ROOT, true, "input root folder");
        options.addOption(OPT_OUT_ROOT, true, "output root folder");
    }

   /* sample argument lists
        -j analysis -c prod.properties;config.properties
        -j sma -c prod.properties;config.properties
//        -m <path to file>
     */

    private static final String JOB_ANALYSIS = "analysis";
    private static final String JOB_TRANSFORM = "transform";
    private static final String JOB_DIFFERENCING = "difference";
    private static final String JOB_SMA = "sma";
    private static final String JOB_ARIMA = "arima";
    private static final List<Triple<String, String, String>> jobList;
    private static final String jobListFmt;
    static {
        jobList = new ArrayList<>();
        jobList.add(Triple.of(JOB_ANALYSIS, "perform Analysis", "Analysis Job"));
        jobList.add(Triple.of(JOB_TRANSFORM, "perform Transformation", "Transform Job"));
        jobList.add(Triple.of(JOB_DIFFERENCING, "perform Differencing", "Differencing Job"));
        jobList.add(Triple.of(JOB_SMA, "perform Simple Moving Average", "SMA Job"));
        jobList.add(Triple.of(JOB_ARIMA, "perform ARIMA", "ARIMA Job"));

        OptionalInt width = jobList.stream().map(Triple::getLeft).mapToInt(String::length).max();
        StringBuffer sb = new StringBuffer("  %");
        width.ifPresent(w -> sb.append("-").append(w));
        sb.append("s : %s%n");
        jobListFmt = sb.toString();
    }

    private String inPathRoot = "";
    private String outPathRoot = "";


    @Override
    public int run(String[] args) throws Exception {

        CommandLineParser parser = new BasicParser();
        int resultCode = STATUS_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_ARG_FILE)) {

                logger.logger().info("Configuration: Argument file takeover!");

                File file = FileUtils.getFile(cmd.getOptionValue(OPT_ARG_FILE));
                List<String> contents = FileUtils.readLines(file, StandardCharsets.UTF_8);
                if (contents.size() > 0) {
                    ArrayList<String> newArgs = Lists.newArrayList();
                    for (String line : contents) {
                        if (!line.startsWith(COMMENT_PREFIX)) {
                            newArgs.addAll(Arrays.asList(line.split(" ")));
                        }
                    }
                    args = newArgs.toArray(new String[newArgs.size()]);
                }
                cmd = parser.parse(options, args);
            }

            if (cmd.hasOption(OPT_IN_ROOT)) {
                inPathRoot = cmd.getOptionValue(OPT_IN_ROOT);
            }
            if (cmd.hasOption(OPT_OUT_ROOT)) {
                outPathRoot = cmd.getOptionValue(OPT_OUT_ROOT);
            }

            if (cmd.hasOption(OPT_MULTI_JOB)) {
                String jobFile = cmd.getOptionValue(OPT_MULTI_JOB);
                if (StringUtils.isEmpty(jobFile)) {
                    resultCode = STATUS_CONFIG_ERROR;
                    logger.warn("No job file specified");
                    help();
                }
                File file = FileUtils.getFile(jobFile);
                List<String> contents = FileUtils.readLines(file, StandardCharsets.UTF_8);

                for (String jobSpec : contents) {
                    if (!jobSpec.trim().startsWith(COMMENT_PREFIX)) {
                        resultCode = processJob(jobSpec.split(" "));
                        if (resultCode != STATUS_SUCCESS) {
                            break;
                        }
                    }
                }
            } else {
                resultCode = processJob(args);
            }
        } catch (ParseException pe) {
            logger.warn(String.format("%s%n%n", pe.getMessage()));
            help();
            resultCode = STATUS_FAIL;
        } catch (Exception e) {
            logger.error(String.format("%s%n%n", e.getMessage()), e);
            resultCode = STATUS_FAIL;
        }

        return resultCode;
    }


    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new WeatherAnalysis(), args);
        System.exit(res);
    }


    private int processJob(String[] args) throws Exception {

        Configuration conf = this.getConf();

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
                        properties.setProperty(CFG_IN_PATH_ROOT, expandPath(inPathRoot));
                    } else {
                        inPathRoot = expandPath(properties.getProperty(CFG_IN_PATH_ROOT));
                    }
                    if (!StringUtils.isEmpty(outPathRoot)) {
                        properties.setProperty(CFG_OUT_PATH_ROOT, expandPath(outPathRoot));
                    } else {
                        outPathRoot = expandPath(properties.getProperty(CFG_OUT_PATH_ROOT));
                    }

                    if (properties.isEmpty()) {
                        resultCode = STATUS_CONFIG_ERROR;
                        logger.warn(String.format("No configuration specified, properties empty%n%n"));
                        help();
                    } else {
                        // run the job
                        JobConfig jobCfg = JobConfig.of(properties,
                                (!cmd.hasOption(OPT_NO_WAIT)), cmd.hasOption(OPT_VERBOSE), inPathRoot, outPathRoot);

                        logger.logger().info(
                                String.format("Run mode: %s",
                                        jobCfg.getProperties().getProperty(CFG_MODE, "prod")));
                        if (jobCfg.isVerbose()) {
                            logger.logger().info("Configuration: " + jobCfg);
                        }

                        if (Boolean.parseBoolean(jobCfg.getProperties().getProperty(CFG_CLR_LAST_RESULT, "false"))) {
                            File outPath = new File(jobCfg.getOutPathRoot());
                            logger.logger().info(String.format("Deleting: %s", outPath));
                            FileUtils.deleteQuietly(outPath);
                        }

                        String name = cmd.getOptionValue(OPT_JOB);
                        jobList.stream()
                                .filter(e -> e.getLeft().equals(name))
                                .findFirst()
                                .ifPresent(t -> logger.logger().info(Utils.getDialog("Running " + t.getRight())));


                        Configuration config = HBaseConfiguration.create(getConf());

                        switch (name) {
                            case JOB_ANALYSIS:
                                resultCode = AnalysisDriver.of(logger).runJob(config, jobCfg);
                                break;
                            case JOB_TRANSFORM:
                                resultCode = TransformDriver.of(logger).runJob(config, jobCfg);
                                break;
                            case JOB_DIFFERENCING:
                                resultCode = DifferencingDriver.of(logger).runJob(config, jobCfg);
                                break;
                            case JOB_SMA:
                                resultCode = SmaDriver.of(logger).runJob(config, jobCfg);
                                break;
                            case JOB_ARIMA:
                                resultCode = ArimaDriver.of(logger).runJob(config, jobCfg);
                                break;
                            default:
                                logger.warn(String.format("Unknown job: %s%n%n", cmd.getOptionValue(OPT_JOB)));
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
            logger.warn(String.format("%s%n%n", pe.getMessage()));
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
