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

package ie.ibuttimer.weather.misc;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ie.ibuttimer.weather.Constants.CFG_TRANSFORM_LAG;
import static ie.ibuttimer.weather.Constants.YYYYMMDDHH_FMT;

/**
 * Miscellaneous utilities
 */
public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);

    private Utils() {
        // class can't be externally instantiated
    }

    public static Logger getLogger() {
        return logger;
    }

    public static int getJREVersion() {
        String version = System.getProperty("java.version");
        if(version.startsWith("1.")) {
            // Java 8 or lower has 1.x.y version numbers
            version = version.substring(2, 3);
        } else {
            // Java 9 or higher has x.y.z version numbers
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        return Integer.parseInt(version);
    }

    @SuppressWarnings("deprecation")
    public static <T> Object getInstance(Class<T> cls) {
        Object instance = null;
        if (getJREVersion() <= 8) {
            try {
                instance = cls.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                getLogger().warn("Unable to instantiate instance of " + cls.getName(), e);
            }
        } else {
            try {
                cls.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                getLogger().warn("Unable to instantiate instance of " + cls.getName(), e);
            }
        }
        return instance;
    }


    public static <T> List<T> iterableToList(Iterable<T> values) {
        // TODO check this, not sure it adds everything to the list
        return StreamSupport.stream(values.spliterator(), false)
            .collect(Collectors.toList());
    }

    public static <K, V, T> List<T> iterableOfMapsToList(Iterable<? extends Map<K, V>> values, Class<T> cls) {
        return streamOfMapsToList(StreamSupport.stream(values.spliterator(), false), cls);
    }

    public static <K, V, T> List<T> listOfMapsToList(List<? extends Map<K, V>> values, Class<T> cls) {
        return streamOfMapsToList(values.stream(), cls);
    }

    public static <K, V, T> List<T> streamOfMapsToList(Stream<? extends Map<K, V>> values, Class<T> cls) {
        return values
            .flatMap(m -> m.entrySet().stream())
            .map(Map.Entry::getValue)
            .filter(cls::isInstance)
            .map(cls::cast)
            .collect(Collectors.toList());
    }


    /**
     * Get the zoned date and time
     * @param dateTime  String of the date and time
     * @param formatter Formatter to use
     * @return  Converted date and time
     */
    public static ZonedDateTime getZonedDateTime(String dateTime, DateTimeFormatter formatter, Logger logger) {
        ZonedDateTime zdt = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.systemDefault());
        try {
            zdt = ZonedDateTime.parse(dateTime, formatter);
        } catch (DateTimeParseException dpte) {
            logger.error("Cannot parse '" + dateTime + "' using format " + formatter.toString(), dpte);
        }
        return zdt;
    }

    /**
     * Get the zoned date and time
     * @param dateTime  String of the date and time
     * @param formatter Formatter to use
     * @return  Converted date and time
     */
    public static ZonedDateTime getZonedDateTime(String dateTime, DateTimeFormatter formatter) {
        return getZonedDateTime(dateTime, formatter, logger);
    }

    /**
     * Get the date and time
     * @param dateTime  String of the date and time
     * @param formatter Formatter to use
     * @return  Converted date and time
     */
    public static LocalDateTime getDateTime(String dateTime, DateTimeFormatter formatter, Logger logger) {
        LocalDateTime ldt = LocalDateTime.MIN;
        try {
            ldt = LocalDateTime.parse(dateTime, formatter);
        } catch (DateTimeParseException dpte) {
            logger.error("Cannot parse '" + dateTime + "' using format " + formatter.toString(), dpte);
        }
        return ldt;
    }

    /**
     * Get the date and time
     * @param dateTime  String of the date and time
     * @param formatter Formatter to use
     * @return  Converted date and time
     */
    public static LocalDateTime getDateTime(String dateTime, DateTimeFormatter formatter) {
        return getDateTime(dateTime, formatter, logger);
    }

    /**
     * Get the date
     * @param date      String of the date
     * @param formatter Formatter to use
     * @return  Converted date
     */
    public static LocalDate getDate(String date, DateTimeFormatter formatter, Logger logger) {
        LocalDate ld = LocalDate.MIN;
        try {
            ld = LocalDate.parse(date, formatter);
        } catch (DateTimeParseException dpte) {
            logger.error("Cannot parse '" + date + "' using format " + formatter.toString(), dpte);
        }
        return ld;
    }

    /**
     * Get the date
     * @param date      String of the date
     * @param formatter Formatter to use
     * @return  Converted date
     */
    public static LocalDate getDate(String date, DateTimeFormatter formatter) {
        return getDate(date, formatter, logger);
    }

    /**
     * Get a banner line
     * @param length
     * @param chr
     * @return
     */
    public static String banner(int length, char chr) {
        char[] line = new char[length];
        Arrays.fill(line, chr);
        return new String(line);
    }

    /**
     * Get an underlined header
     * @param heading
     * @return
     */
    public static String heading(String heading) {
        return String.format("%s%n%s%n", heading, banner(heading.length(), '-'));
    }

    /**
     * Get a multiline text dialog with banner above and below
     * @param lines
     * @return
     */
    public static String getDialog(List<String> lines) {
        AtomicInteger maxLen = new AtomicInteger(25);
        lines.stream().mapToInt(String::length).max().ifPresent(maxLen::set);
        String banner = banner(maxLen.get(), '*');
        String ls = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder(ls);
        sb.append(banner).append(ls);
        lines.forEach(l -> sb.append(l).append(ls));
        sb.append(banner);
        return sb.toString();
    }

    /**
     * Get a text dialog with banner above and below
     * @return
     */
    public static String getDialog(String line) {
        return getDialog(Collections.singletonList(line));
    }

    /**
     * Get a multiline text dialog with space above and below
     * @param lines
     * @return
     */
    public static String getSpacedDialog(List<String> lines) {
        String ls = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder(ls).append(ls);
        lines.forEach(l -> sb.append(l).append(ls));
        return sb.toString();
    }

    /**
     * Get a text dialog with space above and below
     * @return
     */
    public static String getSpacedDialog(String line) {
        return getSpacedDialog(Collections.singletonList(line));
    }


    public static final String ROWNAME_PREFIX = "r-";

    /**
     * Generate row name from timestamp
     * @param timestamp
     * @return
     */
    public static String getRowName(long timestamp) {
        return ROWNAME_PREFIX +
                LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).format(YYYYMMDDHH_FMT);

    }


    /**
     * Get date time from row name
     * @param rowName
     * @return
     */
    public static LocalDateTime getRowDateTime(String rowName) {
        return LocalDateTime.parse(rowName.substring(ROWNAME_PREFIX.length()), YYYYMMDDHH_FMT);
    }


    public static String buildTag(List<String> elements) {
        StringBuffer sb = new StringBuffer();
        elements.forEach(s -> {
            if (sb.length() > 0) {
                sb.append('_');
            }
            sb.append(s);
        });
        return sb.toString();
    }

    /**
     * Expand user home relative paths
     * @param path
     * @return
     */
    public static String expandPath(String path) {
        if (path.startsWith("~" + File.separator)) {
            path = System.getProperty("user.home") + path.substring(1);
        } else if (path.startsWith("~")) {
            throw new UnsupportedOperationException("Home dir expansion not implemented for explicit usernames");
        }
        return path;
    }


    public static List<Integer> rangeSpec(String config) {

        List<Integer> range;
        if (!StringUtils.isEmpty(config)) {
            if (config.contains(",")) {
                String[] splits = config.split(",");
                range = Arrays.stream(splits)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
            } else if (config.contains("-")) {
                String[] splits = config.split("-");
                int start = Integer.parseInt(splits[0]);
                int end = Integer.parseInt(splits[1]);
                range = IntStream.rangeClosed(start,end)
                        .boxed()
                        .collect(Collectors.toList());
            } else {
                range = Lists.newArrayList(Integer.parseInt(config));
            }
        } else {
            range = Lists.newArrayList();
        }
        return range;
    }


}
