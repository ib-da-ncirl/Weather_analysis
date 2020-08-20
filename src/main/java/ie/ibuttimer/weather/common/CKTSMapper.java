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

import ie.ibuttimer.weather.hbase.TypeMap;
import ie.ibuttimer.weather.misc.AppLogger;
import ie.ibuttimer.weather.misc.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;

import static ie.ibuttimer.weather.Constants.*;
import static ie.ibuttimer.weather.hbase.TypeMap.STRING_MAPPER;
import static ie.ibuttimer.weather.misc.Utils.getRowDateTime;

public class CKTSMapper extends TableMapper<CompositeKey, TimeSeriesData> {

    private static final AppLogger logger = AppLogger.of(Logger.getLogger("CKTSMapper"));

    private final CompositeKey reducerKey = new CompositeKey();
    private final TimeSeriesData reducerValue = new TimeSeriesData();

    private String[] columnList;

    private TypeMap typeMap;

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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        String param = conf.get(CFG_COLUMN_LIST);
        columnList = param.split(CFG_COLUMN_LIST_SEP);

        param = conf.get(CFG_KEY_TYPE_MAP, "");
        if (StringUtils.isEmpty(param)) {
            typeMap = STRING_MAPPER;
        } else {
            typeMap = TypeMap.of(param);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        LocalDateTime dateTime;
        if (value.containsColumn(FAMILY_BYTES, DATE_ATTR)) {
            String val = new String(value.getValue(FAMILY_BYTES, DATE_ATTR));
            dateTime = LocalDateTime.parse(val, DATETIME_FMT);
        } else {
            // get date time from row name
            String val = new String(value.getRow());
            dateTime = getRowDateTime(val);
        }

        value.listCells().stream()
                .map(x -> ((NoTagsKeyValue) x).toStringMap())
                .filter(x -> !x.get("qualifier").equals(DATE_COL))
                .filter(x -> Arrays.stream(columnList).anyMatch(y -> ((String)x.get("qualifier")).matches(y)))
                .forEach(x -> {
                    // read the cell value and write it out as
                    // CompositeKey(column name, timestamp), TimeSeriesData(timestamp, float value)
                    String columnName = (String)x.get("qualifier");
                    long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC);

                    // set output key to column name, timestamp
                    reducerKey.set(columnName, timestamp);

                    reducerValue.setTimestamp(timestamp);

                    /* hbase stores everything as bytes, so need to decode the bytes appropriately,
                     * i.e. do bytes represent a float value or the string representation of a float value */
                    Optional<Object> colVal = typeMap.decode(columnName, value.getValue(FAMILY_BYTES, columnName.getBytes()));

                    if (!colVal.isPresent()) {
                        logger.warn(String.format(
                                "Could not decode value for column %s using map %s", columnName, typeMap));
                    }
                    colVal.ifPresent(v -> {
                        Value val = null;
                        if (v instanceof String) {
                            val = Value.of(Float.parseFloat((String)v));
                        } else if (v instanceof Float) {
                            val = Value.of(v);
                        } else if (v instanceof Double) {
                            val = Value.of(((Double) v).floatValue());
                        }
                        if (val != null) {
                            reducerValue.setValue(val);
                            try {
                                context.write(reducerKey, reducerValue);
                            } catch (IOException | InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            logger.warn("Ignoring column value of type " + v.getClass().getSimpleName());
                        }
                    });
                });
    }
}
