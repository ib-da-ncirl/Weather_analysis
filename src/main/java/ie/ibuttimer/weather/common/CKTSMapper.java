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

import ie.ibuttimer.weather.misc.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static ie.ibuttimer.weather.Constants.*;

public class CKTSMapper extends TableMapper<CompositeKey, TimeSeriesData> {

    private final CompositeKey reducerKey = new CompositeKey();
    private final TimeSeriesData reducerValue = new TimeSeriesData();

    private String[] columnList;

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

        String listParam = conf.get(CFG_COLUMN_LIST);
        columnList = listParam.split(CFG_COLUMN_LIST_SEP);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String val = new String(value.getValue(FAMILY_BYTES, DATE_ATTR));
        LocalDateTime dateTime = LocalDateTime.parse(val, DATETIME_FMT);

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
                    String columnVal = new String(value.getValue(FAMILY_BYTES, columnName.getBytes()));
                    reducerValue.setValue(Value.of(Float.parseFloat(columnVal)));
                    try {
                        context.write(reducerKey, reducerValue);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
