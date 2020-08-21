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

package ie.ibuttimer.weather.hbase;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import ie.ibuttimer.weather.misc.DataTypes;
import ie.ibuttimer.weather.misc.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static ie.ibuttimer.weather.Constants.FAMILY_BYTES;
import static org.apache.hadoop.hbase.client.TableDescriptor.COMPARATOR;

public class Hbase {

    private final Configuration configuration;
    private Connection connection = null;

    private Hbase(String resource) {
        this.configuration = configure(resource);
    }

    public static Hbase of(String resource) {
        return new Hbase(resource);
    }

    public Configuration configure(String resource) {
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
            .getClassLoader()
            .getResource(resource)
            .getPath();
        config.addResource(new Path(path));

        return config;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Connection getConnection() throws IOException {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    public void closeConnection() throws IOException {
        connection.close();
        connection = null;
    }

    public TableDescriptor tableDescriptor(String tableName, String columnFamily) {
        return TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build())
                .build();
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        getConnection().getAdmin().createTable(tableDescriptor(tableName, columnFamily));
    }

    public void disableTable(String tableName) throws IOException {
        getConnection().getAdmin().disableTable(TableName.valueOf(tableName));
    }

    public void deleteTable(String tableName) throws IOException {
        getConnection().getAdmin().deleteTable(TableName.valueOf(tableName));
    }

    public void removeTable(String tableName) throws IOException {
        disableTable(tableName);
        deleteTable(tableName);
    }

    public List<TableDescriptor> getTables() throws IOException {
        return getConnection().getAdmin().listTableDescriptors();
    }

    public boolean tableExists(String tableName, String columnFamily) throws IOException {
        List<TableDescriptor> tableList = getTables();
        TableDescriptor tableDescriptor = tableDescriptor(tableName, columnFamily);
        AtomicBoolean exists = new AtomicBoolean(false);
        tableList.stream()
                .filter(t -> COMPARATOR.compare(t, tableDescriptor) == 0)
                .findFirst()
                .ifPresent(t -> exists.set(true));
        return exists.get();
    }

    public boolean tableExists(TableName table) throws IOException {
        List<TableDescriptor> tableList = getTables();
        AtomicBoolean exists = new AtomicBoolean(false);
        tableList.stream()
                .filter(t -> table.compareTo(t.getTableName()) == 0)
                .findFirst()
                .ifPresent(t -> exists.set(true));
        return exists.get();
    }

    public boolean isAvailable() {
        boolean available = false;
        try {
            HBaseAdmin.available(configuration);
            available = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return available;
    }


    public Map<String, Value> read(String tableName, String row, Map<String, DataTypes> columns) throws IOException {

        Map<String, Value> data = Maps.newHashMap();

        Table table = getConnection().getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);

        return readValues(result, columns, data);
    }

    private Map<String, Value> readValues(Result result, Map<String, DataTypes> columns, Map<String, Value> addTo) throws IOException {

        columns.forEach((name, type) -> {
            byte [] value = result.getValue(FAMILY_BYTES, Bytes.toBytes(name));
            Value val;
            switch (type) {
                case INT:       val = Value.of(Bytes.toInt(value));     break;
                case LONG:      val = Value.of(Bytes.toLong(value));    break;
                case FLOAT:     val = Value.of(Bytes.toFloat(value));   break;
                case DOUBLE:    val = Value.of(Bytes.toDouble(value));  break;
                default:        val = Value.of(new String(value));      break;
            }
            addTo.put(name, val);
        });

        return addTo;
    }

    private Map<String, Value> readValues(Result result, Map<String, DataTypes> columns) throws IOException {
        return readValues(result, columns, Maps.newHashMap());
    }

    /**
     * Get a table of values <row, column, value>
     * @param tableName
     * @param scan
     * @param columns   required columns
     * @param matchRegex    regex to match rows required
     * @return
     * @throws IOException
     */
    public HashBasedTable<String, String, Value> read(String tableName, Scan scan, Map<String, DataTypes> columns,
                                                      String matchRegex) throws IOException {

        HashBasedTable<String, String, Value> data = HashBasedTable.create();

        Table table = getConnection().getTable(TableName.valueOf(tableName));

        ResultScanner scanner = table.getScanner(scan);

        scanner.forEach(result -> {
            try {
                String row = new String(result.getRow());
                boolean skip = false;
                if (!StringUtils.isEmpty(matchRegex)) {
                    // check is required
                    skip = !row.matches(matchRegex);
                }
                if (!skip) {
                    Map<String, Value> rowValues = readValues(result, columns);
                    rowValues.forEach((key, val) -> {
                        data.put(row, key, val);
                    });
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return data;
    }

    public HashBasedTable<String, String, Value> read(String tableName, Scan scan, Map<String, DataTypes> columns) throws IOException {
        return read(tableName, scan, columns, "");
    }

    /**
     * Convert to byte array of string value to save so easy to read in hbase shell
     * @param value
     * @return
     */
    public static byte[] storeValueAsString(int value) {
        return Integer.toString(value).getBytes();
    }

    /**
     * Convert to byte array of string value to save so easy to read in hbase shell
     * @param value
     * @return
     */
    public static byte[] storeValueAsString(long value) {
        return Long.toString(value).getBytes();
    }

    /**
     * Convert to byte array of string value to save so easy to read in hbase shell
     * @param value
     * @return
     */
    public static byte[] storeValueAsString(float value) {
        return Float.toString(value).getBytes();
    }

    /**
     * Convert to byte array of string value to save so easy to read in hbase shell
     * @param value
     * @return
     */
    public static byte[] storeValueAsString(double value) {
        return Double.toString(value).getBytes();
    }

    /**
     * Convert to byte array of string value to save so easy to read in hbase shell
     * @param value
     * @return
     */
    public static byte[] storeValueAsString(String value) {
        return value.getBytes();
    }
}
