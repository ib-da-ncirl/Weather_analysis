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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class Hbase {

    private Configuration configuration;
    private Connection connection = null;

    public Hbase(String resource) {
        this.configuration = configure(resource);
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

    public void createTable(String tableName, String columnFamily) throws IOException {
        getConnection().getAdmin().createTable(
            TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build())
                .build()
        );
    }

    public List<TableDescriptor> getTables() throws IOException {
        return getConnection().getAdmin().listTableDescriptors();
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
}
