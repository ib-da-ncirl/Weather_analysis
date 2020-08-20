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


import com.google.common.collect.Maps;
import ie.ibuttimer.weather.misc.DataTypes;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeMap {

    private Pattern pattern = Pattern.compile("^(\\w{3,})\\((.+)\\)$");

    private Map<String, DataTypes> map;

    public static final TypeMap STRING_MAPPER = of("str(.*)");

    private TypeMap(String mapString) {
        this.map = decodeMap(mapString);
    }

    public static TypeMap of() {
        return of("");
    }

    public static TypeMap of(String mapString) {
        return new TypeMap(mapString);
    }

    public Map<String, DataTypes> decodeMap(String mapString) {
        Map<String, DataTypes> map = Maps.newHashMap();

        Arrays.asList(mapString.split(",")).forEach(spec -> {
            Matcher matcher = pattern.matcher(spec.trim());
            if (matcher.find()) {
                map.put(matcher.group(2), DataTypes.of(matcher.group(1)));
            }
        });
        return map;
    }

    public Optional<Object> decode(String key, Object value) {
        AtomicReference<Optional<Object>> result = new AtomicReference<>(Optional.empty());
        map.entrySet().stream()
                .filter(e -> key.matches(e.getKey()))
                .findFirst()
                .ifPresent(e -> result.set(
                        Optional.of(decode(e.getValue(), value))));
        return result.get();
    }

    public static Object decode(DataTypes type, Object value) {
        Object result = value;
        boolean isStr = (value instanceof String);
        boolean isBytes = (value instanceof byte[]);
        switch (type) {
            case INT:
                if (isStr) {
                    result = Integer.parseInt((String)value);
                } else if (isBytes) {
                    result = Bytes.toInt((byte[])value);
                }
                break;
            case LONG:
                if (isStr) {
                    result = Long.parseLong((String)value);
                } else if (isBytes) {
                    result = Bytes.toLong((byte[])value);
                }
                break;
            case FLOAT:
                if (isStr) {
                    result = Float.parseFloat((String)value);
                } else if (isBytes) {
                    result = Bytes.toFloat((byte[])value);
                }
                break;
            case DOUBLE:
                if (isStr) {
                    result = Double.parseDouble((String)value);
                } else if (isBytes) {
                    result = Bytes.toDouble((byte[])value);
                }
                break;
            case STRING:
                if (isStr) {
                    result = value;
                } else if (isBytes) {
                    result = new String((byte[])value);
                }
                break;
        }
        return result;
    }

    public static String encode(DataTypes type, String name) {
        return type.shortId() + "(" + name + ")";
    }


    @Override
    public String toString() {
        return "TypeMap{" +
                "map=" + map +
                '}';
    }
}
