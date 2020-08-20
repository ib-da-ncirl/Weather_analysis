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

import java.util.Arrays;
import java.util.Map;

public enum DataTypes {
    UNKNOWN, STRING, INT, LONG, FLOAT, DOUBLE;

    public static DataTypes of(String type) {
        DataTypes dType = UNKNOWN;
        for (DataTypes iType : values() ) {
            if (type.equals(shortId(iType)) || type.equals(longId(iType))) {
                dType = iType;
                break;
            }

        }
        return dType;
    }

    public String shortId() {
        return shortId(this);
    }

    public String longId() {
        return longId(this);
    }

    public static String shortId(DataTypes type) {
        String id;
        switch (type) {
            case INT:       id = "int";     break;
            case LONG:      id = "lng";     break;
            case FLOAT:     id = "flt";     break;
            case DOUBLE:    id = "dbl";     break;
            case STRING:    id = "str";     break;
            default:        id = "";        break;
        }
        return id;
    }

    public static String longId(DataTypes type) {
        String id;
        switch (type) {
            case INT:       id = "integer"; break;
            case LONG:      id = "long";    break;
            case FLOAT:     id = "float";   break;
            case DOUBLE:    id = "double";  break;
            case STRING:    id = "string";  break;
            default:        id = "";        break;
        }
        return id;
    }
}
