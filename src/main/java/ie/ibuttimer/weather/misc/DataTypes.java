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

public enum DataTypes {
    UNKNOWN, STRING, INT, LONG, FLOAT, DOUBLE;

    public static DataTypes of(String type) {
        DataTypes dType;
        if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer")) {
            dType = INT;
        } else if (type.equalsIgnoreCase("lng") || type.equalsIgnoreCase("long")) {
            dType = LONG;
        } else if (type.equalsIgnoreCase("flt") || type.equalsIgnoreCase("float")) {
            dType = FLOAT;
        } else if (type.equalsIgnoreCase("dbl") || type.equalsIgnoreCase("double")) {
            dType = DOUBLE;
        } else if (type.equalsIgnoreCase("str") || type.equalsIgnoreCase("string")) {
            dType = STRING;
        } else {
            dType = UNKNOWN;
        }
        return dType;
    }

}
