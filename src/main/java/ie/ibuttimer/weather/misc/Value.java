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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static ie.ibuttimer.weather.misc.Functional.exceptionLoggingConsumer;
import static ie.ibuttimer.weather.misc.Utils.getLogger;

/**
 * A value wrapper class
 */
public class Value implements IStatOps<Value>, Writable {

    // not really but something to work with
    public static final BigDecimal MAX_BIG_DECIMAL = new BigDecimal(Double.MAX_VALUE);
    public static final BigDecimal MIN_BIG_DECIMAL = new BigDecimal(Double.MIN_VALUE);
    public static final BigInteger MAX_BIG_INTEGER = BigInteger.valueOf(Long.MAX_VALUE);
    public static final BigInteger MIN_BIG_INTEGER = BigInteger.valueOf(Long.MIN_VALUE);

    private static Map<String, String> defaultValues;
    static {
        defaultValues = new HashMap<>();
        defaultValues.put(Integer.class.getSimpleName(), "0");
        defaultValues.put(Long.class.getSimpleName(), "0");
        defaultValues.put(Float.class.getSimpleName(), "0");
        defaultValues.put(Double.class.getSimpleName(), "0");
        defaultValues.put(BigDecimal.class.getSimpleName(), "0");
        defaultValues.put(BigInteger.class.getSimpleName(), "0");
        defaultValues.put(String.class.getSimpleName(), "0");
        defaultValues.put(LocalDate.class.getSimpleName(), DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.MIN));
        defaultValues.put(LocalDateTime.class.getSimpleName(), DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.MIN));
    }

    public static <Value> Optional<Value> empty() {
        return Optional.empty();
    }

    private Object value;

    public Value() {
        this.value = null;
    }

    private Value(Object value) {
        this.value = value;
    }

    public static Value of(Object value) {
        return new Value(value);
    }

    public static Value of() {
        return of(null);
    }

    public static Value of(String value, Class<?> cls) {
        return of(value, cls, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    public static Value of(String value, Class<?> cls, DateTimeFormatter formatter) {
        return of(value, cls, formatter, null);
    }

    private static void logException(String msg, Exception e, Logger logger) {
        if (logger != null) {
            logger.warn(msg, e);
        }
    }

    private static void logNumberFormatException(NumberFormatException e, Logger logger) {
        logException("Unable to read value", e, logger);
    }

    public static Value of(String value, Class<?> cls, DateTimeFormatter formatter, Logger logger) {
        Object converted;
        if (cls.equals(Integer.class)) {
            try {
                converted = Integer.valueOf(value);
            } catch (NumberFormatException nfe) {
                converted = 0;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(Long.class)) {
            try {
                converted = Long.valueOf(value);
            } catch (NumberFormatException nfe) {
                converted = 0;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(Float.class)) {
            try {
                converted = Float.valueOf(value);
            } catch (NumberFormatException nfe) {
                converted = 0;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(Double.class)) {
            try {
                converted = Double.valueOf(value);
            } catch (NumberFormatException nfe) {
                converted = 0;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(BigDecimal.class)) {
            try {
                converted = new BigDecimal(value);
            } catch (NumberFormatException nfe) {
                converted = BigDecimal.ZERO;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(BigInteger.class)) {
            try {
                converted = new BigInteger(value);
            } catch (NumberFormatException nfe) {
                converted = BigInteger.ZERO;
                logNumberFormatException(nfe, logger);
            }
        } else if (cls.equals(String.class)) {
            converted = value;
        } else if (cls.equals(LocalDate.class)) {
            converted = Utils.getDate(value, formatter, logger);
        } else if (cls.equals(LocalDateTime.class)) {
            converted = Utils.getDateTime(value, formatter, logger);
        } else {
            throw new UnsupportedOperationException("Unsupported class: " + cls);
        }
        return Value.of(converted);
    }

    public static Optional<Value> ofOptional(Object value) {
        return Optional.of(new Value(value));
    }

    private static boolean is(Class<?> cls, Object value) {
        return cls.isInstance(value);
    }

    public static boolean isNumber(Object value) {
        return value instanceof Number;
    }

    public static boolean isFloatingPoint(Object value) {
        return (value instanceof Float) || (value instanceof Double) || (value instanceof BigDecimal);
    }

    public static boolean isIntegerNumber(Object value) {
        return (value instanceof Integer) || (value instanceof Long) || (value instanceof BigInteger);
    }

    public static boolean isBigNumber(Object value) {
        return isBigInteger(value) || isBigDecimal(value);
    }

    public static boolean isBigInteger(Object value) {
        return (value instanceof BigInteger);
    }

    public static boolean isBigDecimal(Object value) {
        return (value instanceof BigDecimal);
    }

    public static boolean isString(Object value) {
        return (value instanceof String);
    }

    public static boolean isDouble(Object value) {
        return (value instanceof Double);
    }

    public static boolean isFloat(Object value) {
        return (value instanceof Float);
    }

    public static boolean isLong(Object value) {
        return (value instanceof Long);
    }

    public static boolean isInteger(Object value) {
        return (value instanceof Integer);
    }

    public static boolean isShort(Object value) {
        return (value instanceof Short);
    }

    public static boolean isByte(Object value) {
        return (value instanceof Byte);
    }

    public static boolean isCharacter(Object value) {
        return (value instanceof Character);
    }

    public static boolean isLocalDateTime(Object value) {
        return (value instanceof LocalDateTime);
    }

    public static boolean isLocalDate(Object value) {
        return (value instanceof LocalDate);
    }

    public static boolean ifNumber(Object value, Consumer<? super Number> action) {
        boolean result = isNumber(value);
        if (result) {
            action.accept((Number) value);
        }
        return result;
    }

    public static boolean ifBigNumber(Object value, Consumer<? super Number> action) {
        boolean result = isBigNumber(value);
        if (result) {
            action.accept((Number) value);
        }
        return result;
    }

    public static boolean ifBigInteger(Object value, Consumer<? super BigInteger> action) {
        boolean result = isBigInteger(value);
        if (result) {
            action.accept((BigInteger) value);
        }
        return result;
    }

    public static boolean ifBigDecimal(Object value, Consumer<? super BigDecimal> action) {
        boolean result = isBigDecimal(value);
        if (result) {
            action.accept((BigDecimal) value);
        }
        return result;
    }

    public static boolean ifString(Object value, Consumer<? super String> action) {
        boolean result = isString(value);
        if (result) {
            action.accept((String) value);
        }
        return result;
    }

    public static boolean ifDouble(Object value, Consumer<? super Double> action) {
        boolean result = isDouble(value);
        if (result) {
            action.accept((Double) value);
        }
        return result;
    }

    public static boolean ifFloat(Object value, Consumer<? super Float> action) {
        boolean result = isFloat(value);
        if (result) {
            action.accept((Float) value);
        }
        return result;
    }

    public static boolean ifLong(Object value, Consumer<? super Long> action) {
        boolean result = isLong(value);
        if (result) {
            action.accept((Long) value);
        }
        return result;
    }

    public static boolean ifInteger(Object value, Consumer<? super Integer> action) {
        boolean result = isInteger(value);
        if (result) {
            action.accept((Integer) value);
        }
        return result;
    }

    public static boolean ifShort(Object value, Consumer<? super Short> action) {
        boolean result = isShort(value);
        if (result) {
            action.accept((Short) value);
        }
        return result;
    }

    public static boolean ifByte(Object value, Consumer<? super Byte> action) {
        boolean result = isByte(value);
        if (result) {
            action.accept((Byte) value);
        }
        return result;
    }

    public static boolean ifCharacter(Object value, Consumer<? super Character> action) {
        boolean result = isCharacter(value);
        if (result) {
            action.accept((Character) value);
        }
        return result;
    }

    public static boolean ifLocalDateTime(Object value, Consumer<? super LocalDateTime> action) {
        boolean result = isLocalDateTime(value);
        if (result) {
            action.accept((LocalDateTime) value);
        }
        return result;
    }

    public static boolean ifLocalDate(Object value, Consumer<? super LocalDate> action) {
        boolean result = isLocalDate(value);
        if (result) {
            action.accept((LocalDate) value);
        }
        return result;
    }

    public boolean ifPresent(Consumer<? super Object> action) {
        boolean result = (value != null);
        if (result) {
            action.accept(value);
        }
        return result;
    }

    public boolean ifNumber(Consumer<? super Number> action) {
        return ifNumber(value, action);
    }

    public boolean ifBigNumber(Consumer<? super Number> action) {
        return ifBigNumber(value, action);
    }

    public boolean ifBigInteger(Consumer<? super BigInteger> action) {
        return ifBigInteger(value, action);
    }

    public boolean ifBigDecimal(Consumer<? super BigDecimal> action) {
        return ifBigDecimal(value, action);
    }

    public boolean ifString(Consumer<? super String> action) {
        return ifString(value, action);
    }

    public boolean ifDouble(Consumer<? super Double> action) {
        return ifDouble(value, action);
    }

    public boolean ifFloat(Consumer<? super Float> action) {
        return ifFloat(value, action);
    }

    public boolean ifLong(Consumer<? super Long> action) {
        return ifLong(value, action);
    }

    public boolean ifInteger(Consumer<? super Integer> action) {
        return ifInteger(value, action);
    }

    public boolean ifShort(Consumer<? super Short> action) {
        return ifShort(value, action);
    }

    public boolean ifByte(Consumer<? super Byte> action) {
        return ifByte(value, action);
    }

    public boolean ifCharacter(Consumer<? super Character> action) {
        return ifCharacter(value, action);
    }

    public boolean ifLocalDateTime(Consumer<? super LocalDateTime> action) {
        return ifLocalDateTime(value, action);
    }

    public boolean ifLocalDate(Consumer<? super LocalDate> action) {
        return ifLocalDate(value, action);
    }

    public boolean isPresent() {
        return (value != null);
    }

    public boolean isNumber() {
        return isNumber(value);
    }

    public boolean isBigNumber() {
        return isBigNumber(value);
    }

    public boolean isBigInteger() {
        return isBigInteger(value);
    }

    public boolean isBigDecimal() {
        return isBigDecimal(value);
    }

    public boolean isDouble() {
        return isDouble(value);
    }

    public boolean isFloat() {
        return isFloat(value);
    }

    public boolean isLong() {
        return isLong(value);
    }

    public boolean isInteger() {
        return isInteger(value);
    }

    public boolean isString() {
        return isString(value);
    }

    public boolean isLocalDate() {
        return isLocalDate(value);
    }

    public boolean isLocalDateTime() {
        return isLocalDateTime(value);
    }

    public Class<?> getValueClass() {
        return value.getClass();
    }

    private void numericCheck() {
        if (!isNumber()) {
            throw new IllegalStateException("Value is not a Number");
        }
    }

    public Object value() {
        return value;
    }

    public double doubleValue() {
        numericCheck();
        return ((Number) this.value).doubleValue();
    }

    public float floatValue() {
        numericCheck();
        return ((Number) this.value).floatValue();
    }

    public long longValue() {
        numericCheck();
        return ((Number) this.value).longValue();
    }

    public int intValue() {
        numericCheck();
        return ((Number) this.value).intValue();
    }

    public String stringValue() {
        return this.value.toString();
    }

    public static BigInteger bigIntegerValue(Number number) {
        BigInteger result;
        if (number instanceof BigInteger) {
            result = (BigInteger)number;
        } else if (number instanceof BigDecimal) {
            result = ((BigDecimal)number).toBigInteger();
        } else {
            result = new BigInteger(String.valueOf(number.longValue()));
        }
        return result;
    }

    public BigInteger bigIntegerValue() {
        numericCheck();
        return bigIntegerValue((Number)this.value);
    }

    public static BigDecimal bigDecimalValue(Number number) {
        BigDecimal result;
        if (number instanceof BigInteger) {
            result = new BigDecimal((BigInteger)number);
        } else if (number instanceof BigDecimal) {
            result = (BigDecimal)number;
        } else {
            result = new BigDecimal(String.valueOf(number.doubleValue()));
        }
        return result;
    }

    public BigDecimal bigDecimalValue() {
        numericCheck();
        return bigDecimalValue((Number)this.value);
    }

    public void asDouble(Consumer<? super Double> action) {
        if (isNumber()) {
            action.accept(((Number) this.value).doubleValue());
        }
    }

    public void asFloat(Consumer<? super Float> action) {
        if (isNumber()) {
            action.accept(((Number) this.value).floatValue());
        }
    }

    public void asLong(Consumer<? super Long> action) {
        if (isNumber()) {
            action.accept(((Number) this.value).longValue());
        }
    }

    public void asInteger(Consumer<? super Integer> action) {
        if (isNumber()) {
            action.accept(((Number) this.value).intValue());
        }
    }

    public void asString(Consumer<? super String> action) {
        action.accept(this.value.toString());
    }

    public <K, V> void addTo(Map<K, V> map, K key) {
        map.put(key, (V) value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, value.getClass().getSimpleName());
        writeRaw(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String className = Text.readString(dataInput);
        readRaw(dataInput, className);
    }

    public void writeRaw(DataOutput dataOutput) {

        boolean written = ifInteger(exceptionLoggingConsumer(dataOutput::writeInt, IOException.class, getLogger()));
        if (!written) {
            written = ifLong(exceptionLoggingConsumer(dataOutput::writeLong, IOException.class, getLogger()));
        }
        if (!written) {
            written = ifFloat(exceptionLoggingConsumer(dataOutput::writeFloat, IOException.class, getLogger()));
        }
        if (!written) {
            written = ifDouble(exceptionLoggingConsumer(dataOutput::writeDouble, IOException.class, getLogger()));
        }
        if (!written) {
            written = ifString(exceptionLoggingConsumer(s -> Text.writeString(dataOutput, s), IOException.class, getLogger()));
        }
        if (!written) {
            written = ifBigDecimal(exceptionLoggingConsumer(d -> Text.writeString(dataOutput, d.toString()), IOException.class, getLogger()));
        }
        if (!written) {
            written = ifBigInteger(exceptionLoggingConsumer(d -> Text.writeString(dataOutput, d.toString()), IOException.class, getLogger()));
        }
        if (!written) {
            written = ifLocalDate(exceptionLoggingConsumer(d -> dataOutput.writeLong(d.toEpochDay()), IOException.class, getLogger()));
        }
        if (!written) {
            written = ifLocalDateTime(exceptionLoggingConsumer(d -> dataOutput.writeLong(d.toEpochSecond(ZoneOffset.UTC)), IOException.class, getLogger()));
        }
        if (!written) {
            throw new UnsupportedOperationException("Unsupported class: " + value.getClass());
        }
    }

    public void readRaw(DataInput dataInput, String className) throws IOException {
        if (className.equals(Integer.class.getSimpleName())) {
            value = dataInput.readInt();
        } else if (className.equals(Long.class.getSimpleName())) {
            value = dataInput.readLong();
        } else if (className.equals(Float.class.getSimpleName())) {
            value = dataInput.readFloat();
        } else if (className.equals(Double.class.getSimpleName())) {
            value = dataInput.readDouble();
        } else if (className.equals(String.class.getSimpleName())) {
            value = Text.readString(dataInput);
        } else if (className.equals(BigDecimal.class.getSimpleName())) {
            value = new BigDecimal(Text.readString(dataInput));
        } else if (className.equals(BigInteger.class.getSimpleName())) {
            value = new BigInteger(Text.readString(dataInput));
        } else if (className.equals(LocalDate.class.getSimpleName())) {
            value = LocalDate.ofEpochDay(dataInput.readLong());
        } else if (className.equals(LocalDateTime.class.getSimpleName())) {
            value = LocalDateTime.ofEpochSecond(dataInput.readLong(), 0, ZoneOffset.UTC);
        } else {
            throw new UnsupportedOperationException("Unsupported class: " + value.getClass());
        }
    }

    private void areNumbers(Value other) {
        if (!isNumber() || !other.isNumber()) {
            throwNonNumMixedTypes(other.getValueClass());
        }
    }

    private void throwNonNumMixedTypes(Class<?> otherClass) {
        throw new IllegalArgumentException("Unable to perform numeric operations on mixed types:" +
            getValueClass().getSimpleName() + " and " + otherClass.getSimpleName());
    }

    @Override
    public void add(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = v + other.intValue());
        if (!done) { done = ifLong(v -> value = v + other.longValue()); }
        if (!done) { done = ifFloat(v -> value = v + other.floatValue()); }
        if (!done) { done = ifDouble(v -> value = v + other.doubleValue()); }
        if (!done) { done = ifBigInteger(v -> value = v.add(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.add(other.bigDecimalValue())); }
    }

    @Override
    public void subtract(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = v - other.intValue());
        if (!done) { done = ifLong(v -> value = v - other.longValue()); }
        if (!done) { done = ifFloat(v -> value = v - other.floatValue()); }
        if (!done) { done = ifDouble(v -> value = v - other.doubleValue()); }
        if (!done) { done = ifBigInteger(v -> value = v.subtract(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.subtract(other.bigDecimalValue())); }
    }

    @Override
    public void multiply(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = v * other.intValue());
        if (!done) { done = ifLong(v -> value = v * other.longValue()); }
        if (!done) { done = ifFloat(v -> value = v * other.floatValue()); }
        if (!done) { done = ifDouble(v -> value = v * other.doubleValue()); }
        if (!done) { done = ifBigInteger(v -> value = v.multiply(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.multiply(other.bigDecimalValue())); }
    }

    @Override
    public void divide(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = v / other.intValue());
        if (!done) { done = ifLong(v -> value = v / other.longValue()); }
        if (!done) { done = ifFloat(v -> value = v / other.floatValue()); }
        if (!done) { done = ifDouble(v -> value = v / other.doubleValue()); }
        if (!done) { done = ifBigInteger(v -> value = v.divide(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.divide(other.bigDecimalValue(), RoundingMode.UP)); }
    }

    @Override
    public void add(Number num) {
        add(Value.of(num));
    }

    @Override
    public void subtract(Number num) {
        subtract(Value.of(num));
    }

    @Override
    public void multiply(Number num) {
        multiply(Value.of(num));
    }

    @Override
    public void divide(Number num) {
        divide(Value.of(num));
    }

    @Override
    public void set(Value other) {
        value = other.value;
    }

    @Override
    public void min(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = Math.min(v, other.intValue()));
        if (!done) { done = ifLong(v -> value = Math.min(v, other.longValue())); }
        if (!done) { done = ifFloat(v -> value = Math.min(v, other.floatValue())); }
        if (!done) { done = ifDouble(v -> value = Math.min(v, other.doubleValue())); }
        if (!done) { done = ifBigInteger(v -> value = v.min(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.min(other.bigDecimalValue())); }
    }

    @Override
    public void max(Value other) {
        areNumbers(other);
        boolean done = ifInteger(v -> value = Math.max(v, other.intValue()));
        if (!done) { done = ifLong(v -> value = Math.max(v, other.longValue())); }
        if (!done) { done = ifFloat(v -> value = Math.max(v, other.floatValue())); }
        if (!done) { done = ifDouble(v -> value = Math.max(v, other.doubleValue())); }
        if (!done) { done = ifBigInteger(v -> value = v.max(other.bigIntegerValue())); }
        if (!done) { ifBigDecimal(v -> value = v.max(other.bigDecimalValue())); }
    }

    @Override
    public void pow(int exp) {
        boolean done = ifInteger(v -> value = Double.valueOf(Math.pow(v, exp)).intValue());
        if (!done) { done = ifLong(v -> value = Double.valueOf(Math.pow(v, exp)).longValue()); }
        if (!done) { done = ifFloat(v -> value = Double.valueOf(Math.pow(v, exp)).floatValue()); }
        if (!done) { done = ifDouble(v -> value = Math.pow(v, exp)); }
        if (!done) { done = ifBigInteger(v -> value = v.pow(exp)); }
        if (!done) { ifBigDecimal(v -> value = v.pow(exp)); }
    }

    @Override
    public Value copyOf() {
        return Value.of(value);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("Value{");
        sb.append("value=")
            .append(value);
        ifPresent(v -> sb.append(" [class=").append(v.getClass().getSimpleName()).append("]"));
        sb.append('}');
        return sb.toString();
    }

    public static String getDefaultValueStr(Class<?> cls) {
        return defaultValues.getOrDefault(cls.getSimpleName(), "");
    }
}
