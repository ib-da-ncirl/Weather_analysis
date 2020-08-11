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

import org.apache.log4j.Logger;

import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * Functional programming utilities
 * Nod to https://www.baeldung.com/java-lambda-exceptions
 */
public class Functional {

    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends Exception> {
        void accept(T t) throws E;
    }

    public static <T> Consumer<T> throwingConsumer(ThrowingConsumer<T, Exception> consumer) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public static <T, E extends Exception> Consumer<T> exceptionLoggingConsumer(
        ThrowingConsumer<T, E> consumer, Class<E> exceptionClass, Logger logger) {

        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception ex) {
                try {
                    E exCast = exceptionClass.cast(ex);
                    logger.warn("Exception: " + exCast.getMessage(), exCast);
                } catch (ClassCastException ccEx) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    @FunctionalInterface
    public interface BiThrowingConsumer<T, E extends Exception, F extends Exception> {
        void accept(T t) throws E, F;
    }

    public static <T, E extends Exception, F extends Exception> Consumer<T> biExceptionLoggingConsumer(
        BiThrowingConsumer<T, E, F> consumer, Class<E> exceptionClass1, Class<F> exceptionClass2, Logger logger) {

        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception ex) {
                try {
                    E exCast = exceptionClass1.cast(ex);
                    logger.warn("Exception: " + exCast.getMessage(), exCast);
                } catch (ClassCastException ccEx1) {
                    try {
                        F exCast = exceptionClass2.cast(ex);
                        logger.warn("Exception: " + exCast.getMessage(), exCast);
                    } catch (ClassCastException ccEx2) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        };
    }

    @FunctionalInterface
    public interface ThrowingBiConsumer<T, U, E extends Exception> {
        void accept(T t, U u) throws E;
    }

    public static <T, U> BiConsumer<T, U> throwingBiConsumer(ThrowingBiConsumer<T, U, Exception> consumer) {
        return (t, u) -> {
            try {
                consumer.accept(t, u);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public static <T, U, E extends Exception> BiConsumer<T, U> exceptionLoggingBiConsumer(
        ThrowingBiConsumer<T, U, E> consumer, Class<E> exceptionClass, Logger logger) {

        return (t, u) -> {
            try {
                consumer.accept(t, u);
            } catch (Exception ex) {
                try {
                    E exCast = exceptionClass.cast(ex);
                    logger.warn("Exception: " + exCast.getMessage(), exCast);
                } catch (ClassCastException ccEx) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }


    @FunctionalInterface
    public interface BiThrowingBiConsumer<T, U, E extends Exception, F extends Exception> {
        void accept(T t, U u) throws E, F;
    }

    public static <T, U, E extends Exception, F extends Exception> BiConsumer<T, U> biExceptionLoggingBiConsumer(
        BiThrowingBiConsumer<T, U, E, F> consumer, Class<E> exceptionClass1, Class<F> exceptionClass2, Logger logger) {

        return (t, u) -> {
            try {
                consumer.accept(t, u);
            } catch (Exception ex) {
                try {
                    E exCast = exceptionClass1.cast(ex);
                    logger.warn("Exception: " + exCast.getMessage(), exCast);
                } catch (ClassCastException ccEx1) {
                    try {
                        F exCast = exceptionClass2.cast(ex);
                        logger.warn("Exception: " + exCast.getMessage(), exCast);
                    } catch (ClassCastException ccEx2) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        };
    }
}
