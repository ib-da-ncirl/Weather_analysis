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

/**
 * Interface for implementation by classes on which statistics operations are performed
 * @param <W>   Object class
 */
public interface IStatOps<W> {

    /**
     * Add other's values to this object's values
     * @param other Object to add
     */
    default void add(W other) {
        // no op
    }

    /**
     * Subtract other's values from this object's values
     * @param other Object to add
     */
    default void subtract(W other) {
        // no op
    }

    /**
     * Multiply this object's values by other object's values
     * @param other Object to add
     */
    default void multiply(W other) {
        // no op
    }

    /**
     * Divide this object's values by other object's values
     * @param other Object to add
     */
    default void divide(W other) {
        // no op
    }

    /**
     * Add a constant to this object's values
     * @param num Constant to add
     */
    default void add(Number num) {
        // no op
    }

    /**
     * Subtract a constant from this object's values
     * @param num Constant to subtract
     */
    default void subtract(Number num) {
        // no op
    }

    /**
     * Multiply this object's values by a constant
     * @param num Constant to multiply by
     */
    default void multiply(Number num) {
        // no op
    }

    /**
     * Divide this object's values by a constant
     * @param num Constant to divide by
     */
    default void divide(Number num) {
        // no op
    }

    /**
     * Set this object's values using other
     * @param other Object to set from
     */
    default void set(W other) {
        // no op
    }

    /**
     * Set this object's values to the min of this object's and other's values
     * @param other Object to compare to
     */
    default void min(W other) {
        // no op
    }

    /**
     * Set this object's values to the max of this object's and other's values
     * @param other Object to compare to
     */
    default void max(W other) {
        // no op
    }

    /**
     * Raise this object's values to the specified power
     * @param exp Power to raise to
     */
    default void pow(int exp) {
        // no op
    }

    /**
     * Return a copy of this object
     * @return New copy object
     */
    W copyOf();

}
