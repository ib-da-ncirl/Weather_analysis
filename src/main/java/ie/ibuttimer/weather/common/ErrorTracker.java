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

/**
 * Class to track error values
 *
 * Criterion from
 * "Assessing the performance of a Gaussian mixture with AIC and BIC"
 * "Hands-On Unsupervised Learning with Python" by Giuseppe Bonaccorso
 * ISBN: 9781789348279
 * https://learning.oreilly.com/library/view/hands-on-unsupervised-learning/9781789348279/966e5cc9-d8bd-401c-afdd-d1a88c2ae896.xhtml
 */
public class ErrorTracker {

    private double sqErrorSum;
    private double absErrorSum;
    private long count;

    public ErrorTracker() {
        this.sqErrorSum = 0.0;
        this.absErrorSum = 0.0;
        this.count = 0;
    }

    public void addError(double value, double error) {
        sqErrorSum += Math.pow(error, 2);
        double percent;
        if (error == 0.0 && value == 0.0) {
            percent = 0;    // special case; 0.0/0.0 = Nan
        } else {
            percent = Math.abs(error) / Math.abs(value);
        }
        // mean arctangent absolute percentage error (MAAPE); has no divide by zero issue like MAPE
        absErrorSum += Math.atan(percent);

        ++count;
    }

    /**
     * Calculate MSE
     * @return
     */
    public double getMSE() {
        return sqErrorSum / count;
    }

    /**
     * Calculate MAAPE
     * @return
     */
    public double getMAAPE() {
        return absErrorSum / count;
    }

    /**
     * Calculate Akaike information criterion (AIC)
     *
     * @param numParam estimated number of parameters
     * @param likelihood log-likelihood
     * @return
     */
    public double getAIC(int numParam, double likelihood) {
        // AIC = 2 * Np -2 * LL
        return (2 * numParam) - (2 * Math.log(likelihood));
    }

    /**
     * Calculate Bayesian Information Criterion (BIC)
     *
     * @param numSamples number of samples
     * @param numParam estimated number of parameters
     * @param likelihood log-likelihood
     * @return
     */
    public double getBIC(int numSamples, int numParam, double likelihood) {
        // BIC = log(N) * Np - 2 * LL
        return (Math.log(numSamples) * numParam) - (2 * Math.log(likelihood));
    }

    public long getCount() {
        return count;
    }
}
