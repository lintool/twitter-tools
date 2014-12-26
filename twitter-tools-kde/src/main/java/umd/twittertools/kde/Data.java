package umd.twittertools.kde;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.Locale;

import umontreal.iro.lecuyer.util.PrintfFormat;

public class Data {
	private int n;
	private boolean allWeightIsOne;
	private DoubleArrayList values;
	private DoubleArrayList weights;
	private double min, max;
	private double sumOfWeights;
	private double sampleMean;
	private double sampleVariance;
	private double sampleStandardDeviation;
	// Silverman's rule of thumb, wiki "kernel density estimation"
	private double silvermanBandwidth; 
	private double baseBandwidth;
	
	public Data() {
		n = 0;
		allWeightIsOne = true;
		sumOfWeights = 0;
		values = new DoubleArrayList();
		weights = new DoubleArrayList();
	}
	
	public Data(double[] obs) {
		if (obs.length <= 1) {
			throw new IllegalArgumentException(
					"Two or more observations are needed");
		}
		n = obs.length;
		allWeightIsOne = true;
		sumOfWeights = obs.length;
		values = new DoubleArrayList(obs);
		weights = new DoubleArrayList();
		for (int i = 0; i < values.size(); i++) {
			weights.add(1);
		}
	}
	
	public Data(double[] obs, double[] obs_weights) {
		if (obs.length <= 1) {
			throw new IllegalArgumentException(
					"Two or more observations are needed");
		}
		n = obs.length;
		allWeightIsOne = false;
		sumOfWeights = 0;
		values = new DoubleArrayList(obs);
		weights = new DoubleArrayList(obs_weights);
		for (double weight: weights) sumOfWeights += weight;
	}

	public void computeStatistics() {
		double sum = 0.0;
		min = max = values.get(0);
		for (double element: values) {
			sum += element;
			if (element < min) {
				min = element;
			}
			if (element > max) {
				max = element;
			}
		}
		sampleMean = sum / n;
		sum = 0.0;
		for (double element: values) {
			double coeff = (element - sampleMean);
			sum += coeff * coeff;
		}
		sampleVariance = sum / (n - 1);
		sampleStandardDeviation = Math.sqrt(sampleVariance);
		silvermanBandwidth = 1.06 * sampleStandardDeviation * Math.pow(n, -0.2);
		baseBandwidth = 0.9 * Math.min(sampleStandardDeviation, getInterQuartileRange() / 1.34) 
				* Math.pow(n, -0.2);
	}
	
	public void addValue(double value, double weight) {
		if (this.values.size() != this.weights.size()) {
			throw new IllegalArgumentException("the size of values and weights don't match");
		}
		if (weight < 0) {
			throw new IllegalArgumentException("weight can't be negative");
		}
		this.values.add(value);
		this.weights.add(weight);
		sumOfWeights += weight;
		n++;
		if (allWeightIsOne) allWeightIsOne = false;
	}
	
	public double getInterQuartileRange() {
		DoubleArrayList sortedValues = new DoubleArrayList(values);
		Collections.sort(sortedValues);
		int j = n/2;
		double lowerqrt=0, upperqrt=0;
		if (j % 2 == 1) {
			lowerqrt = sortedValues.get((j+1)/2-1);
		    upperqrt = sortedValues.get(n-(j+1)/2);
		}
		else {
			lowerqrt = 0.5 * (sortedValues.get(j/2-1) + sortedValues.get(j/2+1-1));
			upperqrt = 0.5 * (sortedValues.get(n-j/2) + sortedValues.get(n-j/2-1));
		}
		double h =upperqrt - lowerqrt;
		if (h < 0) {
			throw new IllegalStateException("Observations MUST be sorted");
		}
	    return h;
	}
	
	public double getMean() {
		return sampleMean;
	}

	public double getStandardDeviation() {
		return sampleStandardDeviation;
	}

	public double getVariance() {
		return sampleVariance;
	}
	
	public int getN() {
		return n;
	}
	
	public double getMin() {
		return min;
	}
	
	public double getMax() {
		return max;
	}
	
	public double getValue(int i) {
		return this.values.get(i);
	}
	
	public double getWeight(int i) {
		return this.weights.get(i);
	}
	
	public double getSumOfWeights() {
		return sumOfWeights;
	}
	
	
	
	public double getSilvermanBandwidth() {
		if (silvermanBandwidth <= 0) {
			throw new IllegalArgumentException ("bandwidth < 0");
		}
		return silvermanBandwidth;
	}
	
	public double getBaseBandwidth() {
		if (baseBandwidth <= 0) {
			throw new IllegalArgumentException ("bandwidth < 0");
		}
		return baseBandwidth;
	}
	
	public double getSampleMean() {
		return sampleMean;
	}
	
	public boolean isWeighted() {
		return !allWeightIsOne;
	}
	/**
	 * Returns the sample variance of the observations.
	 * 
	 */
	public double getSampleVariance() {
		return sampleVariance;
	}

	/**
	 * Returns the sample standard deviation of the observations.
	 * 
	 */
	public double getSampleStandardDeviation() {
		return sampleStandardDeviation;
	}

	/**
	 * Returns a String containing information about the current
	 * distribution.
	 * 
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb, Locale.US);
		formatter.format(getClass().getSimpleName() + PrintfFormat.NEWLINE);
		for (int i = 0; i < n; i++) {
			formatter.format("%f%n", values.get(i));
		}
		return sb.toString();
	}
}
