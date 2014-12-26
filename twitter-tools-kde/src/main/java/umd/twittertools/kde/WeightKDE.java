package umd.twittertools.kde;

import umontreal.iro.lecuyer.probdist.ContinuousDistribution;

public class WeightKDE {
	
	
	public static double computeDensity(Data data,  
			ContinuousDistribution kern, double h, double x) {
		double min = kern.getXinf();
		double max = kern.getXsup();
		double bandwidth = h;
		int n = data.getN();
		double sum = 0;
		for (int i = 0; i < n; i++) {
			double y = (x - data.getValue(i))/bandwidth;
			double weight = data.getWeight(i);
			if (weight != 1.0) {
				boolean debug = true;
			}
			if ((y >= min) && (y <= max)) {
				sum += weight * kern.density(y);
			}
		}
		sum /= (bandwidth * data.getSumOfWeights());
		return sum;
	}
	
	public static double computeDensity(Data data,  
			ContinuousDistribution kern, double x) {
		double min = kern.getXinf();
		double max = kern.getXsup();
		double bandwidth = data.getBaseBandwidth(); // dafault use base bandwidth
		int n = data.getN();
		double sum = 0;
		for (int i = 0; i < n; i++) {
			double y = (x - data.getValue(i))/bandwidth;
			double weight = data.getWeight(i);
			if ((y >= min) && (y <= max)) {
				sum += weight * kern.density(y);
			}
		}
		sum /= (bandwidth * data.getSumOfWeights());
		return sum;
	}
	
	public static double[] computeDensity(Data data,  
			ContinuousDistribution kern, double[] X) {
		double[] densities = new double[X.length];
		for (int i = 0; i < X.length; i++) {
			densities[i] = computeDensity(data, kern, X[i]);
		}
		return densities;
	}
	
	public static double[] computeDensity(Data data,  
			ContinuousDistribution kern, double h, double[] X) {
		double[] densities = new double[X.length];
		for (int i = 0; i < X.length; i++) {
			densities[i] = computeDensity(data, kern, h, X[i]);
		}
		return densities;
	}
	
	public static double[] computeDensity(Data data,  
			ContinuousDistribution kern, String bandwidthOption, double[] X) {
		double bandwidth;
		if (bandwidthOption.equals("base")) {
			bandwidth = data.getBaseBandwidth();
			return computeDensity(data, kern, bandwidth, X);
		}else if (bandwidthOption.equals("silverman")) {
			bandwidth = data.getSilvermanBandwidth();
			return computeDensity(data, kern, bandwidth, X);
		} else {
			throw new IllegalArgumentException("illegal bandwidth option, "
					+ "option can only be 'base' or 'silverman'");
		}
	}
	
}
