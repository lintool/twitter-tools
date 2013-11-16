package edu.illinois.lis.feedback;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import cc.twittertools.thrift.gen.TResult;

import edu.illinois.lis.document.FeatureVector;
import edu.illinois.lis.utils.Stopper;
import edu.illinois.lis.utils.KeyValuePair;




public class FeedbackRelevanceModel extends FeedbackModel {
	private boolean stripNumbers = false;
	private double[] docWeights = null;
	
	@Override
	public void build(Stopper stopper) {
		this.stopper = stopper;
		try {
			Set<String> vocab = new HashSet<String>();
			List<FeatureVector> fbDocVectors = new LinkedList<FeatureVector>();

			

			double[] rsvs = new double[relDocs.size()];
			int k=0;
			Iterator<TResult> hitIterator = relDocs.iterator();
			while(hitIterator.hasNext()) {
				TResult hit = hitIterator.next();
				rsvs[k++] = hit.getRsv();
			}
			
			hitIterator = relDocs.iterator();
			while(hitIterator.hasNext()) {
				TResult hit = hitIterator.next();
				String text = hit.getText().toLowerCase();
				FeatureVector docVector = new FeatureVector(text, stopper);
				vocab.addAll(docVector.getFeatures());
				fbDocVectors.add(docVector);
			}

			features = new LinkedList<KeyValuePair>();

			
			Iterator<String> it = vocab.iterator();
			while(it.hasNext()) {
				String term = it.next();				
				double fbWeight = 0.0;

				Iterator<FeatureVector> docIT = fbDocVectors.iterator();
				k=0;
				while(docIT.hasNext()) {
					double docWeight = 1.0;
					if(docWeights != null)
						docWeight = docWeights[k];
					FeatureVector docVector = docIT.next();
					double docProb = docVector.getFeaturetWeight(term) / docVector.getLength();
					docProb *= rsvs[k++] * docWeight;

					fbWeight += docProb;
				}
				
				fbWeight /= (double)fbDocVectors.size();
				
				KeyValuePair tuple = new KeyValuePair(term, fbWeight);
				features.add(tuple);
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void setDocWeights(double[] docWeights) {
		this.docWeights = docWeights;
	}


}
