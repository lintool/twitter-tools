Model Option:
-model kde
-model recency
-model win

Using KDE model:

(1) Training on even 2011 topics, testing on odd 2011 topics:

sh target/appassembler/bin/RunTemporalModel -model kde -debug true -output output/ \
-traininput data/ql.2011.even.txt -trainqrels data/qrels.2011.even.txt \
-testinput data/ql.2011.odd.txt -testqrels data/qrels.2011.odd.txt

(2) Training on odd 2011 topics, testing on even 2011 topics:

sh target/appassembler/bin/RunTemporalModel -model kde -debug true -output output/ \
-traininput data/ql.2011.odd.txt -trainqrels data/qrels.2011.odd.txt \
-testinput data/ql.2011.even.txt -testqrels data/qrels.2011.even.txt 

(3) Cross-Training, training on 2013 topics, testing on 2011 topics:

sh target/appassembler/bin/RunTemporalModel -model kde -debug true -output output/ \
-traininput data/ql.2013.total.txt -trainqrels data/qrels.2013.total.txt \
-testinput data/ql.2011.total.txt -testqrels data/qrels.2011.total.txt 

Similar commands on 2013 topics or using recency and WIN models.
