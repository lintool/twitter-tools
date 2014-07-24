microblogTTGBaseline
====================

A baseline run using an (empirically determined) Jaccard similarity score to cluster tweets.

1. Clean with `mvn eclipse:clean`
2. Build with `mvn package`
3. Set your `host`, `group`, and `package` parameters in `config/run_params.json`. Change any other parameters you want.
4. Run with `java -cp target/microblogTTGBaseline-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.gslis.ttg.main.RunTTGBaseline`

Note: Weighted scoring does not work properly, yet.
