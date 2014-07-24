microblogTTGBaseline
====================

A baseline run using an (empirically determined) Jaccard similarity score to cluster tweets.

1. Build with `mvn package`
2. Set your `host`, `group`, and `package` parameters in `config/run_params.json`. Change any other parameters you want.
3. Run with `java -cp target/microblogTTGBaseline-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.gslis.ttg.main.RunTTGBaseline`

Note: Weighted scoring does not work properly, yet.
