(ns vip.data-processor.validation.v5.retention-contest-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.v5.retention-contest :as v5.retention-contest]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-candidate-ids-test
  (let [ctx {:input (xml-input "v5-missing-candidate-ids.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.retention-contest/validate-no-missing-candidate-ids]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing CandidateIds are flagged"
      (is (get-in out-ctx [:errors :retention-contests "VipObject.0.RetentionContest.0.CandidateId" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:errors :retention-contests "VipObject.0.RetentionContest.1.CandidateId" :missing]))))))
