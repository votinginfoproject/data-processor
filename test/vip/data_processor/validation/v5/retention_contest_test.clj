(ns vip.data-processor.validation.v5.retention-contest-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.v5.retention-contest :as v5.retention-contest]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-candidate-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-missing-candidate-ids.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.retention-contest/validate-no-missing-candidate-ids]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing CandidateIds are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :retention-contests
                            :identifier "VipObject.0.RetentionContest.0.CandidateId"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :retention-contests
                           :identifier "VipObject.0.RetentionContest.1.CandidateId"
                           :error-type :missing}))))
