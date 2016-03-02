(ns vip.data-processor.validation.v5.candidate-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.candidate :as v5.candidate]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-post-election-statuses-test
  (let [ctx {:input (xml-input "v5-post-election-statuses.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-post-election-statuses]}
        out-ctx (pipeline/run-pipeline ctx)]
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.0.PostElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.1.PostElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.2.PostElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.3.PostElectionStatus.0"])))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.4.PostElectionStatus.0"
                         :format]))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.5.PostElectionStatus.0"
                         :format]))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.6.PostElectionStatus.0"])))))
