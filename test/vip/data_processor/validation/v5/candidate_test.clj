(ns vip.data-processor.validation.v5.candidate-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.candidate :as v5.candidate]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-pre-election-statuses-test
  (let [ctx {:input (xml-input "v5-election-statuses.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-pre-election-statuses]}
        out-ctx (pipeline/run-pipeline ctx)]
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.0.PreElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.1.PreElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.2.PreElectionStatus.0"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.3.PreElectionStatus.0"])))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.4.PreElectionStatus.0"
                         :format]))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.5.PreElectionStatus.0"
                         :format]))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.6.PreElectionStatus.0"])))))

(deftest ^:postgres validate-post-election-statuses-test
  (let [ctx {:input (xml-input "v5-election-statuses.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-post-election-statuses]}
        out-ctx (pipeline/run-pipeline ctx)]
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.0.PostElectionStatus.1"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.1.PostElectionStatus.1"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.2.PostElectionStatus.1"])))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.3.PostElectionStatus.1"])))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.4.PostElectionStatus.1"
                         :format]))
    (is (get-in out-ctx [:errors
                         :candidates
                         "VipObject.0.Candidate.5.PostElectionStatus.1"
                         :format]))
    (is (not (get-in out-ctx [:errors
                              :candidates
                              "VipObject.0.Candidate.6.PostElectionStatus.1"])))))

(deftest ^:postgres validate-no-missing-ballot-names-test
  (let [ctx {:input (xml-input "v5-missing-ballot-names.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-no-missing-ballot-names]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing BallotNames are flagged"
      (is (get-in out-ctx [:errors :candidates "VipObject.0.Candidate.0.BallotName" :missing]))
      (is (get-in out-ctx [:errors :candidates "VipObject.0.Candidate.1.BallotName" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:errors :candidates "VipObject.0.Candidate.2.BallotName" :missing]))))
    (testing "doesn't care if BallotName isn't first"
      (is (not (get-in out-ctx [:errors :candidates "VipObject.0.Candidate.3.BallotName" :missing]))))))
