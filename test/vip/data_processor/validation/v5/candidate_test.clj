(ns vip.data-processor.validation.v5.candidate-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.candidate :as v5.candidate]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-pre-election-statuses-test
  (let [errors-chan (a/chan 100)
        ctx {:errors-chan errors-chan
             :input (xml-input "v5-election-statuses.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-pre-election-statuses]}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (are [path]
        (assert-no-problems-2 errors
                              {:severity :errors
                               :scope :candidates
                               :identifier path})
      "VipObject.0.Candidate.0.PreElectionStatus.0"
      "VipObject.0.Candidate.1.PreElectionStatus.0"
      "VipObject.0.Candidate.2.PreElectionStatus.0"
      "VipObject.0.Candidate.3.PreElectionStatus.0"
      "VipObject.0.Candidate.6.PreElectionStatus.0")
    (are [path]
        (is (contains-error? errors
                             {:severity :errors
                              :scope :candidates
                              :identifier path
                              :error-type :format}))
      "VipObject.0.Candidate.4.PreElectionStatus.0"
      "VipObject.0.Candidate.5.PreElectionStatus.0")))

(deftest ^:postgres validate-post-election-statuses-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-election-statuses.xml")
             :errors-chan errors-chan
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-post-election-statuses]}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (are [path]
        (assert-no-problems-2 errors
                              {:severity :errors
                               :scope :candidates
                               :identifier path})
      "VipObject.0.Candidate.0.PostElectionStatus.1"
      "VipObject.0.Candidate.1.PostElectionStatus.1"
      "VipObject.0.Candidate.2.PostElectionStatus.1"
      "VipObject.0.Candidate.3.PostElectionStatus.1"
      "VipObject.0.Candidate.6.PostElectionStatus.1")
    (are [path]
        (is (contains-error? errors
                             {:severity :errors
                              :scope :candidates
                              :identifier path
                              :error-type :format}))
      "VipObject.0.Candidate.4.PostElectionStatus.1"
      "VipObject.0.Candidate.5.PostElectionStatus.1")))

(deftest ^:postgres validate-no-missing-ballot-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-missing-ballot-names.xml")
             :errors-chan errors-chan
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v5.candidate/validate-no-missing-ballot-names]}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing BallotNames are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :candidates
                            :identifier "VipObject.0.Candidate.0.BallotName"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :candidates
                            :identifier "VipObject.0.Candidate.1.BallotName"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :candidates
                             :identifier "VipObject.0.Candidate.2.BallotName"
                             :error-type :missing}))
    (testing "doesn't care if BallotName isn't first"
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :candidates
                             :identifier "VipObject.0.Candidate.3.BallotName"
                             :error-type :missing}))))
