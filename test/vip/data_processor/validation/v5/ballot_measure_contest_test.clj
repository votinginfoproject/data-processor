(ns vip.data-processor.validation.v5.ballot-measure-contest-test
  (:require [vip.data-processor.validation.v5.ballot-measure-contest :as v5.bmc]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-types-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-ballot-measure-contests.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.bmc/validate-no-missing-types)
        errors (all-errors errors-chan)]
    (testing "type missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ballot-measure-contest
                            :identifier "VipObject.0.BallotMeasureContest.0.Type"
                            :error-type :missing})))
    (testing "type present is OK"
      (doseq [path ["VipObject.0.BallotMeasureContest.1.Type"
                    "VipObject.0.BallotMeasureContest.2.Type"
                    "VipObject.0.BallotMeasureContest.3.Type"
                    "VipObject.0.BallotMeasureContest.4.Type"
                    "VipObject.0.BallotMeasureContest.5.Type"]]
        (assert-no-problems errors {:scope :ballot-measure-contest
                                      :identifier path
                                      :error-type :missing})))))

(deftest ^:postgres validate-ballot-measure-types-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-ballot-measure-contests.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.bmc/validate-ballot-measure-types)
        errors (all-errors errors-chan)]
    (testing "a bad type is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ballot-measure-contest
                            :identifier "VipObject.0.BallotMeasureContest.1.Type.0"
                            :error-type :format})))
    (testing "a good type is okay"
      (doseq [path ["VipObject.0.BallotMeasureContest.2.Type.0"
                    "VipObject.0.BallotMeasureContest.3.Type.0"
                    "VipObject.0.BallotMeasureContest.4.Type.0"
                    "VipObject.0.BallotMeasureContest.5.Type.0"]]
        (assert-no-problems errors
                              {:scope :ballot-measure-contest
                               :identifier path
                               :error-type :format})))))
