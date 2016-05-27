(ns vip.data-processor.validation.v5.ballot-measure-contest-test
  (:require [vip.data-processor.validation.v5.ballot-measure-contest :as v5.bmc]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-types-test
  (let [ctx {:input (xml-input "v5-ballot-measure-contests.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.bmc/validate-no-missing-types)]
    (testing "type missing is an error"
      (is (get-in out-ctx [:errors :ballot-measure-contest
                           "VipObject.0.BallotMeasureContest.0.Type" :missing])))
    (testing "type present is OK"
      (doseq [path ["VipObject.0.BallotMeasureContest.1.Type"
                    "VipObject.0.BallotMeasureContest.2.Type"
                    "VipObject.0.BallotMeasureContest.3.Type"
                    "VipObject.0.BallotMeasureContest.4.Type"
                    "VipObject.0.BallotMeasureContest.5.Type"]]
        (assert-no-problems out-ctx [:ballot-measure-contest
                                     path
                                     :missing])))))

(deftest ^:postgres validate-ballot-measure-types-test
  (let [ctx {:input (xml-input "v5-ballot-measure-contests.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.bmc/validate-ballot-measure-types)]
    (testing "a bad type is an error"
      (is (get-in out-ctx [:errors :ballot-measure-contest
                           "VipObject.0.BallotMeasureContest.1.Type.0"
                           :format])))
    (testing "a good type is okay"
      (doseq [path ["VipObject.0.BallotMeasureContest.2.Type.0"
                    "VipObject.0.BallotMeasureContest.3.Type.0"
                    "VipObject.0.BallotMeasureContest.4.Type.0"
                    "VipObject.0.BallotMeasureContest.5.Type.0"]]
        (assert-no-problems out-ctx [:ballot-measure-contest
                                     path
                                     :format])))))
