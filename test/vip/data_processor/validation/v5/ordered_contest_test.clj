(ns vip.data-processor.validation.v5.ordered-contest-test
  (:require [vip.data-processor.validation.v5.ordered-contest :as v5.ordered-contest]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-contest-ids-test
  (let [ctx {:input (xml-input "v5-ordered-contests.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ordered-contest/validate-no-missing-contest-ids)]
    (testing "contest-id missing is an error"
      (is (get-in out-ctx [:errors :ordered-contest
                           "VipObject.0.OrderedContest.0.ContestId" :missing])))
    (testing "contest-id present is OK"
      (is (not (get-in out-ctx [:errors :ordered-contest
                                "VipObject.0.OrderedContest.1.ContestId"
                                :missing]))))))
