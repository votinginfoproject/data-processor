(ns vip.data-processor.validation.v5.ordered-contest-test
  (:require [vip.data-processor.validation.v5.ordered-contest :as v5.ordered-contest]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-contest-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-ordered-contests.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ordered-contest/validate-no-missing-contest-ids)
        errors (all-errors errors-chan)]
    (testing "contest-id missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ordered-contest
                            :identifier "VipObject.0.OrderedContest.0.ContestId"
                            :error-type :missing})))
    (testing "contest-id present is OK"
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :ordered-contest
                             :identifier "VipObject.0.OrderedContest.1.ContestId"
                             :error-type :missing}))))
