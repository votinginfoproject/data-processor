(ns vip.data-processor.validation.v5.candidate-contest-test
  (:require [vip.data-processor.validation.v5.candidate-contest :as v5.cc]
            [clojure.test :refer [deftest testing use-fixtures is run-tests]]
            [vip.data-processor.test-helpers :as test-helpers]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once test-helpers/setup-postgres)

(deftest ^:postgres validate-no-missing-electoral-district-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (test-helpers/xml-input "v5-candidate-contests.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.cc/validate-no-missing-types)
        errors (test-helpers/all-errors errors-chan)]
    (testing "electoral-district-id missing is an error"
      (is (test-helpers/contains-error?
           errors
           {:severity :errors
            :scope :candidate-contest
            :identifier "VipObject.0.CandidateContest.1.ElectoralDistrictId"
            :error-type :missing})))
    (testing "electoral-district-id present is OK"
      (doseq [path ["VipObject.0.CandidateContest.2.ElectoralDistrictId"]]
        (test-helpers/assert-no-problems
         errors {:scope :candidate-contest
                 :identifier path
                 :error-type :missing})))))
