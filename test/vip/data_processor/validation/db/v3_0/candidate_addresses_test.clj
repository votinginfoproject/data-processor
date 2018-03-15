(ns vip.data-processor.validation.db.v3-0.candidate-addresses-test
  (:require
   [clojure.core.async :as a]
   [clojure.test :refer [deftest testing is run-tests]]
   [vip.data-processor.db.sqlite :as sqlite]
   [vip.data-processor.pipeline :as pipeline]
   [vip.data-processor.test-helpers :as test-helpers]
   [vip.data-processor.validation.csv :as csv]
   [vip.data-processor.validation.data-spec :as data-spec]
   [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
   [vip.data-processor.validation.db.v3-0.candidate-addresses :as candidate-addresses]))

(deftest validate-candidate-addresses-test
  (testing "errors are returned if either the physical or mailing address is incomplete"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (test-helpers/csv-inputs ["bad-candidate-addresses/candidate.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 candidate-addresses/validate-addresses]}
                     (sqlite/temp-db "incomplete-candidate-addresses" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (test-helpers/all-errors errors-chan)]

      (is (test-helpers/contains-error? errors
                                        {:severity :critical
                                         :scope :candidates
                                         :identifier 10478
                                         :error-type :incomplete-candidate-address}))

      (is (test-helpers/contains-error? errors
                                        {:severity :critical
                                         :scope :candidates
                                         :identifier 10479
                                         :error-type :incomplete-candidate-address})))))
