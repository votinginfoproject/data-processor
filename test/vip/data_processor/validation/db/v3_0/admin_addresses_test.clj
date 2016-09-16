(ns vip.data-processor.validation.db.v3-0.admin-addresses-test
  (:require [vip.data-processor.validation.db.v3-0.admin-addresses :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-election-administration-addresses-test
  (testing "errors are returned if either the physical or mailing address is incomplete"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["bad-election-administration-addresses/election_administration.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-addresses]}
                     (sqlite/temp-db "incomplete-addresses" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administrations
                            :identifier 99990
                            :error-type :incomplete-physical-address}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administrations
                            :identifier 99991
                            :error-type :incomplete-mailing-address})))))
