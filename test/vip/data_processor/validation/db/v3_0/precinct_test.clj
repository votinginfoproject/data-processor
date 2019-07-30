(ns vip.data-processor.validation.db.v3-0.precinct-test
  (:require [vip.data-processor.validation.db.v3-0.precinct :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-no-missing-polling-locations-test
  (testing "finds precincts without polling locations"
    (let [errors-chan (a/chan 100)
          ctx (merge {:csv-source-file-paths
                      (csv-inputs ["missing-polling-locations/precinct.txt"
                                   "missing-polling-locations/polling_location.txt"
                                   "missing-polling-locations/precinct_polling_location.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-missing-polling-locations]}
                     (sqlite/temp-db "missing-polling-locations" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :warnings
                                   :scope :precincts
                                   :identifier 3
                                   :error-type :missing-polling-location}))
      (is (contains-error? errors {:severity :warnings
                                   :scope :precincts
                                   :identifier 4
                                   :error-type :missing-polling-location}))
      (is (not (contains-error? errors {:severity :warnings
                                        :scope :precincts
                                        :identifier 2
                                        :error-type :missing-polling-location})))
      (is (not (contains-error? errors {:severity :warnings
                                        :scope :precincts
                                        :identifier 1
                                        :error-type :missing-polling-location}))))))
