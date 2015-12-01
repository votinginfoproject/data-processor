(ns vip.data-processor.validation.db.precinct-test
  (:require [vip.data-processor.validation.db.precinct :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]))

(deftest validate-no-missing-polling-locations-test
  (testing "finds precincts without polling locations"
    (let [ctx (merge {:input (csv-inputs ["missing-polling-locations/precinct.txt"
                                          "missing-polling-locations/polling_location.txt"
                                          "missing-polling-locations/precinct_polling_location.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-missing-polling-locations]}
                     (sqlite/temp-db "missing-polling-locations" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:warnings :precincts 3 :missing-polling-location]))
      (is (get-in out-ctx [:warnings :precincts 4 :missing-polling-location]))
      (is (nil? (get-in out-ctx [:warnings :precincts 2 :missing-polling-location])))
      (is (nil? (get-in out-ctx [:warnings :precincts 1 :missing-polling-location]))))))
