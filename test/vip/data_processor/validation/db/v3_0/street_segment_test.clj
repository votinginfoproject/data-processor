(ns vip.data-processor.validation.db.v3-0.street-segment-test
  (:require [vip.data-processor.validation.db.v3-0.street-segment :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-no-overlapping-street-segments-test
  (let [errors-chan (a/chan 100)
        ctx (merge {:input (csv-inputs ["overlapping-street-segments/street_segment.txt"])
                    :errors-chan errors-chan
                    :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                               csv/load-csvs
                               validate-no-overlapping-street-segments]}
                   (sqlite/temp-db "overlapping-street-segments" "3.0"))
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (are [id overlap-id] (is (contains-error? errors
                                              {:severity :errors
                                               :scope :street-segments
                                               :identifier id
                                               :error-type :overlaps
                                               :error-value overlap-id}))
      11 12
      13 14
      15 16
      15 17
      18 19
      20 21
      27 28)
    (doseq [id [22 23 24 25 26 29 30]]
      (is (nil? (contains-error?  errors
                                  {:severity :errors
                                   :scope :street-segments
                                   :identifier id
                                   :error-type :overlaps}))))))
