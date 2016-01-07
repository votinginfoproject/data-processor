(ns vip.data-processor.validation.db.v3-0.street-segment-test
  (:require [vip.data-processor.validation.db.v3-0.street-segment :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]))

(deftest validate-no-overlapping-street-segments-test
  (let [ctx (merge {:input (csv-inputs ["overlapping-street-segments/street_segment.txt"])
                    :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                               csv/load-csvs
                               validate-no-overlapping-street-segments]}
                   (sqlite/temp-db "overlapping-street-segments" "3.0"))
        out-ctx (pipeline/run-pipeline ctx)]
    (is (= '(12) (get-in out-ctx [:errors :street-segments 11 :overlaps])))
    (is (= '(14) (get-in out-ctx [:errors :street-segments 13 :overlaps])))
    (is (= #{16 17} (set (get-in out-ctx [:errors :street-segments 15 :overlaps]))))
    (is (= '(19) (get-in out-ctx [:errors :street-segments 18 :overlaps])))
    (is (= '(21) (get-in out-ctx [:errors :street-segments 20 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 22 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 23 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 24 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 25 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 26 :overlaps])))
    (is (= '(28) (get-in out-ctx [:errors :street-segments 27 :overlaps])))
    (assert-error-format out-ctx)))
