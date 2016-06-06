(ns vip.data-processor.db.translations.v5-1.street-segment-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.street-segments :as ss]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "street_segment.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/street_segment.txt"])
               :spec-version "5.1"
               :ltree-index 1001
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [ss/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "ss000001" "VipObject.0.StreetSegment.1001.id"
        "N" "VipObject.0.StreetSegment.1001.AddressDirection.0"
        "Washington" "VipObject.0.StreetSegment.1001.City.1"
        "false" "VipObject.0.StreetSegment.1001.IncludesAllAddresses.2"
        "false" "VipObject.0.StreetSegment.1001.IncludesAllStreets.3"
        "odd" "VipObject.0.StreetSegment.1001.OddEvenBoth.4"
        "p001" "VipObject.0.StreetSegment.1001.PrecinctId.5"
        "101" "VipObject.0.StreetSegment.1001.StartHouseNumber.6"
        "199" "VipObject.0.StreetSegment.1001.EndHouseNumber.7"
        "DC" "VipObject.0.StreetSegment.1001.State.8"
        "NW" "VipObject.0.StreetSegment.1001.StreetDirection.9"
        "Delaware" "VipObject.0.StreetSegment.1001.StreetName.10"
        "St" "VipObject.0.StreetSegment.1001.StreetSuffix.11"
        "20001" "VipObject.0.StreetSegment.1001.Zip.12"

        "ss000002" "VipObject.0.StreetSegment.1002.id"
        "S" "VipObject.0.StreetSegment.1002.AddressDirection.0"
        "Washington" "VipObject.0.StreetSegment.1002.City.1"
        "true" "VipObject.0.StreetSegment.1002.IncludesAllAddresses.2"
        "false" "VipObject.0.StreetSegment.1002.IncludesAllStreets.3"
        "p002" "VipObject.0.StreetSegment.1002.PrecinctId.4"
        "DC" "VipObject.0.StreetSegment.1002.State.5"
        "SE" "VipObject.0.StreetSegment.1002.StreetDirection.6"
        "Wisconsin" "VipObject.0.StreetSegment.1002.StreetName.7"
        "Ave" "VipObject.0.StreetSegment.1002.StreetSuffix.8"
        "20002" "VipObject.0.StreetSegment.1002.Zip.9"))))
