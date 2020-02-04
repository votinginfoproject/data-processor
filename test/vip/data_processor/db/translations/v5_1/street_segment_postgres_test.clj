(ns vip.data-processor.db.translations.v5-1.street-segment-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.street-segments :as ss]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "street_segment.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-1/street_segment.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          ss/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values out-ctx
        "ss000001" "VipObject.0.StreetSegment.0.id"
        "N" "VipObject.0.StreetSegment.0.AddressDirection.0"
        "Washington" "VipObject.0.StreetSegment.0.City.1"
        "false" "VipObject.0.StreetSegment.0.IncludesAllAddresses.2"
        "false" "VipObject.0.StreetSegment.0.IncludesAllStreets.3"
        "odd" "VipObject.0.StreetSegment.0.OddEvenBoth.4"
        "p001" "VipObject.0.StreetSegment.0.PrecinctId.5"
        "101" "VipObject.0.StreetSegment.0.StartHouseNumber.6"
        "199" "VipObject.0.StreetSegment.0.EndHouseNumber.7"
        "DC" "VipObject.0.StreetSegment.0.State.8"
        "NW" "VipObject.0.StreetSegment.0.StreetDirection.9"
        "Delaware" "VipObject.0.StreetSegment.0.StreetName.10"
        "St" "VipObject.0.StreetSegment.0.StreetSuffix.11"
        "20001" "VipObject.0.StreetSegment.0.Zip.12"

        "ss000002" "VipObject.0.StreetSegment.1.id"
        "S" "VipObject.0.StreetSegment.1.AddressDirection.0"
        "Washington" "VipObject.0.StreetSegment.1.City.1"
        "true" "VipObject.0.StreetSegment.1.IncludesAllAddresses.2"
        "false" "VipObject.0.StreetSegment.1.IncludesAllStreets.3"
        "p002" "VipObject.0.StreetSegment.1.PrecinctId.4"
        "DC" "VipObject.0.StreetSegment.1.State.5"
        "SE" "VipObject.0.StreetSegment.1.StreetDirection.6"
        "Wisconsin" "VipObject.0.StreetSegment.1.StreetName.7"
        "Ave" "VipObject.0.StreetSegment.1.StreetSuffix.8"
        "20002" "VipObject.0.StreetSegment.1.Zip.9"))))
