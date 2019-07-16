(ns vip.data-processor.db.translations.v5-1.polling-locations-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.polling-locations :as pl]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "polling_location.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/polling_location.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          pl/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values
       out-ctx
       "pl81274" "VipObject.0.PollingLocation.0.id"
       "2775 Hydraulic Rd Charlottesville, VA 22901" "VipObject.0.PollingLocation.0.AddressLine.0"
       "Use back door" "VipObject.0.PollingLocation.0.Directions.1.Text.0"
       "en" "VipObject.0.PollingLocation.0.Directions.1.Text.0.language"
       "7am-8pm" "VipObject.0.PollingLocation.0.Hours.2.Text.0"
       "en" "VipObject.0.PollingLocation.0.Hours.2.Text.0.language"
       "hours0001" "VipObject.0.PollingLocation.0.HoursOpenId.3"
       "false" "VipObject.0.PollingLocation.0.IsDropBox.4"
       "true" "VipObject.0.PollingLocation.0.IsEarlyVoting.5"
       "38.0754627" "VipObject.0.PollingLocation.0.LatLng.6.Latitude.0"
       "78.5014875" "VipObject.0.PollingLocation.0.LatLng.6.Longitude.1"
       "Google Maps" "VipObject.0.PollingLocation.0.LatLng.6.Source.2"
       "ALBERMARLE HIGH SCHOOL" "VipObject.0.PollingLocation.0.Name.7"
       "www.picture.com" "VipObject.0.PollingLocation.0.PhotoUri.8"))))
