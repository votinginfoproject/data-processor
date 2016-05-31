(ns vip.data-processor.db.translations.v5-1.polling-locations-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.polling-locations :as ploc]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "polling_location.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/polling_location.txt"])
               :spec-version "5.1"
               :ltree-index 77
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [ploc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "pl81274" "VipObject.0.PollingLocation.77.id"
       "ALBERMARLE HIGH SCHOOL 2775 Hydraulic Rd Charlottesville, VA 22901" "VipObject.0.PollingLocation.77.AddressLine.0"
       "Use back door" "VipObject.0.PollingLocation.77.Directions.1.Text.0"
       "en" "VipObject.0.PollingLocation.77.Directions.1.Text.0.language"
       "7am-8pm" "VipObject.0.PollingLocation.77.Hours.2.Text.0"
       "en" "VipObject.0.PollingLocation.77.Hours.2.Text.0.language"
       "www.picture.com" "VipObject.0.PollingLocation.77.PhotoUri.3"
       "hours0001" "VipObject.0.PollingLocation.77.HoursOpenId.4"
       "false" "VipObject.0.PollingLocation.77.IsDropBox.5"
       "true" "VipObject.0.PollingLocation.77.IsEarlyVoting.6"
       "38.0754627" "VipObject.0.PollingLocation.77.LatLng.7.Latitude.0"
       "78.5014875" "VipObject.0.PollingLocation.77.LatLng.7.Longitude.1"
       "Google Maps" "VipObject.0.PollingLocation.77.LatLng.7.Source.2"))))
