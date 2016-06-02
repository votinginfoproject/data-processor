(ns vip.data-processor.db.translations.v5-1.sources-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.sources :as sources]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "source.txt is loaded and transformed with contact_information"
    (let [ctx {:input (csv-inputs ["5-1/contact_information.txt" "5-1/source.txt"])
               :spec-version "5.1"
               :ltree-index 0
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [sources/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "ci1024" "VipObject.0.Source.0.ContactInformation.2.label"
       "DemocracyWorks" "VipObject.0.Source.0.ContactInformation.2.AddressLine.0"
       "Take the mall-ride" "VipObject.0.Source.0.ContactInformation.2.Directions.3.Text.0"
       "en" "VipObject.0.Source.0.ContactInformation.2.Directions.3.Text.0.language"
       "39.7500162" "VipObject.0.Source.0.ContactInformation.2.LatLng.8.Latitude.0"

       "source01" "VipObject.0.Source.0.id"
       "2016-06-02T10:24:08" "VipObject.0.Source.0.DateTime.0"
       "SBE is the official source for Virginia data" "VipObject.0.Source.0.Description.1.Text.0"
       "en" "VipObject.0.Source.0.Description.1.Text.0.language"
       "State Board of Elections, Commonwealth of Virginia" "VipObject.0.Source.0.Name.3"
       "http://www.sbe.virginia.gov/" "VipObject.0.Source.0.OrganizationUri.4"
       "http://example.com/terms" "VipObject.0.Source.0.TermsOfUseUri.5"
       "VIP-51" "VipObject.0.Source.0.VipId.6"))))
