(ns vip.data-processor.db.translations.v5-1.sources-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.sources :as s]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "source.txt is loaded and transformed with contact_information"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths
               (csv-inputs ["5-1/contact_information.txt" "5-1/source.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          s/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values
       out-ctx
       "ci1024" "VipObject.0.Source.0.FeedContactInformation.2.label"
       "DemocracyWorks" "VipObject.0.Source.0.FeedContactInformation.2.AddressLine.0"
       "Take the mall-ride" "VipObject.0.Source.0.FeedContactInformation.2.Directions.3.Text.0"
       "en" "VipObject.0.Source.0.FeedContactInformation.2.Directions.3.Text.0.language"
       "39.7500162" "VipObject.0.Source.0.FeedContactInformation.2.LatLng.8.Latitude.0"

       "source01" "VipObject.0.Source.0.id"
       "2016-06-02T10:24:08" "VipObject.0.Source.0.DateTime.0"
       "SBE is the official source for Virginia data" "VipObject.0.Source.0.Description.1.Text.0"
       "en" "VipObject.0.Source.0.Description.1.Text.0.language"
       "State Board of Elections, Commonwealth of Virginia" "VipObject.0.Source.0.Name.3"
       "http://www.sbe.virginia.gov/" "VipObject.0.Source.0.OrganizationUri.4"
       "http://example.com/terms" "VipObject.0.Source.0.TermsOfUseUri.5"
       "VIP-51" "VipObject.0.Source.0.VipId.6"))))
