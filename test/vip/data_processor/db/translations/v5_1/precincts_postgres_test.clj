(ns vip.data-processor.db.translations.v5-1.precincts-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "precinct.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/precinct.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "pre90111" "VipObject.0.Precinct.0.id"
       "bs00010" "VipObject.0.Precinct.0.BallotStyleId.0"
       "ed101 ed102" "VipObject.0.Precinct.0.ElectoralDistrictIds.1"
       "ocd-id" "VipObject.0.Precinct.0.ExternalIdentifiers.2.ExternalIdentifier.0.Type.0"
       "ocd-division/country:us" "VipObject.0.Precinct.0.ExternalIdentifiers.2.ExternalIdentifier.0.Value.1"
       "false" "VipObject.0.Precinct.0.IsMailOnly.3"
       "loc70001" "VipObject.0.Precinct.0.LocalityId.4"
       "203 - GEORGETOWN" "VipObject.0.Precinct.0.Name.5"
       "0203" "VipObject.0.Precinct.0.Number.6"
       "poll001 poll002" "VipObject.0.Precinct.0.PollingLocationIds.7"
       "split13" "VipObject.0.Precinct.0.PrecinctSplitName.8"
       "5" "VipObject.0.Precinct.0.Ward.9"
       "pre90112" "VipObject.0.Precinct.1.id"))))
