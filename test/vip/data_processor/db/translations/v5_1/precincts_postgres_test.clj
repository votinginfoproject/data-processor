(ns vip.data-processor.db.translations.v5-1.precincts-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.precincts :as precincts]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "precinct.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/precinct.txt"])
               :spec-version "5.1"
               :ltree-index 28
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [precincts/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "pre90111" "VipObject.0.Precinct.28.id"
       "bs00010" "VipObject.0.Precinct.28.BallotStyleId.0"
       "ed101 ed102" "VipObject.0.Precinct.28.ElectoralDistrictIds.1"
       "ocd-id" "VipObject.0.Precinct.28.ExternalIdentifiers.2.ExternalIdentifier.0.Type.0"
       "ocd-division/country:us" "VipObject.0.Precinct.28.ExternalIdentifiers.2.ExternalIdentifier.0.Value.1"
       "false" "VipObject.0.Precinct.28.IsMailOnly.3"
       "loc70001" "VipObject.0.Precinct.28.LocalityId.4"
       "203 - GEORGETOWN" "VipObject.0.Precinct.28.Name.5"
       "0203" "VipObject.0.Precinct.28.Number.6"
       "poll001 poll002" "VipObject.0.Precinct.28.PollingLocationIds.7"
       "split13" "VipObject.0.Precinct.28.PrecinctSplitName.8"
       "5" "VipObject.0.Precinct.28.Ward.9"
       "pre90112" "VipObject.0.Precinct.29.id"))))
