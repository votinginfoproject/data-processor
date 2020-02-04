(ns vip.data-processor.db.translations.v5-1.precincts-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.precincts :as p]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "precinct.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-1/precinct.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          p/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
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
