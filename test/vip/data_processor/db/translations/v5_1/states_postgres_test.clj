(ns vip.data-processor.db.translations.v5-1.states-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.states :as s]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres -transformer-test
  (testing "state.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/state.txt"])
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
       "st51" "VipObject.0.State.0.id"
       "ea40133" "VipObject.0.State.0.ElectionAdministrationId.0"
       "poll001 poll010 poll100" "VipObject.0.State.0.PollingLocationIds.3"
       "st42" "VipObject.0.State.1.id"
       "Colorado" "VipObject.0.State.1.Name.2"
       "ocd-id" "VipObject.0.State.1.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
       "ocd-division/country:us/state:co" "VipObject.0.State.1.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"))))
