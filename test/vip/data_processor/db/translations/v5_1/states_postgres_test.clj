(ns vip.data-processor.db.translations.v5-1.states-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.states :as states]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres -transformer-test
  (testing "state.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/state.txt"])
               :spec-version "5.1"
               :ltree-index 5
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [states/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "st51" "VipObject.0.State.5.id"
       "ea40133" "VipObject.0.State.5.ElectionAdministrationId.0"
       "poll001 poll010 poll100" "VipObject.0.State.5.PollingLocationIds.4"
       "st42" "VipObject.0.State.6.id"
       "Colorado" "VipObject.0.State.6.Name.3"
       "ocd-id" "VipObject.0.State.6.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
       "ocd-division/country:us/state:co" "VipObject.0.State.6.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"))))
