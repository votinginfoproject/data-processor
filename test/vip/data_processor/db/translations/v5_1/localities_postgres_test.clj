(ns vip.data-processor.db.translations.v5-1.localities-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.localities :as localities]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "locality.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/locality.txt"])
               :spec-version "5.1"
               :ltree-index 3
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [localities/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "loc001" "VipObject.0.Locality.3.id"
        "ele123" "VipObject.0.Locality.3.ElectionAdministrationId.0"
        "ocd-id" "VipObject.0.Locality.3.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
        "ocd-division/country:us/state:co/county:denver" "VipObject.0.Locality.3.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"
        "Locality #1" "VipObject.0.Locality.3.Name.2"
        "poll100" "VipObject.0.Locality.4.PollingLocationIds.2"
        "st038" "VipObject.0.Locality.4.StateId.3"
        "other" "VipObject.0.Locality.4.Type.4"
        "unique type" "VipObject.0.Locality.4.OtherType.5"))))
