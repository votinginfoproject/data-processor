(ns vip.data-processor.db.translations.v5-1.localities-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "locality.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/locality.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "loc001" "VipObject.0.Locality.0.id"
        "ele123" "VipObject.0.Locality.0.ElectionAdministrationId.0"
        "ocd-id" "VipObject.0.Locality.0.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
        "ocd-division/country:us/state:co/county:denver" "VipObject.0.Locality.0.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"
        "Locality #1" "VipObject.0.Locality.0.Name.2"
        "poll100" "VipObject.0.Locality.1.PollingLocationIds.2"
        "st038" "VipObject.0.Locality.1.StateId.3"
        "other" "VipObject.0.Locality.1.Type.4"
        "unique type" "VipObject.0.Locality.1.OtherType.5"))))
