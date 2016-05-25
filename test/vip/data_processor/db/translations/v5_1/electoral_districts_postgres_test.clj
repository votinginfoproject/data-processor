(ns vip.data-processor.db.translations.v5-1.electoral-districts-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.electoral-districts :as ed]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres electoral-district-transforms-test
  (testing "electoral_district.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/electoral_district.txt"])
               :spec-version "5.1"
               :ltree-index 100
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [ed/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "ed001" "VipObject.0.ElectoralDistrict.100.id"
        "ocd-id" "VipObject.0.ElectoralDistrict.100.ExternalIdentifiers.0.ExternalIdentifier.0.Type.0"
        "ocd-division/country:us/state:ny/borough:brooklyn" "VipObject.0.ElectoralDistrict.100.ExternalIdentifiers.0.ExternalIdentifier.0.Value.1"
        "Brooklyn" "VipObject.0.ElectoralDistrict.100.Name.1"
        "1" "VipObject.0.ElectoralDistrict.100.Number.2"
        "borough" "VipObject.0.ElectoralDistrict.100.Type.3"

        "ed002" "VipObject.0.ElectoralDistrict.101.id"
        "community-board" "VipObject.0.ElectoralDistrict.101.ExternalIdentifiers.0.ExternalIdentifier.0.OtherType.0"
        "4" "VipObject.0.ElectoralDistrict.101.ExternalIdentifiers.0.ExternalIdentifier.0.Value.1"
        "other" "VipObject.0.ElectoralDistrict.101.Type.3"
        "community-board" "VipObject.0.ElectoralDistrict.101.OtherType.4"))))
