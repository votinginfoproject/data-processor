(ns vip.data-processor.db.translations.v5-1.electoral-districts-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.electoral-districts :as ed]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres electoral-district-transforms-test
  (testing "electoral_district.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths
               (csv-inputs ["5-1/electoral_district.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          ed/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values out-ctx
        "ed001" "VipObject.0.ElectoralDistrict.0.id"
        "ocd-id" "VipObject.0.ElectoralDistrict.0.ExternalIdentifiers.0.ExternalIdentifier.0.Type.0"
        "ocd-division/country:us/state:ny/borough:brooklyn" "VipObject.0.ElectoralDistrict.0.ExternalIdentifiers.0.ExternalIdentifier.0.Value.1"
        "Brooklyn" "VipObject.0.ElectoralDistrict.0.Name.1"
        "1" "VipObject.0.ElectoralDistrict.0.Number.2"
        "borough" "VipObject.0.ElectoralDistrict.0.Type.3"

        "ed002" "VipObject.0.ElectoralDistrict.1.id"
        "community-board" "VipObject.0.ElectoralDistrict.1.ExternalIdentifiers.0.ExternalIdentifier.0.OtherType.0"
        "4" "VipObject.0.ElectoralDistrict.1.ExternalIdentifiers.0.ExternalIdentifier.0.Value.1"
        "other" "VipObject.0.ElectoralDistrict.1.Type.3"
        "community-board" "VipObject.0.ElectoralDistrict.1.OtherType.4"))))
