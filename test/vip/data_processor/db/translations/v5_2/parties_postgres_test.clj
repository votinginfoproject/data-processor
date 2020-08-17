(ns vip.data-processor.db.translations.v5-2.parties-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-2.parties :as p]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-2 :as v5-2]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "party.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-2/party.txt"])
               :errors-chan errors-chan
               :spec-version "5.2"
               :spec-family "5.2"
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-2/data-specs)
                          postgres/prep-v5-2-run
                          process/process-v5-validations
                          csv/load-csvs
                          p/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values
       out-ctx
       "par01" "VipObject.0.Party.0.id"
       "ff0000" "VipObject.0.Party.0.Color.1"
       "http://example.com/donkey.png" "VipObject.0.Party.0.LogoUri.3"
       "Republican" "VipObject.0.Party.0.Name.4.Text.0"
       "en" "VipObject.0.Party.0.Name.4.Text.0.language"
       "DEM" "VipObject.0.Party.1.Abbreviation.0"
       "other" "VipObject.0.Party.1.ExternalIdentifiers.2.ExternalIdentifier.0.Type.0"
       "something silly" "VipObject.0.Party.1.ExternalIdentifiers.2.ExternalIdentifier.0.OtherType.1"
       "curtsying horses" "VipObject.0.Party.1.ExternalIdentifiers.2.ExternalIdentifier.0.Value.2"))))
