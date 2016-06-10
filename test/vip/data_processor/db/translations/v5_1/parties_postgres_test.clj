(ns vip.data-processor.db.translations.v5-1.parties-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "party.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/party.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
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
