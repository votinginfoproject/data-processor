(ns vip.data-processor.db.translations.v5-1.parties-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.parties :as p]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "party.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/party.txt"])
               :spec-version "5.1"
               :ltree-index 8
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [p/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
                           "par01" "VipObject.0.Party.8.id"
                           "ff0000" "VipObject.0.Party.8.Color.1"
                           "http://example.com/donkey.png" "VipObject.0.Party.8.LogoUri.3"
                           "Republican" "VipObject.0.Party.8.Name.4.Text.0"
                           "en" "VipObject.0.Party.8.Name.4.Text.0.language"
                           "DEM" "VipObject.0.Party.9.Abbreviation.0"
                           "other" "VipObject.0.Party.9.ExternalIdentifiers.2.ExternalIdentifier.0.Type.0"
                           "something silly" "VipObject.0.Party.9.ExternalIdentifiers.2.ExternalIdentifier.0.OtherType.1"
                           "curtsying horses" "VipObject.0.Party.9.ExternalIdentifiers.2.ExternalIdentifier.0.Value.2"))))
