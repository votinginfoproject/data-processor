(ns vip.data-processor.validation.v5.external-identifiers-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.v5.external-identifiers :as external-identifiers]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-types-test
  (let [ctx {:input (xml-input "v5-external-identifiers.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        external-identifiers/validate-no-missing-types]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing Types are flagged"
      (is (get-in out-ctx
                  [:errors
                   :external-identifiers
                   "VipObject.0.Party.1.ExternalIdentifiers.1.ExternalIdentifier.0.Type"
                   :missing])))
    (testing "existing Types are not flagged"
      (assert-no-problems out-ctx [:external-identifiers "VipObject.0.Party.0.ExternalIdentifiers.1.ExternalIdentifier.0.Type"])
      (assert-no-problems out-ctx [:external-identifiers "VipObject.0.Party.2.ExternalIdentifiers.1.ExternalIdentifier.0.Type"]))))

(deftest ^:postgres validate-no-missing-values-test
  (let [ctx {:input (xml-input "v5-external-identifiers.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        external-identifiers/validate-no-missing-values]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing Values are flagged"
      (is (get-in out-ctx
                  [:errors
                   :external-identifiers
                   "VipObject.0.Party.2.ExternalIdentifiers.1.ExternalIdentifier.0.Value"
                   :missing])))
    (testing "existing Values are not flagged"
      (assert-no-problems out-ctx [:external-identifiers "VipObject.0.Party.0.ExternalIdentifiers.1.ExternalIdentifier.0.Value"])
      (assert-no-problems out-ctx [:external-identifiers "VipObject.0.Party.1.ExternalIdentifiers.1.ExternalIdentifier.0.Value"]))))
