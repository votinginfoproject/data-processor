(ns vip.data-processor.validation.v5.external-identifiers-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.v5.external-identifiers :as external-identifiers]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-types-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-external-identifiers.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        external-identifiers/validate-no-missing-types]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing Types are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :external-identifiers
                            :identifier "VipObject.0.Party.1.ExternalIdentifiers.1.ExternalIdentifier.0.Type"
                            :error-type :missing})))
    (testing "existing Types are not flagged"
      (assert-no-problems-2 errors
                            {:scope :external-identifiers
                             :identifier "VipObject.0.Party.0.ExternalIdentifiers.1.ExternalIdentifier.0.Type"})
      (assert-no-problems-2 errors
                            {:scope :external-identifiers
                             :identifier "VipObject.0.Party.2.ExternalIdentifiers.1.ExternalIdentifier.0.Type"}))))

(deftest ^:postgres validate-no-missing-values-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-external-identifiers.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        external-identifiers/validate-no-missing-values]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing Values are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :external-identifiers
                            :identifier "VipObject.0.Party.2.ExternalIdentifiers.1.ExternalIdentifier.0.Value"
                            :error-type :missing})))
    (testing "existing Values are not flagged"
      (assert-no-problems-2 errors
                            {:scope :external-identifiers
                             :identifier "VipObject.0.Party.0.ExternalIdentifiers.1.ExternalIdentifier.0.Value"})
      (assert-no-problems-2 errors
                            {:scope :external-identifiers
                             :identifier "VipObject.0.Party.1.ExternalIdentifiers.1.ExternalIdentifier.0.Value"}))))
