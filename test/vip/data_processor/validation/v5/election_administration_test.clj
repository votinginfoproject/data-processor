(ns vip.data-processor.validation.v5.election-administration-test
  (:require [vip.data-processor.validation.v5.election-administration :as v5.election-admin]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.string :as str]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-departments-test
  (testing "missing Department element is an error"
    (let [errors-chan (a/chan 100)
          ctx {:xml-source-file-path (xml-input "v5-election-administrations.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election-admin/validate-no-missing-departments)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administration
                            :identifier "VipObject.0.ElectionAdministration.2.Department"
                            :error-type :missing}))
      (assert-no-problems errors
                          {:severity :errors
                           :scope :election-administration
                           :identifier "VipObject.0.ElectionAdministration.0.Department"})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :election-administration
                           :identifier "VipObject.0.ElectionAdministration.1.Department"}))))

(deftest ^:postgres validate-voter-service-type-format-test
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-election-administrations.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.election-admin/validate-voter-service-type-format)
        errors (all-errors errors-chan)]
    (testing "valid VoterService -> Type is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :election-administration
                           :identifier (str/join "." ["VipObject" "0"
                                                      "ElectionAdministration" "0"
                                                      "Department" "0"
                                                      "VoterService" "0"
                                                      "Type" "0"])}))
    (testing "invalid VoterService -> Type is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administration
                            :identifier (str/join "." ["VipObject" "0"
                                                       "ElectionAdministration" "1"
                                                       "Department" "0"
                                                       "VoterService" "0"
                                                       "Type" "0"])
                            :error-type :format})))))
