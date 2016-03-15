(ns vip.data-processor.validation.v5.election-administration-test
  (:require [vip.data-processor.validation.v5.election-administration :as v5.election-admin]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.string :as str]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-departments-test
  (testing "missing Department element is an error"
    (let [ctx {:input (xml-input "v5-election-administrations.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election-admin/validate-no-missing-departments)]
      (is (get-in out-ctx [:errors :election-administration
                           "VipObject.0.ElectionAdministration.2.Department"
                           :missing]))
      (is (not (get-in out-ctx
                       [:errors :election-administration
                        "VipObject.0.ElectionAdministration.0.Department"])))
      (is (not (get-in out-ctx
                       [:errors :election-administration
                        "VipObject.0.ElectionAdministration.1.Department"]))))))

(deftest ^:postgres validate-voter-service-type-format-test
  (let [ctx {:input (xml-input "v5-election-administrations.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.election-admin/validate-voter-service-type-format)]
    (testing "valid VoterService -> Type is OK"
      (is (not (get-in out-ctx
                       [:errors :election-administration
                        (str/join "." ["VipObject" "0"
                                       "ElectionAdministration" "0"
                                       "Department" "0"
                                       "VoterService" "0"
                                       "Type" "0"])]))))
    (testing "invalid VoterService -> Type is an error"
      (is (get-in out-ctx
                  [:errors :election-administration
                   (str/join "." ["VipObject" "0"
                                  "ElectionAdministration" "1"
                                  "Department" "0"
                                  "VoterService" "0"
                                  "Type" "0"])
                   :format])))))
