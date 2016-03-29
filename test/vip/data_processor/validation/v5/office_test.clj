(ns vip.data-processor.validation.v5.office-test
  (:require [vip.data-processor.validation.v5.office :as v5.office]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [ctx {:input (xml-input "v5-offices.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-no-missing-names)]
    (testing "name missing is an error"
      (is (get-in out-ctx [:errors :office "VipObject.0.Office.0.Name"
                           :missing])))
    (testing "name present is OK"
      (is (not (get-in out-ctx [:errors :office "VipObject.0.Office.1.Name"
                                :missing]))))))

(deftest ^:postgres validate-no-missing-term-types-test
  (let [ctx {:input (xml-input "v5-offices.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-no-missing-term-types)]
    (testing "term missing is OK"
      (is (not (get-in out-ctx [:errors :office "VipObject.0.Office.0.Term"
                                :missing])))
      (is (not (get-in out-ctx [:errors :office "VipObject.0.Office.1.Term"
                                :missing]))))
    (testing "term present w/ type is OK"
      (is (not (get-in out-ctx [:errors :office "VipObject.0.Office.2.Term.0.Type"
                                :missing]))))
    (testing "term present w/o type is an error"
      (is (get-in out-ctx [:errors :office "VipObject.0.Office.3.Term.1.Type"
                           :missing])))))

(deftest ^:postgres validate-term-types-test
  (let [ctx {:input (xml-input "v5-offices.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-term-types)]
    (testing "valid term type is OK"
      (is (not (get-in out-ctx [:errors :term "VipObject.0.Office.2.Term.1.Type.0"
                                :format]))))
    (testing "invalid term type is an error"
      (is (get-in out-ctx [:errors :term "VipObject.0.Office.4.Term.1.Type.0"
                           :format])))))
