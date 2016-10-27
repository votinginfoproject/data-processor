(ns vip.data-processor.validation.v5.office-test
  (:require [vip.data-processor.validation.v5.office :as v5.office]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-offices.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-no-missing-names)
        errors (all-errors errors-chan)]
    (testing "name missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :office
                            :identifier "VipObject.0.Office.0.Name"
                            :error-type :missing})))
    (testing "name present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :office
                           :identifier "VipObject.0.Office.1.Name"
                           :error-type :missing}))))

(deftest ^:postgres validate-no-missing-term-types-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-offices.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-no-missing-term-types)
        errors (all-errors errors-chan)]
    (testing "term missing is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :office
                           :identifier "VipObject.0.Office.0.Term"
                           :error-type :missing})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :office
                           :identifier "VipObject.0.Office.1.Term"
                           :error-type :missing}))
    (testing "term present w/ type is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :office
                           :identifier "VipObject.0.Office.2.Term.0.Type"
                           :error-type :missing}))
    (testing "term present w/o type is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :office
                            :identifier "VipObject.0.Office.3.Term.1.Type"
                            :error-type :missing})))))

(deftest ^:postgres validate-term-types-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-offices.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.office/validate-term-types)
        errors (all-errors errors-chan)]
    (testing "valid term type is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :term
                           :identifier "VipObject.0.Office.2.Term.1.Type.0"
                           :error-type :format}))
    (testing "invalid term type is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :term
                            :identifier "VipObject.0.Office.4.Term.1.Type.0"
                            :error-type :format})))))
