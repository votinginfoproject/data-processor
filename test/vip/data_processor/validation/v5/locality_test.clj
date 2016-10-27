(ns vip.data-processor.validation.v5.locality-test
  (:require [vip.data-processor.validation.v5.locality :as v5.locality]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-names)
        errors (all-errors errors-chan)]
    (testing "name missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.Name"
                            :error-type :missing})))
    (testing "name present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.Name"
                           :error-type :missing}))))

(deftest ^:postgres validate-no-missing-state-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-state-ids)
        errors (all-errors errors-chan)]
    (testing "state-id missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.StateId"
                            :error-type :missing})))
    (testing "state-id present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.StateId"
                           :error-type :missing}))))
