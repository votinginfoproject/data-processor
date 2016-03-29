(ns vip.data-processor.validation.v5.locality-test
  (:require [vip.data-processor.validation.v5.locality :as v5.locality]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [ctx {:input (xml-input "v5-localities.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-names)]
    (testing "name missing is an error"
      (is (get-in out-ctx [:errors :locality "VipObject.0.Locality.0.Name"
                           :missing])))
    (testing "name present is OK"
      (is (not (get-in out-ctx [:errors :locality "VipObject.0.Locality.1.Name"
                                :missing]))))))

(deftest ^:postgres validate-no-missing-state-ids-test
  (let [ctx {:input (xml-input "v5-localities.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-state-ids)]
    (testing "state-id missing is an error"
      (is (get-in out-ctx [:errors :locality
                           "VipObject.0.Locality.0.StateId" :missing])))
    (testing "state-id present is OK"
      (is (not (get-in out-ctx [:errors :locality
                                "VipObject.0.Locality.1.StateId" :missing]))))))
