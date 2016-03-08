(ns vip.data-processor.validation.v5.election-test
  (:require [vip.data-processor.validation.v5.election :as v5.election]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-one-election-test
  (testing "more than one Election element is a fatal error"
    (let [ctx {:input (xml-input "v5-more-than-one-election.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-one-election)]
      (is (get-in out-ctx [:fatal :election "VipObject.0.Election" :count]))))
  (testing "one and only one Election element is OK"
    (let [ctx {:input (xml-input "v5-one-election.xml")
               :pipeline []}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-one-election)]
      (assert-no-problems out-ctx []))))

(deftest ^:postgres validate-date-test
  (testing "Date element missing is a fatal error"
    (let [ctx {:input (xml-input "v5-election-without-date.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-date)]
      (is (get-in out-ctx [:fatal :election "VipObject.0.Election.0.Date"
                           :missing]))))
  (testing "Date element present is OK"
    (let [ctx {:input (xml-input "v5-election-with-date.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-date)]
      (assert-no-problems out-ctx []))))

(deftest ^:postgres validate-state-id-test
  (testing "StateId element missing is a fatal error"
    (let [ctx {:input (xml-input "v5-election-without-state-id.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-state-id)]
      (is (get-in out-ctx [:fatal :election "VipObject.0.Election.0.StateId"
                           :missing]))))
  (testing "StateId element present is OK"
    (let [ctx {:input (xml-input "v5-election-with-state-id.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-state-id)]
      (assert-no-problems out-ctx []))))
