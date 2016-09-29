(ns vip.data-processor.validation.v5.election-test
  (:require [vip.data-processor.validation.v5.election :as v5.election]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-one-election-test
  (testing "more than one Election element is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-more-than-one-election.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-one-election)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :election
                            :identifier "VipObject.0.Election"
                            :error-type :count}))))
  (testing "one and only one Election element is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-one-election.xml")
               :pipeline []
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-one-election)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-date-test
  (testing "Date element missing is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-one-election.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-date)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :election
                            :identifier "VipObject.0.Election.0.Date"
                            :error-type :missing}))))
  (testing "Date element present is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-election-with-date.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-date)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-state-id-test
  (testing "StateId element missing is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-one-election.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-state-id)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :election
                            :identifier "VipObject.0.Election.0.StateId"
                            :error-type :missing}))))
  (testing "StateId element present is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-election-with-state-id.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.election/validate-state-id)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))
