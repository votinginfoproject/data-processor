(ns vip.data-processor.validation.v5.id-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :refer :all]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.id :as v5.id]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres id-uniqueness-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-duplicate-ids.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-unique-ids]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]

    (testing "duplicate IDs are flagged"
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :id
                            :identifier "VipObject.0.ElectionAuthority.0.id"
                            :error-type :duplicates
                            :error-value "super-duper"}))
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :id
                            :identifier "VipObject.0.ElectionAuthority.1.id"
                            :error-type :duplicates
                            :error-value "super-duper"})))

    (testing "unique IDs are not flagged"
      (assert-no-problems errors
                          {:severity :fatal
                           :scope :id
                           :identifier "VipObject.0.ElectionAuthority.2.id"
                           :error-type :duplicates})
      (assert-no-problems errors
                          {:severity :fatal
                           :scope :id
                           :identifier "VipObject.0.ElectionAuthority.3.id"
                           :error-type :duplicates}))))

(deftest ^:postgres validate-no-missing-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-missing-ids.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-no-missing-ids]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing IDs are flagged"
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :id
                            :identifier "VipObject.0.Person.0.id"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :id
                            :identifier "VipObject.0.Person.2.id"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems errors
                          {:severity :fatal
                           :scope :id
                           :identifier "VipObject.0.Person.1.id"
                           :error-type :missing}))))

(deftest ^:postgres validate-idrefs-refer-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-idrefs.xml")
             :spec-version (atom "5.1")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-idref-references
                        v5.id/validate-idrefs-references]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]

    (testing "IDREF elements that don't have referents are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :id
                            :identifier "VipObject.0.Person.1.PartyId.1"
                            :error-type :no-referent}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :id
                            :identifier "VipObject.0.Person.3.PartyId.1"
                            :error-type :no-referent})))

    (testing "IDREF elements that point to something are good"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :id
                           :identifier "VipObject.0.Person.0.PartyId.1"})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :id
                           :identifier "VipObject.0.Person.2.PartyId.1"}))))

(deftest ^:postgres validate-idrefs-plural-refer-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-idrefs.xml")
             :spec-version (atom "5.1")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-idrefs-references]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]

    (testing "IDREFS elements that don't have any referents are flagged"
      (is (contains-error? errors
           {:severity :errors
            :scope :id
            :identifier "VipObject.0.Locality.8.PollingLocationIds.2"
            :error-type :no-referent})))

    (testing "IDREFS elements that point to some things are good"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :id
                           :identifier "VipObject.0.Locality.11.PollingLocationIds.3"}))))
