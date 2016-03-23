(ns vip.data-processor.validation.v5.id-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :refer :all]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.id :as v5.id]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres id-uniqueness-test
  (let [ctx {:input (xml-input "v5-duplicate-ids.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-unique-ids]}
        out-ctx (pipeline/run-pipeline ctx)]

    (testing "duplicate IDs are flagged"
      (is (= (get-in out-ctx [:fatal :id "VipObject.0.ElectionAuthority.0.id" :duplicates])
             ["super-duper"]))
      (is (= (get-in out-ctx [:fatal :id "VipObject.0.ElectionAuthority.1.id" :duplicates])
             ["super-duper"])))

    (testing "unique IDs are not flagged"
      (is (not (get-in out-ctx [:fatal :id "VipObject.0.ElectionAuthority.2.id" :duplicates])))
      (is (not (get-in out-ctx [:fatal :id "VipObject.0.ElectionAuthority.3.id" :duplicates]))))))

(deftest ^:postgres validate-no-missing-ids-test
  (let [ctx {:input (xml-input "v5-missing-ids.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-no-missing-ids]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing IDs are flagged"
      (is (get-in out-ctx [:fatal :id "VipObject.0.Person.0.id" :missing]))
      (is (get-in out-ctx [:fatal :id "VipObject.0.Person.2.id" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:fatal :id "VipObject.0.Person.1.id" :missing]))))))

(deftest ^:postgres validate-idrefs-refer-test
  (let [ctx {:input (xml-input "v5-idrefs.xml")
             :spec-version "5.0"
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.id/validate-idrefs-refer]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "IDREF elements that don't have referents are flagged"
      (is (get-in out-ctx [:errors :id "VipObject.0.Person.1.PartyId.1" :no-referent]))
      (is (get-in out-ctx [:errors :id "VipObject.0.Person.3.PartyId.1" :no-referent])))
    (testing "IDREF elements that point to something are good"
      (assert-no-problems out-ctx [:errors :id "VipObject.0.Person.0.PartyId.1"])
      (assert-no-problems out-ctx [:errors :id "VipObject.0.Person.2.PartyId.1"]))))
