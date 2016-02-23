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
