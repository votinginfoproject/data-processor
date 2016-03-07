(ns vip.data-processor.validation.v5.state-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.state :as state]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [ctx {:input (xml-input "v5-states.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        state/validate-no-missing-names]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing Names are flagged"
      (is (get-in out-ctx [:errors :states "VipObject.0.State.0.Name" :missing]))
      (is (get-in out-ctx [:errors :states "VipObject.0.State.1.Name" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:errors :states "VipObject.0.State.2.Name" :missing]))))))
