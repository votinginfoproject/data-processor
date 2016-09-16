(ns vip.data-processor.validation.v5.state-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.state :as state]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-states.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        state/validate-no-missing-names]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing Names are flagged"
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :states
                            :identifier "VipObject.0.State.0.Name"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :states
                            :identifier "VipObject.0.State.1.Name"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems-2 errors
                            {:scope :states
                             :identifier "VipObject.0.State.2.Name"}))))
