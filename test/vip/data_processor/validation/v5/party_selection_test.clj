(ns vip.data-processor.validation.v5.party-selection-test
  (:require [vip.data-processor.validation.v5.party-selection :as v5.party-selection]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-party-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-party-selections.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.party-selection/validate-no-missing-party-ids)
        errors (all-errors errors-chan)]
    (testing "party-ids missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :party-selection
                            :identifier "VipObject.0.PartySelection.0.PartyIds"
                            :error-type :missing})))
    (testing "party-ids present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :party-selection
                           :identifier "VipObject.0.PartySelection.1.PartyIds"
                           :error-type :missing}))))
