(ns vip.data-processor.validation.v5.party-selection-test
  (:require [vip.data-processor.validation.v5.party-selection :as v5.party-selection]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-party-ids-test
  (let [ctx {:input (xml-input "v5-party-selections.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.party-selection/validate-no-missing-party-ids)]
    (testing "party-id missing is an error"
      (is (get-in out-ctx [:errors :party-selection
                           "VipObject.0.PartySelection.0.PartyId" :missing])))
    (testing "party-id present is OK"
      (is (not (get-in out-ctx [:errors :party-selection
                                "VipObject.0.PartySelection.1.PartyId"
                                :missing]))))))
