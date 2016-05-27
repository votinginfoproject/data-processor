(ns vip.data-processor.validation.v5.party-test
  (:require [vip.data-processor.validation.v5.party :as v5.party]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-colors-test
  (let [ctx {:input (xml-input "v5-parties.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.party/validate-colors)]
    (testing "color invalid is an error"
      (is (get-in out-ctx [:errors :party "VipObject.0.Party.0.Color.0"
                           :format])))
    (testing "color valid is OK"
      (is (not (get-in out-ctx [:errors :party "VipObject.0.Party.1.Color.0"
                                :format]))))))
