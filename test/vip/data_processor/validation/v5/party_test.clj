(ns vip.data-processor.validation.v5.party-test
  (:require [vip.data-processor.validation.v5.party :as v5.party]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-colors-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-parties.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.party/validate-colors)
        errors (all-errors errors-chan)]
    (testing "color invalid is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :party
                            :identifier "VipObject.0.Party.0.Color.0"
                            :error-type :format})))
    (testing "color valid is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :party
                           :identifier "VipObject.0.Party.1.Color.0"
                           :error-type :format}))))
