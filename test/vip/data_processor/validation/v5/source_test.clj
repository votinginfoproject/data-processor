(ns vip.data-processor.validation.v5.source-test
  (:require [vip.data-processor.validation.v5.source :as v5.source]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-one-source-test
  (testing "more than one Source element is a fatal error"
    (let [ctx {:input (xml-input "v5-more-than-one-source.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-one-source]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:fatal :source "VipObject.0.Source" :count]))))
  (testing "one and only one Source element is OK"
    (let [ctx {:input (xml-input "v5-one-source.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-one-source]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (not (get-in out-ctx [:fatal :source "VipObject.0.Source" :count]))))))
