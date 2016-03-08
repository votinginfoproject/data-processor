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

(deftest ^:postgres validate-name-test
  (testing "missing Name is a fatal error"
    (let [ctx {:input (xml-input "v5-source-without-name.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:fatal :source "VipObject.0.Source.*{1}.Name.*{1}"
                           :missing]))))
  (testing "Name present is OK"
    (let [ctx {:input (xml-input "v5-source-with-name.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (not (:fatal out-ctx)))))
  (testing "Name present is OK even if it's not first"
    (let [ctx {:input (xml-input "v5-source-second-with-name-second.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (not (:fatal out-ctx))))))

(deftest ^:postgres validate-date-time-test
  (testing "missing DateTime is a fatal error"
    (let [ctx {:input (xml-input "v5-source-without-date-time.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:fatal :source "VipObject.0.Source.*{1}.DateTime.*{1}"
                           :missing]))))
  (testing "DateTime present is OK"
    (let [ctx {:input (xml-input "v5-source-with-date-time.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (not (:fatal out-ctx)))))
  (testing "DateTime present is OK even if it's not first"
    (let [ctx {:input (xml-input "v5-source-second-with-date-time-second.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (not (:fatal out-ctx))))))
