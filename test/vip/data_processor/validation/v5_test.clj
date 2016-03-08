(ns vip.data-processor.validation.v5-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5 :as v5]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres full-good-v5-test
  (let [ctx {:input (xml-input "v5_sample_feed.xml")
             :pipeline (concat [psql/start-run
                                xml/load-xml-ltree]
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)]
    (assert-no-problems out-ctx [])))
