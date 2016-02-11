(ns vip.data-processor.validation.xml-test-with-postgres
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.postgres :as psql]
            [squishy.data-readers]
            [korma.core :as korma]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres load-xml-ltree-test
  (testing "imports xml values into postgres"
    (let [ctx {:input (xml-input "v5_sample_feed.xml")
               :pipeline [psql/start-run
                          load-xml-ltree]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (-> (korma/select psql/xml-tree-values
                     (korma/where {:results_id (:import-id out-ctx)})
                     (korma/where (psql/ltree-match psql/xml-tree-values
                                                    :path
                                                    "VipObject.0.Source.*{1}.VipId.*{1}")))
                 first
                 :value)
             "51")))))
