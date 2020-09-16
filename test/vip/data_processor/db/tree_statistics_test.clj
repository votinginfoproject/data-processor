(ns vip.data-processor.db.tree-statistics-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.tree-statistics :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.test-helpers :as helpers]
            [korma.core :as korma]))

(deftest camel->snake-test
  (testing "got the regex close"
    (are [x y] (= (camel->snake x) y)
      "SnakeFromCamel" "snake_from_camel"
       "Oneword" "oneword"
       "ABBRHandledLikeThis" "abbr_handled_like_this"
       "lowercase" "lowercase")))

(deftest ^:postgres validate-polling-locations-by-type
  (helpers/setup-postgres #())
  (let [ctx {:xml-source-file-path (helpers/xml-input
                                   "v5-polling-locations-by-type.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        store-tree-stats]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "the polling locations were added by type to the v5_statistics table"
      (let [polling-location-count (-> (korma/select psql/v5-statistics
                                        (korma/fields :polling_location_count)
                                        (korma/where {:id (:import-id out-ctx)}))
                                       first
                                       :polling_location_count)
            db-polling-location-count (-> (korma/select psql/v5-statistics
                                            (korma/fields :db_polling_location_count)
                                            (korma/where {:id (:import-id out-ctx)}))
                                          first
                                          :db_polling_location_count)
            ev-polling-location-count (-> (korma/select psql/v5-statistics
                                            (korma/fields :ev_polling_location_count)
                                            (korma/where {:id (:import-id out-ctx)}))
                                          first
                                          :ev_polling_location_count)]
        (is (= polling-location-count 2))
        (is (= db-polling-location-count 1))
        (is (= ev-polling-location-count 5))))))
