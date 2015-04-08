(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]))

(deftest load-xml-test
  (testing "loads data into the db from an XML file"
    (let [ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs data-spec/data-specs}
                     (sqlite/temp-db "full-good-run-xml"))
          out-ctx (load-xml ctx)]
      (is (= [{:id 39 :name "Ohio" :election_administration_id 3456}]
             (korma/select (get-in out-ctx [:tables :states])))))))
