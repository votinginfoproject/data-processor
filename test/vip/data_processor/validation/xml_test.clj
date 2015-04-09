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
      (testing "loads simple data from XML"
        (is (= [{:id 39 :name "Ohio" :election_administration_id 3456}]
               (korma/select (get-in out-ctx [:tables :states]))))
        (is (= 7
               (:cnt (first (korma/select (get-in out-ctx [:tables :ballots])
                                          (korma/aggregate (count "*") :cnt))))))
        (assert-column out-ctx :ballot-responses :text ["Yes" "No"])
        (assert-column out-ctx :contests :office ["State Treasurer"
                                                  "Attorney General"
                                                  "State Senate"
                                                  "County Commisioner"
                                                  "County Supervisor At Large"
                                                  nil
                                                  nil])
        (assert-column out-ctx :contest-results :total_votes [1002 250])
        (assert-column out-ctx :custom-ballots :heading ["Should Judge Carlton Smith be retained?"]))
      (testing "loads data from attributes"
        (assert-column out-ctx :ballot-line-results :certification (repeat 4 "certified")))
      (testing "transforms boolean values"
        (assert-column out-ctx :elections :statewide [1])))))
