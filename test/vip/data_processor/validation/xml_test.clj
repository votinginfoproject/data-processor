(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]))

(deftest load-xml-test
  (testing "loads data into the db from an XML file"
    (let [ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs data-spec/data-specs}
                     (sqlite/temp-db "load-xml-test"))
          out-ctx (load-xml ctx)]
      (testing "loads simple data from XML"
        (is (= [{:id 39 :name "Ohio" :election_administration_id 3456}]
               (korma/select (get-in out-ctx [:tables :states]))))
        (is (= 6
               (:cnt (first (korma/select (get-in out-ctx [:tables :ballots])
                                          (korma/aggregate (count "*") :cnt))))))
        (assert-column out-ctx :ballot-responses :text ["Yes" "No"])
        (assert-column out-ctx :contests :office ["County Commisioner" nil])
        (assert-column out-ctx :contest-results :total_votes [1002 250])
        (assert-column out-ctx :custom-ballots :heading ["Should Judge Carlton Smith be retained?"]))
      (testing "loads data from attributes"
        (assert-column out-ctx :ballot-line-results :certification (repeat 4 "certified")))
      (testing "transforms boolean values"
        (assert-column out-ctx :elections :statewide [1]))
      (testing "loads addresses"
        (is (= [{:mailing_address_line1 "P.O. Box 1776"
                 :mailing_address_city "Columbus"
                 :mailing_address_state "OH"
                 :mailing_address_zip "33333"}]
               (korma/select (get-in out-ctx [:tables :election-administrations])
                             (korma/fields :mailing_address_line1
                                           :mailing_address_city
                                           :mailing_address_state
                                           :mailing_address_zip))))
        (is (= [{:physical_address_location_name "Government Center"
                 :physical_address_line1 "12 Chad Ct."
                 :physical_address_city "Columbus"
                 :physical_address_state "OH"
                 :physical_address_zip "33333"}]
               (korma/select (get-in out-ctx [:tables :election-administrations])
                             (korma/fields :physical_address_location_name
                                           :physical_address_line1
                                           :physical_address_city
                                           :physical_address_state
                                           :physical_address_zip))))
        (is (= [{:filed_mailing_address_line1 "123 Fake St."
                 :filed_mailing_address_city "Rockville"
                 :filed_mailing_address_state "OH"
                 :filed_mailing_address_zip "20852"}]
               (korma/select (get-in out-ctx [:tables :candidates])
                             (korma/fields :filed_mailing_address_line1
                                           :filed_mailing_address_city
                                           :filed_mailing_address_state
                                           :filed_mailing_address_zip)
                             (korma/where {:id 90001}))))
        (is (= [{:non_house_address_street_direction "E"
                 :non_house_address_street_name "Guinevere"
                 :non_house_address_street_suffix "Dr"
                 :non_house_address_address_direction "SE"
                 :non_house_address_apartment "616S"
                 :non_house_address_state "VA"
                 :non_house_address_city "Annandale"
                 :non_house_address_zip "22003"}]
               (korma/select (get-in out-ctx [:tables :street-segments])
                             (korma/fields :non_house_address_street_direction
                                           :non_house_address_street_name
                                           :non_house_address_street_suffix
                                           :non_house_address_address_direction
                                           :non_house_address_apartment
                                           :non_house_address_state
                                           :non_house_address_city
                                           :non_house_address_zip)
                             (korma/where {:id 1210001}))))
        (is (= [{:address_location_name "Springfield Elementary"
                 :address_line1 "123 Main St."
                 :address_city "Fake Twp"
                 :address_state "OH"
                 :address_zip "33333"}]
               (korma/select (get-in out-ctx [:tables :polling-locations])
                             (korma/fields :address_location_name
                                           :address_line1
                                           :address_city
                                           :address_state
                                           :address_zip)
                             (korma/where {:id 20121})))))
      (testing "loads join table data"
        (let [precinct-polling-locations (korma/select
                                          (get-in out-ctx [:tables :precinct-polling-locations])
                                          (korma/where {:precinct_id 10103}))]
          (is (= #{20121 20122} (set (map :polling_location_id precinct-polling-locations)))))
        (let [referendum-ballot-responses (korma/select
                                           (get-in out-ctx [:tables :referendum-ballot-responses])
                                           (korma/where {:referendum_id 90011}))]
          (is (= #{120001 120002} (set (map :ballot_response_id referendum-ballot-responses)))))
        (let [locality-early-vote-sites (korma/select
                                         (get-in out-ctx [:tables :locality-early-vote-sites]))]
          (is (= [{:locality_id 101 :early_vote_site_id 30203}]
                 locality-early-vote-sites))))))
  (testing "adds errors for non-UTF-8 data"
    (let [ctx (merge {:input (xml-input "non-utf-8.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml]}
                     (sqlite/temp-db "non-utf-8"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (get-in out-ctx [:errors :candidates "90001" "name"])
             ["Is not valid UTF-8."]))
      (assert-error-format out-ctx))))

(deftest full-good-run-test
  (testing "a good XML file produces no erorrs or warnings"
    (let [ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs data-spec/data-specs
                      :pipeline (concat [load-xml] db/validations)}
                     (sqlite/temp-db "full-good-xml"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (nil? (:stop out-ctx)))
      (is (nil? (:exception out-ctx)))
      (assert-no-problems out-ctx []))))

(deftest validate-no-duplicated-ids-test
  (testing "returns an error when there is a duplicated id"
    (let [ctx (merge {:input (xml-input "duplicated-ids.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicated-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= #{"precincts" "localities"}
             (set (get-in out-ctx [:errors :import 101 :duplicate-ids]))))
      (assert-error-format out-ctx))))

(deftest validate-no-duplicated-rows-test
  (testing "returns a warning if two nodes have the same data"
    (let [ctx (merge {:input (xml-input "duplicated-rows.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicated-rows"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:warnings :ballots 80000 :duplicate-rows]))
      (is (get-in out-ctx [:warnings :ballots 80001 :duplicate-rows]))
      (assert-error-format out-ctx))))

(deftest validate-references-test
  (testing "returns an error if there are unreferenced objects"
    (let [ctx (merge {:input (xml-input "unreferenced-ids.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-references]}
                     (sqlite/temp-db "unreferenced-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '({"state_id" 99}) (get-in out-ctx [:errors :localities 101 :unmatched-reference])))
      (assert-error-format out-ctx))))

(deftest validate-jurisdiction-references-test
  (testing "returns an error if there are unreferenced jurisdiction references"
    (let [ctx (merge {:input (xml-input "unreferenced-jurisdictions.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-jurisdiction-references]}
                     (sqlite/temp-db "unreferenced-jurisdictions"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '({:jurisdiction_id 99999}) (get-in out-ctx [:errors :ballot-line-results 91008 :unmatched-reference])))
      (assert-error-format out-ctx))))

(deftest validate-one-record-limit-test
  (testing "returns an error if particular nodes are duplicated more than once"
    (let [ctx (merge {:input (xml-input "one-record-limit.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-one-record-limit]}
                     (sqlite/temp-db "one-record-limit"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= ["File needs to contain exactly one row."]
             (get-in out-ctx [:errors :elections :global :row-constraint])))
      (is (= ["File needs to contain exactly one row."]
             (get-in out-ctx [:errors :sources :global :row-constraint])))
      (assert-error-format out-ctx))))

(deftest validate-no-unreferenced-rows
  (testing "returns a warning if it finds rows that are unreferenced"
    (let [ctx (merge {:input (xml-input "unreferenced-rows.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:warnings :candidates 90000 :unreferenced-row]))
      (assert-error-format out-ctx))))

(deftest validate-no-overlapping-street-segments
  (testing "returns an error if street segments overlap"
    (let [ctx (merge {:input (xml-input "overlapping-street-segments.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml db/validate-no-overlapping-street-segments]}
                     (sqlite/temp-db "overlapping-street-segments"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '(1210003) (get-in out-ctx [:errors :street-segments 1210002 :overlaps])))
      (assert-error-format out-ctx))))

(deftest validate-election-administration-addresses
  (testing "returns an error if either the physical or mailing address is incomplete"
    (let [ctx (merge {:input (xml-input "incomplete-election-administrations.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [load-xml
                                 db/validate-election-administration-addresses]}
                     (sqlite/temp-db "incomplete-election-administrations"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors :election-administrations 3456
                           :incomplete-physical-address]))
      (is (get-in out-ctx [:errors :election-administrations 3456
                           :incomplete-mailing-address]))
      (assert-error-format out-ctx))))

(deftest validate-data-formats
  (let [ctx (merge {:input (xml-input "bad-data-values.xml")
                    :data-specs data-spec/data-specs
                    :pipeline [load-xml]}
                   (sqlite/temp-db "bad-data-values"))
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "adds fatal errors for missing required fields"
      (is (get-in out-ctx [:fatal :candidates "90001" "name"])))
    (testing "adds errors for values that fail format validation"
      (is (get-in out-ctx [:errors :candidates "90001" "candidate_url"]))
      (is (get-in out-ctx [:errors :candidates "90001" "phone"]))
      (is (get-in out-ctx [:errors :candidates "90001" "email"]))
      (is (get-in out-ctx [:errors :candidates "90001" "sort_order"])))
    (testing "puts errors in the right format"
      (assert-error-format out-ctx))))
