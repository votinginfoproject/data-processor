(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [clojure.data.xml :as data.xml]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.db.v3-0.admin-addresses :as admin-addresses]
            [vip.data-processor.validation.db.v3-0.jurisdiction-references :as jurisdiction-references]
            [vip.data-processor.validation.db.v3-0.street-segment :as street-segment]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]))

(deftest load-xml-test
  (testing "loads data into the db from an XML file"
    (let [ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "load-xml-test" "3.0"))
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
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml]}
                     (sqlite/temp-db "non-utf-8" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (get-in out-ctx [:errors :candidates "90001" "name"])
             ["Is not valid UTF-8."]))
      (assert-error-format out-ctx)))
  (testing "can continue after reaching malformed XML"
    (let [ctx (merge {:input (xml-input "malformed.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml]}
                     (sqlite/temp-db "malformed-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:critical :import :global :malformed-xml]))
      (testing "but still loads data before the malformation!"
        (is (= [{:id 39 :name "Ohio" :election_administration_id 3456}]
               (korma/select (get-in out-ctx [:tables :states])))))
      (assert-error-format out-ctx))))

(deftest full-good-run-test
  (testing "a good XML file produces no erorrs or warnings"
    (let [ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs v3-0/data-specs
                      :pipeline (concat [determine-spec-version
                                         branch-on-spec-version]
                                        db/validations)}
                     (sqlite/temp-db "full-good-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (nil? (:stop out-ctx)))
      (is (nil? (:exception out-ctx)))
      (assert-no-problems out-ctx [])
      (testing "inserts values for columns not in the first element of a type"
        (let [mail-only-precinct (first
                                  (korma/select (get-in out-ctx [:tables :precincts])
                                                (korma/where {:id 10203})))]
          (is (= 1 (:mail_only mail-only-precinct))))))))

(deftest validate-no-duplicated-ids-test
  (testing "returns an error when there is a duplicated id"
    (let [ctx (merge {:input (xml-input "duplicated-ids.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml db/validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicated-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= #{"precincts" "localities"}
             (set (get-in out-ctx [:errors :import 101 :duplicate-ids]))))
      (assert-error-format out-ctx))))

(deftest validate-no-duplicated-rows-test
  (testing "returns a warning if two nodes have the same data"
    (let [ctx (merge {:input (xml-input "duplicated-rows.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml db/validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicated-rows" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (doseq [id [900101 900102 900103]]
        (is (get-in out-ctx [:warnings :candidates id :duplicate-rows])))
      (testing "except for ballots"
        (doseq [id [80000 80001]]
          (is (nil? (get-in out-ctx [:warnings :ballots id :duplicate-rows])))))
      (assert-error-format out-ctx))))

(deftest validate-references-test
  (testing "returns an error if there are unreferenced objects"
    (let [ctx (merge {:input (xml-input "unreferenced-ids.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml db/validate-references]}
                     (sqlite/temp-db "unreferenced-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '({"state_id" 99}) (get-in out-ctx [:errors :localities 101 :unmatched-reference])))
      (assert-error-format out-ctx))))

(deftest validate-jurisdiction-references-test
  (testing "returns an error if there are unreferenced jurisdiction references"
    (let [ctx (merge {:input (xml-input "unreferenced-jurisdictions.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml jurisdiction-references/validate-jurisdiction-references]}
                     (sqlite/temp-db "unreferenced-jurisdictions" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '({:jurisdiction_id 99999}) (get-in out-ctx [:errors :ballot-line-results 91008 :unmatched-reference])))
      (assert-error-format out-ctx))))

(deftest validate-one-record-limit-test
  (testing "returns an error if particular nodes are duplicated more than once"
    (let [ctx (merge {:input (xml-input "one-record-limit.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml db/validate-one-record-limit]}
                     (sqlite/temp-db "one-record-limit" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= ["File needs to contain exactly one row."]
             (get-in out-ctx [:errors :elections :global :row-constraint])))
      (is (= ["File needs to contain exactly one row."]
             (get-in out-ctx [:errors :sources :global :row-constraint])))
      (assert-error-format out-ctx))))

(deftest validate-no-unreferenced-rows
  (testing "returns a warning if it finds rows that are unreferenced"
    (let [ctx (merge {:input (xml-input "unreferenced-rows.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml db/validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:warnings :candidates 90000 :unreferenced-row]))
      (assert-error-format out-ctx))))

(deftest validate-no-overlapping-street-segments
  (testing "returns an error if street segments overlap"
    (let [ctx (merge {:input (xml-input "overlapping-street-segments.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml street-segment/validate-no-overlapping-street-segments]}
                     (sqlite/temp-db "overlapping-street-segments" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= '(1210003) (get-in out-ctx [:errors :street-segments 1210002 :overlaps])))
      (assert-error-format out-ctx))))

(deftest validate-election-administration-addresses
  (testing "returns an error if either the physical or mailing address is incomplete"
    (let [ctx (merge {:input (xml-input "incomplete-election-administrations.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml
                                 admin-addresses/validate-addresses]}
                     (sqlite/temp-db "incomplete-election-administrations" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors :election-administrations 3456
                           :incomplete-physical-address]))
      (is (get-in out-ctx [:errors :election-administrations 3456
                           :incomplete-mailing-address]))
      (assert-error-format out-ctx))))

(deftest validate-data-formats
  (let [ctx (merge {:input (xml-input "bad-data-values.xml")
                    :data-specs v3-0/data-specs
                    :pipeline [load-xml]}
                   (sqlite/temp-db "bad-data-values" "3.0"))
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "adds critical errors for missing required fields in candidates"
      (is (get-in out-ctx [:critical :candidates "90001" "name"])))
    (testing "adds errors for values that fail format validation"
      (is (get-in out-ctx [:errors :candidates "90001" "candidate_url"]))
      (is (get-in out-ctx [:errors :candidates "90001" "phone"]))
      (is (get-in out-ctx [:errors :candidates "90001" "email"]))
      (is (get-in out-ctx [:errors :candidates "90001" "sort_order"])))
    (testing "puts errors in the right format"
      (assert-error-format out-ctx))))

(deftest same-element-duplicate-ids-test
  (testing "doesn't die when elements share ids"
    (let [ctx (merge {:input (xml-input "same-element-duplicated-ids.xml")
                      :data-specs v3-0/data-specs
                      :pipeline [load-xml]}
                     (sqlite/temp-db "same-element-duplicated-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:fatal :localities "101" :duplicate-ids]))
      (is (get-in out-ctx [:fatal :precincts "10101" :duplicate-ids]))
      (is (get-in out-ctx [:fatal :precincts "10103" :duplicate-ids]))
      (testing "but still loads some data"
        (is (= [{:id 103}]
               (korma/select (get-in out-ctx [:tables :localities])
                             (korma/fields :id)))))
      (assert-error-format out-ctx))))

(deftest determine-spec-version-test
  (testing "finds and assocs the schemaVersion of the xml feed"
    (let [ctx {:input (xml-input "full-good-run.xml")}
          out-ctx (determine-spec-version ctx)]
      (is (= "3.0" (get out-ctx :spec-version))))))

(deftest branch-on-spec-version-test
  (testing "adds the 3.0 import pipeline to the front of the pipeline for 3.0 feeds"
    (let [ctx {:spec-version "3.0"}
          out-ctx (branch-on-spec-version ctx)
          v3-pipeline (get version-pipelines "3.0")]
      (is (= v3-pipeline
             (take (count v3-pipeline) (:pipeline out-ctx))))))
  (testing "adds the 5.1 import pipeline to the front of the pipeline for 5.1 feeds"
    (let [ctx {:spec-version "5.1"
               :pipeline [branch-on-spec-version]}
          out-ctx (branch-on-spec-version ctx)
          v5-pipeline (get version-pipelines "5.1")]
      (is (= v5-pipeline
             (take (count v5-pipeline) (:pipeline out-ctx))))))
  (testing "stops with unsupported version for other versions"
    (let [ctx {:spec-version "2.0"  ; 2.0 is too old
               :pipeline [branch-on-spec-version]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (.startsWith (:stop out-ctx) "Unsupported XML version")))))

(deftest path-and-values-test
  (let [node (data.xml/element :country
                               {:id "country1"
                                :founded "1788"}
                               (data.xml/element :state
                                                 {:id "state1"}
                                                 "Delaware")
                               (data.xml/element :state
                                                 {:id "state11"}
                                                 "New York"))
        pvs (path-and-values node)]
    (is (= 6 (count pvs)))
    (is (= #{"country.id" "country.founded" "country.state" "country.state.id"}
           (set (map :simple_path pvs))))
    (is (= #{"country.0.id" "country.0.founded"
             "country.0.state.0" "country.0.state.0.id"
             "country.0.state.1" "country.0.state.1.id"}
           (set (map :path pvs))))
    (is (some #{{:path "country.0.state.1.id"
                 :simple_path "country.state.id"
                 :value "state11"
                 :parent_with_id "country.0.state.1"}}
              pvs))))
