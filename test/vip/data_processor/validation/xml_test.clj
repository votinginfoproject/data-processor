(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [clojure.core.async :as a]
            [clojure.data.xml :as data.xml]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.db.v3-0.admin-addresses :as admin-addresses]
            [vip.data-processor.validation.db.v3-0.jurisdiction-references :as jurisdiction-references]
            [vip.data-processor.validation.db.v3-0.street-segment :as street-segment]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]
            [vip.data-processor.errors :as errors]))

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
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "non-utf-8.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml]}
                     (sqlite/temp-db "non-utf-8" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :candidates
                            :identifier "90001"
                            :error-type "name"
                            :error-value "Is not valid UTF-8."}))))
  (testing "can continue after reaching malformed XML"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "malformed.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml]}
                     (sqlite/temp-db "malformed-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :critical
                            :scope :import
                            :identifier :global
                            :error-type :malformed-xml}))
      (testing "but still loads data before the malformation!"
        (is (= [{:id 39 :name "Ohio" :election_administration_id 3456}]
               (korma/select (get-in out-ctx [:tables :states]))))))))

(deftest full-good-run-test
  (testing "a good XML file produces no erorrs or warnings"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "full-good-run.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :spec-version (atom nil)
                      :pipeline (concat [determine-spec-version
                                         branch-on-spec-version]
                                        db/validations)}
                     (sqlite/temp-db "full-good-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (nil? (:stop out-ctx)))
      (is (nil? (:exception out-ctx)))
      (assert-no-problems-2 errors {})
      (testing "inserts values for columns not in the first element of a type"
        (let [mail-only-precinct (first
                                  (korma/select (get-in out-ctx [:tables :precincts])
                                                (korma/where {:id 10203})))]
          (is (= 1 (:mail_only mail-only-precinct))))))))

(deftest validate-no-duplicated-ids-test
  (testing "returns an error when there is a duplicated id"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "duplicated-ids.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml db/validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicated-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)
          duplicate-id-errors (matching-errors errors
                                               {:severity :errors
                                                :scope :import
                                                :identifier 101
                                                :error-type :duplicate-ids})]
      (is (= #{"precincts" "localities"}
             (apply set (map :error-value duplicate-id-errors)))))))

(deftest validate-no-duplicated-rows-test
  (testing "returns a warning if two nodes have the same data"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "duplicated-rows.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml db/validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicated-rows" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (doseq [id [900101 900102 900103]]
        (is (contains-error? errors {:severity :warnings
                                     :scope :candidates
                                     :identifier id
                                     :error-type :duplicate-rows})))
      (testing "except for ballots"
        (doseq [id [80000 80001]]
          (is (nil? (contains-error? errors {:severity :warnings
                                             :scope :ballots
                                             :identifier id
                                             :error-type :duplicate-rows}))))))))

(deftest validate-references-test
  (testing "returns an error if there are unreferenced objects"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "unreferenced-ids.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml db/validate-references]}
                     (sqlite/temp-db "unreferenced-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :errors
                                   :scope :localities
                                   :identifier 101
                                   :error-type :unmatched-reference
                                   :error-value {"state_id" 99}})))))

(deftest validate-jurisdiction-references-test
  (testing "returns an error if there are unreferenced jurisdiction references"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "unreferenced-jurisdictions.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml jurisdiction-references/validate-jurisdiction-references]}
                     (sqlite/temp-db "unreferenced-jurisdictions" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :errors
                                   :scope :ballot-line-results
                                   :identifier 91008
                                   :error-type :unmatched-reference
                                   :error-value {:jurisdiction_id 99999}})))))

(deftest validate-one-record-limit-test
  (testing "returns an error if particular nodes are duplicated more than once"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "one-record-limit.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml db/validate-one-record-limit]}
                     (sqlite/temp-db "one-record-limit" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :errors
                                   :scope :elections
                                   :identifier :global
                                   :error-type :row-constraint
                                   :error-value "File needs to contain exactly one row."}))
      (is (contains-error? errors {:severity :errors
                                   :scope :sources
                                   :identifier :global
                                   :error-type :row-constraint
                                   :error-value "File needs to contain exactly one row."})))))

(deftest validate-no-unreferenced-rows
  (testing "returns a warning if it finds rows that are unreferenced"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "unreferenced-rows.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml db/validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :warnings
                                   :scope :candidates
                                   :identifier 90000
                                   :error-type :unreferenced-row})))))

(deftest validate-no-overlapping-street-segments
  (testing "returns an error if street segments overlap"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "overlapping-street-segments.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml street-segment/validate-no-overlapping-street-segments]}
                     (sqlite/temp-db "overlapping-street-segments" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segments
                            :identifier 1210002
                            :error-type :overlaps
                            :error-value 1210003})))))

(deftest validate-election-administration-addresses
  (testing "returns an error if either the physical or mailing address is incomplete"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "incomplete-election-administrations.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml
                                 admin-addresses/validate-addresses]}
                     (sqlite/temp-db "incomplete-election-administrations" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administrations
                            :identifier 3456
                            :error-type :incomplete-physical-address}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :election-administrations
                            :identifier 3456
                            :error-type :incomplete-mailing-address})))))

(deftest validate-data-formats
  (let [errors-chan (a/chan 100)
        ctx (merge {:input (xml-input "bad-data-values.xml")
                    :data-specs v3-0/data-specs
                    :errors-chan errors-chan
                    :pipeline [load-xml]}
                   (sqlite/temp-db "bad-data-values" "3.0"))
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "adds critical errors for missing required fields in candidates"
      (is (contains-error? errors {:severity :critical
                                   :scope :candidates
                                   :identifier "90001"
                                   :error-type "name"})))
    (testing "adds errors for values that fail format validation"
      (is (contains-error? errors {:severity :errors
                                   :scope :candidates
                                   :identifier "90001"
                                   :error-type "candidate_url"}))
      (is (contains-error? errors {:severity :errors
                                   :scope :candidates
                                   :identifier "90001"
                                   :error-type "phone"}))
      (is (contains-error? errors {:severity :errors
                                   :scope :candidates
                                   :identifier "90001"
                                   :error-type "email"}))
      (is (contains-error? errors {:severity :fatal
                                   :scope :candidates
                                   :identifier "90001"
                                   :error-type "sort_order"})))))

(deftest same-element-duplicate-ids-test
  (testing "doesn't die when elements share ids"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "same-element-duplicated-ids.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [load-xml]}
                     (sqlite/temp-db "same-element-duplicated-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :localities
                            :identifier "101"
                            :error-type :duplicate-ids}))
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :precincts
                            :identifier "10101"
                            :error-type :duplicate-ids}))
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :precincts
                            :identifier "10103"
                            :error-type :duplicate-ids}))
      (testing "but still loads some data"
        (is (= [{:id 103}]
               (korma/select (get-in out-ctx [:tables :localities])
                             (korma/fields :id))))))))

(deftest determine-spec-version-test
  (testing "finds and assocs the schemaVersion of the xml feed"
    (let [ctx {:input (xml-input "full-good-run.xml")
               :spec-version (atom nil)}
          out-ctx (determine-spec-version ctx)]
      (is (= "3.0" @(get out-ctx :spec-version))))))

(deftest branch-on-spec-version-test
  (testing "adds the 3.0 import pipeline to the front of the pipeline for 3.0 feeds"
    (let [ctx {:spec-version (atom "3.0")}
          out-ctx (branch-on-spec-version ctx)
          v3-pipeline (get version-pipelines "3.0")]
      (is (= v3-pipeline
             (take (count v3-pipeline) (:pipeline out-ctx))))))
  (testing "adds the 5.1 import pipeline to the front of the pipeline for 5.1 feeds"
    (let [ctx {:spec-version (atom "5.1")
               :pipeline [branch-on-spec-version]}
          out-ctx (branch-on-spec-version ctx)
          v5-pipeline (get version-pipelines "5.1")]
      (is (= v5-pipeline
             (take (count v5-pipeline) (:pipeline out-ctx))))))
  (testing "stops with unsupported version for other versions"
    (let [ctx {:spec-version (atom "2.0")   ; 2.0 is too old
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
