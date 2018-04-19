(ns vip.data-processor.output.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.output.xml :refer :all]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.transforms :as transforms]
            [clojure.java.io :as io]
            [clojure.xml :as xml]
            [clj-xpath.core :as xpath]
            [clojure.core.async :as a])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(deftest write-xml-test
  (testing "generates XML from an import"
    (let [db (sqlite/temp-db "xml-output" "3.0")
          filenames (->> v3-0/data-specs
                         (map
                          #(io/as-file (io/resource (str "csv/full-good-run/" (:filename %)))))
                         (remove nil?))
          errors-chan (a/chan 100)
          ctx (merge {:input filenames
                      :spec-version (atom "3.0")
                      :errors-chan errors-chan
                      :pipeline (concat [(data-spec/add-data-specs
                                          v3-0/data-specs)
                                         csv/error-on-missing-files
                                         csv/determine-spec-version
                                         csv/remove-bad-filenames
                                         sqlite/attach-sqlite-db
                                         (csv-files/validate-dependencies
                                          csv-files/v3-0-file-dependencies)
                                         csv/load-csvs]
                                        pipeline)} db)
          results-ctx (pipeline/run-pipeline ctx)
          xml-doc (-> results-ctx
                      :xml-output-file
                      .toString
                      slurp
                      xpath/xml->doc)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are [path text] (= text (xpath/$x:text path xml-doc))
           "/vip_object/ballot[@id=200000]/image_url" "http://i.imgur.com/PMgfJSm.jpg"
           "/vip_object/ballot[@id=200000]/candidate_id[@sort_order=3]" "10000"
           "/vip_object/ballot[@id=200000]/candidate_id[@sort_order=1]" "10004"
           "/vip_object/ballot_line_result/votes" "81"
           "/vip_object/ballot_line_result/entire_district" "no"
           "/vip_object/candidate[@id=10000]/name" "Gail H. Timberlake"
           "/vip_object/candidate[@id=10004]/party" "Republican"
           "/vip_object/contest[@id=20000]/partisan" "no"
           "/vip_object/contest[@id=20000]/special" "no"
           "/vip_object/contest[@id=20002]/office" "Member Board of Supervisors"
           "/vip_object/contest_result[@id=867002]/total_votes" "300"
           "/vip_object/contest_result[@id=867002]/blank_votes" "2"
           "/vip_object/custom_ballot[@id=765001]/heading" "Should Judge Judy be retained?"
           "/vip_object/custom_ballot[@id=765001]/ballot_response_id[@sort_order=1]" "87701"
           "/vip_object/custom_ballot[@id=765001]/ballot_response_id[@sort_order=2]" "87702"
           "/vip_object/referendum[@id=86702]/title" "More Cats"
           "/vip_object/election/election_type" "State"
           "/vip_object/election/statewide" "yes"
           "/vip_object/election/registration_deadline" "2015-01-27"
           "/vip_object/election_administration[@id=40122]/name" "CITY OF PORTSMOUTH"
           "/vip_object/election_administration[@id=40122]/physical_address/line1" "801 CRAWFORD STREET, FIRST FLOOR"
           "/vip_object/election_administration[@id=40122]/registration_url" "http://www.vote.virginia.gov"
           "/vip_object/election_official[@id=50122]/title" "General Registrar Physical"
           "/vip_object/election_official[@id=50122]/phone" "(757) 393-8644"
           "/vip_object/electoral_district[@id=60001]/name" "POWHATAN COUNTY"
           "/vip_object/locality[@id=70069]/name" "POWHATAN COUNTY"
           "/vip_object/locality[@id=70069]/type" "county"
           "/vip_object/locality[@id=70069]/early_vote_site_id" "556001"
           "/vip_object/precinct[@id=90000]/name" "034- THIRTY-FOUR"
           "/vip_object/precinct[@id=90000]/mail_only" "yes"
           "/vip_object/precinct[@id=90000]/polling_location_id" "80016"
           "/vip_object/precinct[@id=90000]/electoral_district_id[1]" "60000"
           "/vip_object/precinct[@id=90000]/electoral_district_id[2]" "60002"
           "/vip_object/precinct_split[@id=100042]/name" "Tobaccoville"
           "/vip_object/precinct_split[@id=100042]/electoral_district_id" "60001"
           "/vip_object/referendum[@id=86702]/ballot_response_id[@sort_order=1]" "87703"
           "/vip_object/referendum[@id=86702]/ballot_response_id[@sort_order=2]" "87704"
           "/vip_object/source/name" "Department of Elections, Commonwealth of Virginia"
           "/vip_object/source/datetime" "2015-01-21T18:13:25"
           "/vip_object/state/name" "Virginia"
           "/vip_object/state/election_administration_id" "40133"
           "/vip_object/street_segment[@id=2000005]/non_house_address/street_name" "Neptune"
           "/vip_object/street_segment[@id=2000005]/odd_even_both" "both"
           "/vip_object/polling_location[@id=80006]/address/location_name" "Huguenot Public Safety Building"
           "/vip_object/polling_location[@id=80006]/polling_hours" "6:00 AM - 7:00 PM")
      (is (= "certified" (:certification (xpath/$x:attrs "/vip_object/ballot_line_result" xml-doc))))
      (is (= "unofficial_complete" (:certification (xpath/$x:attrs "/vip_object/contest_result[@id=867001]" xml-doc))))
      (is (= "certified" (:certification (xpath/$x:attrs "/vip_object/contest_result[@id=867002]" xml-doc)))))))

(deftest validate-xml-test
  (testing "good data generates validatable XML"
    (let [good-xml (-> "xml/full-good-run.xml"
                      io/resource
                      io/file
                      .toPath)
          errors-chan (a/chan 100)
          ctx {:xml-output-file good-xml
               :errors-chan errors-chan
               :vip-version "3.0"}
          results-ctx (validate-xml-output ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})))

  (testing "bad XML won't validate, adding to `:errors`"
    (let [errors-chan (a/chan 100)
          bad-xml (-> "xml/malformed.xml"
                      io/resource
                      io/file
                      .toPath)
          ctx {:xml-output-file bad-xml
               :errors-chan errors-chan
               :vip-version "3.0"}
          results-ctx (validate-xml-output ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :xml-generation
                            :identifier :global
                            :error-type :invalid-xml})))))
