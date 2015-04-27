(ns vip.data-processor.output.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.output.xml :refer :all]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.transforms :as transforms]
            [clojure.java.io :as io]
            [clojure.xml :as xml]
            [clj-xpath.core :as xpath])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(deftest create-xml-file-test
  (testing "adds an :xml-output-file key to a context"
    (let [ctx {:filename "create-xml-test"}
          out-ctx (create-xml-file ctx)]
      (is (:xml-output-file out-ctx)))))

(deftest write-xml-test
  (testing "writes XML to :xml-output-file generated from :xml-children"
    (let [temp-file (Files/createTempFile "write-xml-test" ".xml" (into-array FileAttribute []))
          xml-children [{:tag :foo :attrs {:bar "baz"} :content ["test"]}]
          ctx {:xml-output-file temp-file
               :xml-children xml-children}
          out-ctx (write-xml ctx)
          xml (xml/parse (.toString temp-file))]
      (is (= :vip_object (:tag xml)))
      (is (= vip-object-attrs (:attrs xml)))
      (is (= xml-children (:content xml)))))
  (testing "generates XML from an import"
    (let [db (sqlite/temp-db "xml-output")
          filenames (->> csv/csv-filenames
                         (map #(io/as-file (io/resource (str "csv/full-good-run/" %))))
                         (remove nil?))
          ctx (merge {:input filenames
                      :pipeline (concat [(data-spec/add-data-specs
                                          data-spec/data-specs)]
                                        transforms/csv-validations
                                        pipeline)} db)
          results-ctx (pipeline/run-pipeline ctx)
          xml-doc (-> results-ctx
                      :xml-output-file
                      .toString
                      slurp
                      xpath/xml->doc)]
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
           "/vip_object/referendum[@id=86702]/title" "More Cats"
           "/vip_object/election/election_type" "State"
           "/vip_object/election/statewide" "yes"
           "/vip_object/election/registration_deadline" "2015-01-27"
           "/vip_object/election_official[@id=50122]/title" "General Registrar Physical"
           "/vip_object/election_official[@id=50122]/phone" "(757) 393-8644"
           "/vip_object/electoral_district[@id=60001]/name" "POWHATAN COUNTY"
           "/vip_object/precinct[@id=90000]/name" "034- THIRTY-FOUR"
           "/vip_object/precinct[@id=90000]/mail_only" "yes"
           "/vip_object/precinct[@id=90000]/polling_location_id" "80016"
           "/vip_object/precinct[@id=90000]/electoral_district_id[1]" "60000"
           "/vip_object/precinct[@id=90000]/electoral_district_id[2]" "60002"
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
      (is (= "certified" (:certification (xpath/$x:attrs "/vip_object/ballot_line_result" xml-doc)))))))
