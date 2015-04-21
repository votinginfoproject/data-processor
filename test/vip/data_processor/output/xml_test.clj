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
           "/vip_object/candidate[@id=10000]/name" "Gail H. Timberlake"
           "/vip_object/candidate[@id=10004]/party" "Republican"
           "/vip_object/contest[@id=20000]/partisan" "no"
           "/vip_object/contest[@id=20000]/special" "no"
           "/vip_object/contest[@id=20002]/office" "Member Board of Supervisors"
           "/vip_object/state/name" "Virginia"
           "/vip_object/state/election_administration_id" "40133"
           "/vip_object/street_segment[@id=2000005]/non_house_address/street_name" "Neptune"
           "/vip_object/street_segment[@id=2000005]/odd_even_both" "both"
           "/vip_object/polling_location[@id=80006]/address/location_name" "Huguenot Public Safety Building"
           "/vip_object/polling_location[@id=80006]/polling_hours" "6:00 AM - 7:00 PM"))))
