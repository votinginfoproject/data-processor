(ns vip.data-processor.db.translations.v5-1.departments-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.departments :as departments]
            [vip.data-processor.db.translations.util :as util]))

(deftest department->ltree-test
  (let [idx-fn (util/index-generator 0)
        parent-path "VipObject.0.ElectionAdministration.0"
        departments [{:id "dep01"
                      :election_official_person_id "eloff01"
                      :ci_address_line_1 "Suite 824"
                      :ci_address_line_2 "20 Jay St"
                      :ci_address_line_3 "Brooklyn, NY 11201"
                      :ci_directions "Take the F to York St."
                      :ci_email "democracy@example.com"
                      :ci_fax "555-555-1FAX"
                      :ci_hours "10am to 6pm"
                      :ci_hours_open_id "ho002"
                      :ci_latitude "40.704"
                      :ci_longitude "-73.989"
                      :ci_latlng_source "Sean"
                      :ci_name "Democracy Works"
                      :ci_phone "555-555-5555"
                      :ci_uri "http://democracy.works"}]
        voter-services {"dep01" [{:id "vs01"
                                  :type "overseas-voting"
                                  :other_type nil
                                  :ci_address_line_1 "Suite 824"
                                  :ci_address_line_2 "20 Jay St"
                                  :ci_address_line_3 "Brooklyn, NY 11201"
                                  :ci_directions "Take the F to York St."
                                  :ci_email "democracy@example.com"
                                  :ci_fax "555-555-1FAX"
                                  :ci_hours "10am to 6pm"
                                  :ci_hours_open_id "ho002"
                                  :ci_latitude "40.704"
                                  :ci_longitude "-73.989"
                                  :ci_latlng_source "Sean"
                                  :ci_name "Democracy Works"
                                  :ci_phone "555-555-5555"
                                  :ci_uri "http://democracy.works"}]}
        transform-fn (departments/departments->ltree
                      departments voter-services
                      "VipObject.0.ElectionAdministration.0.id")
        ltree-entries (set (transform-fn idx-fn parent-path nil))]
    (testing "AddressLines come from address_line_{1,2,3}"
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.ContactInformation.0.AddressLine.0"
                      :value "Suite 824"
                      :simple_path "VipObject.ElectionAdministration.Department.ContactInformation.AddressLine"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"}))
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.ContactInformation.0.AddressLine.1"
                      :value "20 Jay St"
                      :simple_path "VipObject.ElectionAdministration.Department.ContactInformation.AddressLine"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"}))
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.ContactInformation.0.AddressLine.2"
                      :value "Brooklyn, NY 11201"
                      :simple_path "VipObject.ElectionAdministration.Department.ContactInformation.AddressLine"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"})))
    (testing "Directions has the ElectionAdministration as the parent_with_id"
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.ContactInformation.0.Directions.3.Text.0"
                      :value "Take the F to York St."
                      :simple_path "VipObject.ElectionAdministration.Department.ContactInformation.Directions.Text"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"})))
    (testing "LatLng has the ElectionAdministration as the parent_with_id"
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.ContactInformation.0.LatLng.8.Latitude.0"
                      :value "40.704"
                      :simple_path "VipObject.ElectionAdministration.Department.ContactInformation.LatLng.Latitude"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"})))

    (testing "Departments include Voter Services"
      (is (contains? ltree-entries
                     {:path "VipObject.0.ElectionAdministration.0.Department.0.VoterService.2.label"
                      :value "vs01"
                      :simple_path "VipObject.ElectionAdministration.Department.VoterService.label"
                      :parent_with_id "VipObject.0.ElectionAdministration.0.id"})))))
