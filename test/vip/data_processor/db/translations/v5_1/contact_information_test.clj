(ns vip.data-processor.db.translations.v5-1.contact-information-test
  (:require [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.util :as util]
            [clojure.test :refer :all]))

(deftest contact-information->ltree-test
  (testing "can build a whole ContactInformation child"
    (let [idx-fn (util/index-generator 27)
          parent-path "VipObject.0.Person.100"
          row {:address_line_1 "Suite 824"
               :address_line_2 "20 Jay St"
               :address_line_3 "Brooklyn, NY 11201"
               :directions "Take the F to York St."
               :email "democracy@example.com"
               :fax "555-555-1FAX"
               :hours "10am to 6pm"
               :hours_open_id "ho002"
               :latitude "40.704"
               :longitude "-73.989"
               :latlng_source "Sean"
               :name "Democracy Works"
               :phone "555-555-5555"
               :uri "http://democracy.works"}
          ltree-entries (set (ci/contact-information->ltree idx-fn parent-path row))]
      (testing "AddressLines come from address_line_{1,2,3}"
        (is (contains? ltree-entries
                       {:path "VipObject.0.Person.100.ContactInformation.27.AddressLine.0"
                        :value "Suite 824"
                        :simple_path "VipObject.Person.ContactInformation.AddressLine"
                        :parent_with_id "VipObject.0.Person.100.id"}))
        (is (contains? ltree-entries
                       {:path "VipObject.0.Person.100.ContactInformation.27.AddressLine.1"
                        :value "20 Jay St"
                        :simple_path "VipObject.Person.ContactInformation.AddressLine"
                        :parent_with_id "VipObject.0.Person.100.id"}))
        (is (contains? ltree-entries
                       {:path "VipObject.0.Person.100.ContactInformation.27.AddressLine.2"
                        :value "Brooklyn, NY 11201"
                        :simple_path "VipObject.Person.ContactInformation.AddressLine"
                        :parent_with_id "VipObject.0.Person.100.id"})))
      (testing "Diretions has the Person as the parent_with_id"
        (is (contains? ltree-entries
                       {:path "VipObject.0.Person.100.ContactInformation.27.Directions.3.Text.0"
                        :value "Take the F to York St."
                        :simple_path "VipObject.Person.ContactInformation.Directions.Text"
                        :parent_with_id "VipObject.0.Person.100.id"})))
      (testing "LatLng has the Person as the parent_with_id"
        (is (contains? ltree-entries
                       {:path "VipObject.0.Person.100.ContactInformation.27.LatLng.8.Latitude.0"
                        :value "40.704"
                        :simple_path "VipObject.Person.ContactInformation.LatLng.Latitude"
                        :parent_with_id "VipObject.0.Person.100.id"}))))))
