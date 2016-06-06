(ns vip.data-processor.db.translations.v5-1.contact-information-test
  (:require [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.util :as util]
            [clojure.test :refer :all]))

(deftest contact-information->ltree-test
  (let [idx-fn (util/index-generator 27)
          parent-path "VipObject.0.Person.100"
          row {:ci_address_line_1 ""
               :ci_address_line_2 ""
               :ci_address_line_3 ""
               :ci_directions ""
               :ci_email ""
               :ci_fax ""
               :ci_hours ""
               :ci_hours_open_id ""
               :ci_latitude ""
               :ci_longitude ""
               :ci_latlng_source ""
               :ci_name ""
               :ci_phone ""
               :ci_uri ""}
          transform-fn (ci/contact-information->ltree)]
    (testing "omitting all optional elements renders no ContactInformation child"
      (is (empty? (set (transform-fn idx-fn parent-path row)))))

    (testing "at least one field builds a ContactInformation child"
      (let [funky-row (assoc row :ci_address_line_1 "123 Lonely Ln")]
        (is (contains?
             (set (transform-fn idx-fn parent-path funky-row))
             {:path "VipObject.0.Person.100.ContactInformation.27.AddressLine.0"
              :value "123 Lonely Ln"
              :simple_path "VipObject.Person.ContactInformation.AddressLine"
              :parent_with_id "VipObject.0.Person.100.id"})))))

  (testing "can build a whole ContactInformation child"
    (let [idx-fn (util/index-generator 27)
          parent-path "VipObject.0.Person.100"
          row {:ci_address_line_1 "Suite 824"
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
               :ci_uri "http://democracy.works"}
          transform-fn (ci/contact-information->ltree)
          ltree-entries (set (transform-fn idx-fn parent-path row))]
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
                        :parent_with_id "VipObject.0.Person.100.id"})))))
  )
