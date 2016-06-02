(ns vip.data-processor.db.translations.v5-1.people-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.v5-1.people :as people]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidates-transforms-test
  (testing "person.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/contact_information.txt" "5-1/person.txt"])
               :spec-version "5.1"
               :ltree-index 14
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [people/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values
       out-ctx
       "per50001" "VipObject.0.Person.14.id"
       "ci10861a" "VipObject.0.Person.14.ContactInformation.0.label"
       "1600 Pennsylvania Ave NW" "VipObject.0.Person.14.ContactInformation.0.AddressLine.0"
       "Washington" "VipObject.0.Person.14.ContactInformation.0.AddressLine.1"
       "DC 20006 USA" "VipObject.0.Person.14.ContactInformation.0.AddressLine.2"
       "Head Southeast of the General Rochambeau statue" "VipObject.0.Person.14.ContactInformation.0.Directions.3.Text.0"
       "en" "VipObject.0.Person.14.ContactInformation.0.Directions.3.Text.0.language"
       "38.899" "VipObject.0.Person.14.ContactInformation.0.LatLng.8.Latitude.0"
       "1961-08-04" "VipObject.0.Person.14.DateOfBirth.1"
       "Barak" "VipObject.0.Person.14.FirstName.2"
       "Obama" "VipObject.0.Person.14.LastName.4"
       "The Rock" "VipObject.0.Person.14.Nickname.6"
       "par002" "VipObject.0.Person.14.PartyId.7"
       "Mr. President" "VipObject.0.Person.14.Title.10"))))
