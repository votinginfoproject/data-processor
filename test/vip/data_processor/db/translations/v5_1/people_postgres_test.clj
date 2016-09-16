(ns vip.data-processor.db.translations.v5-1.people-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidates-transforms-test
  (testing "person.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/contact_information.txt" "5-1/person.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems-2 errors {})
      (are-xml-tree-values
       out-ctx
       "per50001" "VipObject.0.Person.0.id"
       "ci10861a" "VipObject.0.Person.0.ContactInformation.0.label"
       "1600 Pennsylvania Ave NW" "VipObject.0.Person.0.ContactInformation.0.AddressLine.0"
       "Washington" "VipObject.0.Person.0.ContactInformation.0.AddressLine.1"
       "DC 20006 USA" "VipObject.0.Person.0.ContactInformation.0.AddressLine.2"
       "Head Southeast of the General Rochambeau statue" "VipObject.0.Person.0.ContactInformation.0.Directions.3.Text.0"
       "en" "VipObject.0.Person.0.ContactInformation.0.Directions.3.Text.0.language"
       "38.899" "VipObject.0.Person.0.ContactInformation.0.LatLng.8.Latitude.0"
       "1961-08-04" "VipObject.0.Person.0.DateOfBirth.1"
       "Barak" "VipObject.0.Person.0.FirstName.2"
       "Obama" "VipObject.0.Person.0.LastName.4"
       "The Rock" "VipObject.0.Person.0.Nickname.6"
       "par002" "VipObject.0.Person.0.PartyId.7"
       "Mr. President" "VipObject.0.Person.0.Title.10.Text.0"
       "en" "VipObject.0.Person.0.Title.10.Text.0.language"))))
