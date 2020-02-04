(ns vip.data-processor.db.translations.v5-1.offices-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.offices :as o]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "office.txt and contact_information.txt are loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths
               (csv-inputs ["5-1/office.txt"
                            "5-1/contact_information.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          o/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (testing "with a contact information"
        (are-xml-tree-values out-ctx
          "off001" "VipObject.0.Office.0.id"
          "ci0827" "VipObject.0.Office.0.ContactInformation.0.label"
          "The White House" "VipObject.0.Office.0.ContactInformation.0.AddressLine.0"
          "1600 Pennsylvania Ave" "VipObject.0.Office.0.ContactInformation.0.AddressLine.1"
          "josh@example.com" "VipObject.0.Office.0.ContactInformation.0.Email.2"
          "Early to very late" "VipObject.0.Office.0.ContactInformation.0.Hours.3.Text.0"
          "en" "VipObject.0.Office.0.ContactInformation.0.Hours.3.Text.0.language"
          "Josh Lyman" "VipObject.0.Office.0.ContactInformation.0.Name.4"

          "ed001" "VipObject.0.Office.0.ElectoralDistrictId.1"
          "true" "VipObject.0.Office.0.IsPartisan.2"
          "Deputy Chief of Staff" "VipObject.0.Office.0.Name.3.Text.0"
          "en" "VipObject.0.Office.0.Name.3.Text.0.language"
          "2002-01-21" "VipObject.0.Office.0.Term.5.StartDate.0"
          "appointed" "VipObject.0.Office.0.Term.5.Type.1"))
      (testing "without a contact information"
        (are-xml-tree-values out-ctx
          "off002" "VipObject.0.Office.1.id"
          "true" "VipObject.0.Office.1.IsPartisan.1"
          "Deputy Deputy Chief of Staff" "VipObject.0.Office.1.Name.2.Text.0"
          "en" "VipObject.0.Office.1.Name.2.Text.0.language"
          "2002-01-21" "VipObject.0.Office.1.Term.4.StartDate.0"
          "appointed" "VipObject.0.Office.1.Term.4.Type.1")))))
