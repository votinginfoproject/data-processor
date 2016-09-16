(ns vip.data-processor.db.translations.v5-1.election-administrations-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (let [errors-chan (a/chan 100)
        ctx {:input (csv-inputs ["5-1/contact_information.txt"
                                 "5-1/department.txt"
                                 "5-1/voter_service.txt"
                                 "5-1/election_administration.txt"])
             :errors-chan errors-chan
             :spec-version (atom "5.1")
             :pipeline (concat
                        [postgres/start-run
                         (data-spec/add-data-specs v5-1/data-specs)]
                        (get csv/version-pipelines "5.1"))}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems-2 errors {})

    (testing "election_administration.txt is loaded and transformed"
      (are-xml-tree-values
       out-ctx
       "ea123" "VipObject.0.ElectionAdministration.0.id"
       "https://example.com/absentee" "VipObject.0.ElectionAdministration.0.AbsenteeUri.0"
       "https://example.com/am-i-registered" "VipObject.0.ElectionAdministration.0.AmIRegisteredUri.1"

       ;; Department starts with a ContactInformation block
       "ci2048" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.label"
       "Argyle Annex" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.AddressLine.0"
       "Argyle Pl" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.AddressLine.1"
       "argyle@example.com" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.Email.4"
       "sunup to sundown" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.Hours.6.Text.0"
       "en" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.Hours.6.Text.0.language"
       "Back porch coworking" "VipObject.0.ElectionAdministration.0.Department.2.ContactInformation.0.Name.7"

       ;; One more part of the root Department type...
       "eloff01" "VipObject.0.ElectionAdministration.0.Department.2.ElectionOfficialPersonId.1"

       ;; ...and now into the VoterService block
       ;; with another ContactInformation block
       "ci0828" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.label"
       "The White House" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.AddressLine.0"
       "1600 Pennsylvania Ave" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.AddressLine.1"
       "josh@example.com" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.Email.2"
       "Early to very late" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.Hours.3.Text.0"
       "en" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.Hours.3.Text.0.language"
       "Josh Lyman" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ContactInformation.0.Name.4"

       ;; ...and now we're back to the other parts of the voter service block
       "A service we provide" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.Description.1.Text.0"
       "en"  "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.Description.1.Text.0.language"
       "eloff02" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.ElectionOfficialPersonId.2"
       "overseas-voting" "VipObject.0.ElectionAdministration.0.Department.2.VoterService.2.Type.3"

       ;; ...and now we're back to the ElectionAdministration block
       "https://example.com/elections" "VipObject.0.ElectionAdministration.0.ElectionsUri.3"
       "https://example.com/registration" "VipObject.0.ElectionAdministration.0.RegistrationUri.4"
       "https://example.com/rules" "VipObject.0.ElectionAdministration.0.RulesUri.5"
       "https://example.com/what-is-on-my-ballot" "VipObject.0.ElectionAdministration.0.WhatIsOnMyBallotUri.6"
       "https://example.com/where-do-i-vote" "VipObject.0.ElectionAdministration.0.WhereDoIVoteUri.7"))

    (testing "an election administration may have multiple departments"
      (are-xml-tree-values
       out-ctx
       "ea625" "VipObject.0.ElectionAdministration.2.id"
       "Curated Voting Consortium" "VipObject.0.ElectionAdministration.2.Department.2.ContactInformation.0.Name.3"
       "Pencil sharpening" "VipObject.0.ElectionAdministration.2.Department.2.VoterService.2.Description.1.Text.0"
       "Guided hike to polling place" "VipObject.0.ElectionAdministration.2.Department.2.VoterService.3.Description.1.Text.0"
       "Bike messenger ballot delivery" "VipObject.0.ElectionAdministration.2.Department.2.VoterService.4.Description.1.Text.0"
       "Voter Fraud Foundation" "VipObject.0.ElectionAdministration.2.Department.3.ContactInformation.0.Name.5"))))
