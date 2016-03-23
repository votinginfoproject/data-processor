(ns vip.data-processor.validation.v5.internationalized-text-test
  (:require [vip.data-processor.validation.v5.internationalized-text :as v5.int-text]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-texts-test
  (let [ctx {:input (xml-input "v5-internationalized-text.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.int-text/validate-no-missing-texts)]
    (testing "Text present is OK"
      (are [element-path] (is (not (get-in out-ctx
                                           [:errors :internationalized-text
                                            element-path :missing])))
        "VipObject.0.BallotMeasureContest.0.BallotTitle.0.Text"
        "VipObject.0.BallotMeasureContest.0.BallotSubTitle.1.Text"
        "VipObject.0.BallotMeasureContest.0.ConStatement.2.Text"
        "VipObject.0.BallotMeasureContest.0.EffectOfAbstain.3.Text"
        "VipObject.0.BallotMeasureContest.0.ProStatement.4.Text"
        "VipObject.0.BallotMeasureContest.0.ElectorateSpecification.5.Text"
        "VipObject.0.BallotMeasureContest.0.FullText.6.Text"
        "VipObject.0.BallotMeasureContest.0.PassageThreshold.7.Text"
        "VipObject.0.BallotMeasureContest.0.SummaryText.8.Text"
        "VipObject.0.BallotMeasureSelection.1.Selection.0.Text"
        "VipObject.0.Candidate.2.BallotName.0.Text"
        "VipObject.0.CandidateContest.3.BallotTitle.0.Text"
        "VipObject.0.CandidateContest.3.BallotSubTitle.1.Text"
        "VipObject.0.CandidateContest.3.ElectorateSpecification.2.Text"
        "VipObject.0.Contest.4.BallotTitle.0.Text"
        "VipObject.0.Contest.4.BallotSubTitle.1.Text"
        "VipObject.0.Contest.4.ElectorateSpecification.2.Text"
        "VipObject.0.Election.5.AbsenteeBallotInfo.0.Text"
        "VipObject.0.Election.5.ElectionType.1.Text"
        "VipObject.0.Election.5.Name.2.Text"
        "VipObject.0.Election.5.PollingHours.3.Text"
        "VipObject.0.Election.5.RegistrationInfo.4.Text"
        "VipObject.0.ElectionAdministration.6.Department.0.ContactInformation.0.Hours.0.Text"
        "VipObject.0.ElectionAdministration.6.Department.0.VoterService.1.ContactInformation.0.Hours.0.Text"
        "VipObject.0.ElectionAdministration.6.Department.0.VoterService.1.Description.1.Text"
        "VipObject.0.Office.7.Name.0.Text"
        "VipObject.0.Office.7.ContactInformation.1.Hours.0.Text"
        "VipObject.0.Party.8.Name.0.Text"
        "VipObject.0.PartyContest.9.BallotTitle.0.Text"
        "VipObject.0.PartyContest.9.BallotSubTitle.1.Text"
        "VipObject.0.PartyContest.9.ElectorateSpecification.2.Text"
        "VipObject.0.Person.10.ContactInformation.0.Hours.0.Text"
        "VipObject.0.Person.10.FullName.1.Text"
        "VipObject.0.Person.10.Profession.2.Text"
        "VipObject.0.Person.10.Title.3.Text"
        "VipObject.0.PollingLocation.11.Directions.0.Text"
        "VipObject.0.PollingLocation.11.Hours.1.Text"
        "VipObject.0.RetentionContest.12.BallotTitle.0.Text"
        "VipObject.0.RetentionContest.12.BallotSubTitle.1.Text"
        "VipObject.0.RetentionContest.12.ConStatement.2.Text"
        "VipObject.0.RetentionContest.12.EffectOfAbstain.3.Text"
        "VipObject.0.RetentionContest.12.ProStatement.4.Text"
        "VipObject.0.RetentionContest.12.ElectorateSpecification.5.Text"
        "VipObject.0.RetentionContest.12.FullText.6.Text"
        "VipObject.0.RetentionContest.12.PassageThreshold.7.Text"
        "VipObject.0.RetentionContest.12.SummaryText.8.Text"
        "VipObject.0.Source.13.Description.0.Text"
        "VipObject.0.Source.13.FeedContactInformation.1.Hours.0.Text"))
    (testing "Text missing is an error"
      (are [element-path] (is (get-in out-ctx
                                      [:errors :internationalized-text
                                       element-path :missing]))
        "VipObject.0.BallotMeasureContest.14.BallotTitle.0.Text"
        "VipObject.0.BallotMeasureContest.14.BallotSubTitle.1.Text"
        "VipObject.0.BallotMeasureContest.14.ConStatement.2.Text"
        "VipObject.0.BallotMeasureContest.14.EffectOfAbstain.3.Text"
        "VipObject.0.BallotMeasureContest.14.ProStatement.4.Text"
        "VipObject.0.BallotMeasureContest.14.ElectorateSpecification.5.Text"
        "VipObject.0.BallotMeasureContest.14.FullText.6.Text"
        "VipObject.0.BallotMeasureContest.14.PassageThreshold.7.Text"
        "VipObject.0.BallotMeasureContest.14.SummaryText.8.Text"
        "VipObject.0.BallotMeasureSelection.15.Selection.0.Text"
        "VipObject.0.Candidate.16.BallotName.0.Text"
        "VipObject.0.CandidateContest.17.BallotTitle.0.Text"
        "VipObject.0.CandidateContest.17.BallotSubTitle.1.Text"
        "VipObject.0.CandidateContest.17.ElectorateSpecification.2.Text"
        "VipObject.0.Contest.18.BallotTitle.0.Text"
        "VipObject.0.Contest.18.BallotSubTitle.1.Text"
        "VipObject.0.Contest.18.ElectorateSpecification.2.Text"
        "VipObject.0.Election.19.AbsenteeBallotInfo.0.Text"
        "VipObject.0.Election.19.ElectionType.1.Text"
        "VipObject.0.Election.19.Name.2.Text"
        "VipObject.0.Election.19.PollingHours.3.Text"
        "VipObject.0.Election.19.RegistrationInfo.4.Text"
        "VipObject.0.ElectionAdministration.20.Department.0.ContactInformation.0.Hours.0.Text"
        "VipObject.0.ElectionAdministration.20.Department.0.VoterService.1.ContactInformation.0.Hours.0.Text"
        "VipObject.0.ElectionAdministration.20.Department.0.VoterService.1.Description.1.Text"
        "VipObject.0.Office.21.Name.0.Text"
        "VipObject.0.Office.21.ContactInformation.1.Hours.0.Text"
        "VipObject.0.Party.22.Name.0.Text"
        "VipObject.0.PartyContest.23.BallotTitle.0.Text"
        "VipObject.0.PartyContest.23.BallotSubTitle.1.Text"
        "VipObject.0.PartyContest.23.ElectorateSpecification.2.Text"
        "VipObject.0.Person.24.ContactInformation.0.Hours.0.Text"
        "VipObject.0.Person.24.FullName.1.Text"
        "VipObject.0.Person.24.Profession.2.Text"
        "VipObject.0.Person.24.Title.3.Text"
        "VipObject.0.PollingLocation.25.Directions.0.Text"
        "VipObject.0.PollingLocation.25.Hours.1.Text"
        "VipObject.0.RetentionContest.26.BallotTitle.0.Text"
        "VipObject.0.RetentionContest.26.BallotSubTitle.1.Text"
        "VipObject.0.RetentionContest.26.ConStatement.2.Text"
        "VipObject.0.RetentionContest.26.EffectOfAbstain.3.Text"
        "VipObject.0.RetentionContest.26.ProStatement.4.Text"
        "VipObject.0.RetentionContest.26.ElectorateSpecification.5.Text"
        "VipObject.0.RetentionContest.26.FullText.6.Text"
        "VipObject.0.RetentionContest.26.PassageThreshold.7.Text"
        "VipObject.0.RetentionContest.26.SummaryText.8.Text"
        "VipObject.0.Source.27.Description.0.Text"
        "VipObject.0.Source.27.FeedContactInformation.1.Hours.0.Text"))))
