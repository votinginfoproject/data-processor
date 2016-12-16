(ns vip.data-processor.validation.v5.boolean-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :refer :all]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.boolean :as v5.boolean]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(set! *print-length* 100)

(deftest ^:postgres boolean-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-incorrect-booleans.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Election.7.IsStatewide.5",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Office.8.IsPartisan.2",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Precinct.9.IsMailOnly.4",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Election.7.HasElectionDayRegistration.3",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Candidate.11.IsTopTicket.4",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Candidate.11.IsIncumbent.3",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.BallotMeasureContest.6.HasRotation.8",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.RetentionContest.5.HasRotation.6",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.CandidateSelection.4.IsWriteIn.2",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.HoursOpen.3.Schedule.0.IsOnlyByAppointment.4",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.HoursOpen.3.Schedule.0.IsOrByAppointment.5",
                            :error-type :format
                            :error-value "False"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.HoursOpen.3.Schedule.0.IsSubjectToChange.6",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.StreetSegment.2.IncludesAllAddresses.1",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.StreetSegment.2.IncludesAllStreets.2",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.CandidateContest.1.HasRotation.7",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.PollingLocation.0.IsEarlyVoting.5",
                            :error-type :format
                            :error-value "True"}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.PollingLocation.0.IsDropBox.4",
                            :error-type :format
                            :error-value "True"})))))
