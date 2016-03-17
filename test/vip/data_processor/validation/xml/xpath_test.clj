(ns vip.data-processor.validation.xml.xpath-test
  (:require [vip.data-processor.validation.xml.xpath :refer :all]
            [clojure.test :refer :all]))

(deftest type->lqueries-test
  (testing "generates lists of lqueries from a type for a spec"
    (are [type version expected] (= (set (type->lqueries type version)) expected)
      "Locality"     "5.0" #{"VipObject.*{1}.Locality.*{1}"}
      "DistrictType" "5.0" #{"VipObject.*{1}.ElectoralDistrict.*{1}.Type.*{1}"
                             "VipObject.*{1}.Locality.*{1}.Type.*{1}"}
      "xs:date"      "3.0" #{"vip_object.*{1}.contest.*{1}.filing_closed_date.*{1}"
                             "vip_object.*{1}.early_vote_site.*{1}.end_date.*{1}"
                             "vip_object.*{1}.early_vote_site.*{1}.start_date.*{1}"
                             "vip_object.*{1}.election.*{1}.absentee_request_deadline.*{1}"
                             "vip_object.*{1}.election.*{1}.date.*{1}"
                             "vip_object.*{1}.election.*{1}.registration_deadline.*{1}"}
      ;; The big one
      "InternationalizedText" "5.0" #{"VipObject.*{1}.BallotMeasureContest.*{1}.ConStatement.*{1}"
                                      "VipObject.*{1}.BallotMeasureContest.*{1}.EffectOfAbstain.*{1}"
                                      "VipObject.*{1}.BallotMeasureContest.*{1}.FullText.*{1}"
                                      "VipObject.*{1}.BallotMeasureContest.*{1}.PassageThreshold.*{1}"
                                      "VipObject.*{1}.BallotMeasureContest.*{1}.ProStatement.*{1}"
                                      "VipObject.*{1}.BallotMeasureContest.*{1}.SummaryText.*{1}"
                                      "VipObject.*{1}.BallotMeasureSelection.*{1}.Selection.*{1}"
                                      "VipObject.*{1}.Candidate.*{1}.BallotName.*{1}"
                                      "VipObject.*{1}.Contest.*{1}.BallotSubTitle.*{1}"
                                      "VipObject.*{1}.Contest.*{1}.BallotTitle.*{1}"
                                      "VipObject.*{1}.Contest.*{1}.ElectorateSpecification.*{1}"
                                      "VipObject.*{1}.Election.*{1}.AbsenteeBallotInfo.*{1}"
                                      "VipObject.*{1}.Election.*{1}.ElectionType.*{1}"
                                      "VipObject.*{1}.Election.*{1}.Name.*{1}"
                                      "VipObject.*{1}.Election.*{1}.PollingHours.*{1}"
                                      "VipObject.*{1}.Election.*{1}.RegistrationInfo.*{1}"
                                      "VipObject.*{1}.ElectionAdministration.*{1}.Department.*{1}.ContactInformation.*{1}.Hours.*{1}"
                                      "VipObject.*{1}.ElectionAdministration.*{1}.Department.*{1}.VoterService.*{1}.ContactInformation.*{1}.Hours.*{1}"
                                      "VipObject.*{1}.ElectionAdministration.*{1}.Department.*{1}.VoterService.*{1}.Description.*{1}"
                                      "VipObject.*{1}.Office.*{1}.ContactInformation.*{1}.Hours.*{1}"
                                      "VipObject.*{1}.Office.*{1}.Name.*{1}"
                                      "VipObject.*{1}.Party.*{1}.Name.*{1}"
                                      "VipObject.*{1}.Person.*{1}.ContactInformation.*{1}.Hours.*{1}"
                                      "VipObject.*{1}.Person.*{1}.FullName.*{1}"
                                      "VipObject.*{1}.Person.*{1}.Profession.*{1}"
                                      "VipObject.*{1}.Person.*{1}.Title.*{1}"
                                      "VipObject.*{1}.PollingLocation.*{1}.Directions.*{1}"
                                      "VipObject.*{1}.PollingLocation.*{1}.Hours.*{1}"
                                      "VipObject.*{1}.Source.*{1}.Description.*{1}"
                                      "VipObject.*{1}.Source.*{1}.FeedContactInformation.*{1}.Hours.*{1}"})))
