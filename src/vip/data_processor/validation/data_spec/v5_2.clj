(ns vip.data-processor.validation.data-spec.v5-2
  (:require [vip.data-processor.validation.data-spec.value-format :as format]
            [vip.data-processor.validation.data-spec.coerce :as coerce]
            [vip.data-processor.validation.data-spec.translate :as translate]))

(def street-segments
  {:columns [{:name "id"}
             {:name "includes_all_addresses"
              :coerce coerce/postgres-boolean}
             {:name "address_direction"}
             {:name "city"}
             {:name "odd_even_both"}
             {:name "precinct_id"}
             {:name "start_house_number"
              :format format/all-digits
              :coerce coerce/coerce-integer}
             {:name "end_house_number"
              :format format/all-digits
              :coerce coerce/coerce-integer}
             {:name "house_number_prefix"}
             {:name "house_number_suffix"}
             {:name "state"}
             {:name "street_direction"}
             {:name "street_name"}
             {:name "street_suffix"}
             {:name "zip"}]})

(def data-specs
  [{:filename "office.txt"
    :table :offices
    :columns [{:name "id"}
              {:name "contact_information_id"}
              {:name "electoral_district_id"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "filing_deadline"}
              {:name "is_partisan"}
              {:name "name"}
              {:name "office_holder_person_ids"}
              {:name "term_type"}
              {:name "term_start_date"}
              {:name "term_end_date"}]}
   {:filename "voter_service.txt"
    :table :voter-services
    :columns [{:name "id"}
              {:name "description"}
              {:name "election_official_person_id"}
              {:name "department_id"}
              {:name "type"}
              {:name "other_type"}]}
   {:filename "ballot_measure_contest.txt"
    :table :ballot-measure-contests
    :columns [{:name "abbreviation"}
              {:name "ballot_selection_ids"}
              {:name "ballot_sub_title"}
              {:name "ballot_title"}
              {:name "electoral_district_id"}
              {:name "electorate_specification"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "has_rotation"}
              {:name "name"}
              {:name "sequence_order"}
              {:name "vote_variation"}
              {:name "other_vote_variation"}
              {:name "id"}
              {:name "con_statement"}
              {:name "effect_of_abstain"}
              {:name "full_text" :translate translate/clean-text}
              {:name "info_uri"}
              {:name "passage_threshold"}
              {:name "pro_statement"}
              {:name "summary_text" :translate translate/clean-text}
              {:name "type"}
              {:name "other_type"}]}
   {:filename "ballot_measure_selection.txt"
    :table :ballot-measure-selections
    :columns [{:name "id"}
              {:name "sequence_order"}
              {:name "selection"}]}
   {:filename "ballot_selection.txt"
    :table :ballot-selections
    :columns [{:name "id"}
              {:name "ballot_measure_contest_ids"}
              {:name "ballot_measure_contest_selection_ids"}
              {:name "text" :translate translate/clean-text}
              {:name "candidate_id"}
              {:name "endorsement_party_id"}
              {:name "is_write_in"}
              {:name "sequence_order"}]}
   {:filename "ballot_style.txt"
    :table :ballot-styles
    :columns [{:name "id"}
              {:name "image_uri"}
              {:name "ordered_contest_ids"}
              {:name "party_ids"}]}
   {:filename "candidate.txt"
    :table :candidates
    :columns [{:name "id"}
              {:name "ballot_name"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "file_date"}
              {:name "is_incumbent"}
              {:name "is_top_ticket"}
              {:name "party_id"}
              {:name "person_id"}
              {:name "post_election_status"}
              {:name "pre_election_status"}
              {:name "contest_id"}]}
   {:filename "candidate_contest.txt"
    :table :candidate-contests
    :columns [{:name "id"}
              {:name "abbreviation"}
              {:name "ballot_sub_title"}
              {:name "ballot_title"}
              {:name "ballot_selection_ids"}
              {:name "electoral_district_id"}
              {:name "electorate_specification"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "has_rotation"}
              {:name "name"}
              {:name "sequence_order"}
              {:name "vote_variation"}
              {:name "other_vote_variation"}
              {:name "number_elected"}
              {:name "primary_party_ids"}
              {:name "votes_allowed"}
              {:name "office_ids"}]}
   {:filename "candidate_selection.txt"
    :table :candidate-selections
    :columns [{:name "id"}
              {:name "sequence_order"}
              {:name "candidate_ids"}
              {:name "endorsement_party_ids"}
              {:name "is_write_in"}]}
   {:filename "contact_information.txt"
    :table :contact-information
    :columns [{:name "id"}
              {:name "address_line_1"}
              {:name "address_line_2"}
              {:name "address_line_3"}
              {:name "directions"}
              {:name "email"}
              {:name "fax"}
              {:name "hours"}
              {:name "hours_open_id"}
              {:name "latitude"}
              {:name "longitude"}
              {:name "latlng_source"}
              {:name "name"}
              {:name "parent_id"}
              {:name "phone"}
              {:name "uri"}]}
   {:filename "contest.txt"
    :table :contests
    :columns [{:name "id"}
              {:name "abbreviation"}
              {:name "ballot_selection_ids"}
              {:name "ballot_sub_title"}
              {:name "ballot_title"}
              {:name "electoral_district_id"}
              {:name "electorate_specification"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "has_rotation"}
              {:name "name"}
              {:name "sequence_order"}
              {:name "vote_variation"}
              {:name "other_vote_variation"}]}
   {:filename "department.txt"
    :table :departments
    :columns [{:name "id"}
              {:name "election_official_person_id"}
              {:name "election_administration_id"}]}
   {:filename "election.txt"
    :table :elections
    :columns [{:name "id"}
              {:name "date"}
              {:name "name"}
              {:name "election_type"}
              {:name "state_id"}
              {:name "is_statewide"}
              {:name "registration_info"}
              {:name "absentee_ballot_info"}
              {:name "results_uri"}
              {:name "polling_hours"}
              {:name "has_election_day_registration"}
              {:name "registration_deadline"}
              {:name "absentee_request_deadline"}
              {:name "hours_open_id"}]}
   {:filename "election_administration.txt"
    :table :election-administrations
    :columns [{:name "id"}
              {:name "absentee_uri"}
              {:name "am_i_registered_uri"}
              {:name "elections_uri"}
              {:name "registration_uri"}
              {:name "rules_uri"}
              {:name "what_is_on_my_ballot_uri"}
              {:name "where_do_i_vote_uri"}
              {:name "ballot_tracking_uri"}
              {:name "ballot_tracking_provisional_uri"}
              {:name "election_notice_text"}
              {:name "election_notice_uri"}]}
   {:filename "electoral_district.txt"
    :table :electoral-districts
    :columns [{:name "id"}
              {:name "name"}
              {:name "type"}
              {:name "number"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "other_type"}]}
   {:filename "schedule.txt"
    :table :schedules
    :columns [{:name "id"}
              {:name "hours_open_id"}
              {:name "start_time"}
              {:name "end_time"}
              {:name "is_only_by_appointment"}
              {:name "is_or_by_appointment"}
              {:name "is_subject_to_change"}
              {:name "start_date"}
              {:name "end_date"}]}
   {:filename "locality.txt"
    :table :localities
    :columns [{:name "id"}
              {:name "name"}
              {:name "state_id"}
              {:name "type"}
              {:name "other_type"}
              {:name "election_administration_id"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "polling_location_ids"}
              {:name "is_mail_only"}]}
   {:filename "ordered_contest.txt"
    :table :ordered-contests
    :columns [{:name "id"}
              {:name "contest_id"}
              {:name "ordered_ballot_selection_ids"}]}
   {:filename "party.txt"
    :table :parties
    :columns [{:name "id"}
              {:name "abbreviation"}
              {:name "color"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "logo_uri"}
              {:name "name"}]}
   {:filename "party_contest.txt"
    :table :party-contests
    :columns [{:name "id"}
              {:name "abbreviation"}
              {:name "ballot_selection_ids"}
              {:name "ballot_sub_title"}
              {:name "ballot_title"}
              {:name "electoral_district_id"}
              {:name "electorate_specification"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "has_rotation"}
              {:name "name"}
              {:name "sequence_order"}
              {:name "vote_variation"}
              {:name "other_vote_variation"}]}
   {:filename "party_selection.txt"
    :table :party-selections
    :columns [{:name "id"}
              {:name "sequence_order"}
              {:name "party_ids"}]}
   {:filename "person.txt"
    :table :people
    :columns [{:name "id"}
              {:name "contact_information_id"}
              {:name "date_of_birth"}
              {:name "first_name"}
              {:name "last_name"}
              {:name "middle_name"}
              {:name "nickname"}
              {:name "prefix"}
              {:name "suffix"}
              {:name "title"}
              {:name "profession"}
              {:name "party_id"}
              {:name "gender"}]}
   {:filename "polling_location.txt"
    :table :polling-locations
    :columns [{:name "id"}
              {:name "name"}
              {:name "address_line"}
              {:name "directions"}
              {:name "hours"}
              {:name "hours_open_id"}
              {:name "photo_uri"}
              {:name "is_drop_box"}
              {:name "is_early_voting"}
              {:name "latitude"}
              {:name "longitude"}
              {:name "latlng_source"}]}
   {:filename "precinct.txt"
    :table :precincts
    :columns [{:name "id"}
              {:name "name"}
              {:name "number"}
              {:name "locality_id"}
              {:name "ward"}
              {:name "is_mail_only"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "precinct_split_name"}
              {:name "ballot_style_id"}
              {:name "electoral_district_ids"}
              {:name "polling_location_ids"}]}
   {:filename "retention_contest.txt"
    :table :retention-contests
    :columns [{:name "id"}
              {:name "abbreviation"}
              {:name "ballot_selection_ids"}
              {:name "ballot_sub_title"}
              {:name "ballot_title"}
              {:name "electoral_district_id"}
              {:name "electorate_specification"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "has_rotation"}
              {:name "name"}
              {:name "sequence_order"}
              {:name "vote_variation"}
              {:name "other_vote_variation"}
              {:name "con_statement"}
              {:name "effect_of_abstain"}
              {:name "full_text" :translate translate/clean-text}
              {:name "info_uri"}
              {:name "passage_threshold"}
              {:name "pro_statement"}
              {:name "summary_text" :translate translate/clean-text}
              {:name "type"}
              {:name "other_type"}
              {:name "candidate_id"}
              {:name "office_id"}]}
   {:filename "source.txt"
    :table :sources
    :columns [{:name "id"}
              {:name "name"}
              {:name "vip_id"}
              {:name "date_time"}
              {:name "description"}
              {:name "organization_uri"}
              {:name "terms_of_use_uri"}
              {:name "version"}]}
   {:filename "state.txt"
    :table :states
    :columns [{:name "id"}
              {:name "name"}
              {:name "election_administration_id"}
              {:name "external_identifier_type"}
              {:name "external_identifier_othertype"}
              {:name "external_identifier_value"}
              {:name "polling_location_ids"}]}
   {:filename "street_segment.txt"
    :table :street-segments
    :columns [{:name "id"}
              {:name "includes_all_addresses"
               :coerce coerce/postgres-boolean}
              {:name "includes_all_streets"
               :coerce coerce/postgres-boolean}
              {:name "address_direction"}
              {:name "city"}
              {:name "odd_even_both"}
              {:name "precinct_id"}
              {:name "start_house_number"
               :format format/all-digits
               :coerce coerce/coerce-integer}
              {:name "end_house_number"
               :format format/all-digits
               :coerce coerce/coerce-integer}
              {:name "house_number_prefix"}
              {:name "house_number_suffix"}
              {:name "unit_number"}
              {:name "state"}
              {:name "street_direction"}
              {:name "street_name"}
              {:name "street_suffix"}
              {:name "zip"}]}])
