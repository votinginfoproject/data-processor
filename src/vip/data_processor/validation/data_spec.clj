(ns vip.data-processor.validation.data-spec
  (:require [vip.data-processor.validation.csv.value-format :as format]))

(defn boolean-value [x]
  (if (re-find #"\A(?i:yes)\z" x) 1 0))

(defn add-data-specs [data-specs]
  (fn [ctx]
    (assoc ctx :data-specs data-specs)))

(def data-specs
  [{:filename "ballot.txt"
    :table :ballots
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "referendum_id" :format format/all-digits :references :referendums}
              {:name "custom_ballot_id" :format format/all-digits :references :custom-ballots}
              {:name "write_in" :format format/yes-no :translate boolean-value}
              {:name "image_url" :format format/url}]}
   {:filename "ballot_candidate.txt"
    :table :ballot-candidates
    :columns [{:name "ballot_id" :required true :format format/all-digits :references :ballots}
              {:name "candidate_id" :required true :format format/all-digits :references :candidates}]}
   {:filename "ballot_line_result.txt"
    :table :ballot-line-results
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "contest_id" :required true :format format/all-digits :references :contests}
              {:name "jurisdiction_id" :required true :format format/all-digits}
              {:name "entire_district" :required true :format format/yes-no :translate boolean-value}
              {:name "candidate_id" :format format/all-digits :references :candidates}
              {:name "ballot_response_id" :format format/all-digits :references :ballot-responses}
              {:name "votes" :required true :format format/all-digits}
              {:name "overvotes" :format format/all-digits}
              {:name "victorious" :format format/yes-no :translate boolean-value}
              {:name "certification"}]}
   {:filename "ballot_response.txt"
    :table :ballot-responses
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "text" :required true}
              {:name "sort_order" :format format/all-digits}]}
   {:filename "candidate.txt"
    :table :candidates
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "party"}
              {:name "candidate_url" :format format/url}
              {:name "biography"}
              {:name "phone" :format format/phone}
              {:name "photo_url" :format format/url}
              {:name "filed_mailing_address_location_name"}
              {:name "filed_mailing_address_line1"}
              {:name "filed_mailing_address_line2"}
              {:name "filed_mailing_address_line3"}
              {:name "filed_mailing_address_city"}
              {:name "filed_mailing_address_state"}
              {:name "filed_mailing_address_zip"}
              {:name "email" :format format/email}
              {:name "sort_order" :format format/all-digits}]}
   {:filename "contest.txt"
    :table :contests
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "election_id" :required true :format format/all-digits :references :elections}
              {:name "electoral_district_id" :required true :format format/all-digits :references :electoral-districts}
              {:name "type" :required true :format format/contest-election-type}
              {:name "partisan" :format format/yes-no :translate boolean-value}
              {:name "primary_party"}
              {:name "electorate_specifications"}
              {:name "special" :format format/yes-no :translate boolean-value}
              {:name "office"}
              {:name "filing_closed_date" :format format/date}
              {:name "number_elected" :format format/all-digits}
              {:name "number_voting_for" :format format/all-digits}
              {:name "ballot_id" :format format/all-digits :references :ballots}
              {:name "ballot_placement" :format format/all-digits}]}
   {:filename "contest_result.txt"
    :table :contest-results
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "contest_id" :required true :format format/all-digits :references :contests}
              {:name "jurisdiction_id" :required true :format format/all-digits}
              {:name "entire_district" :required true :format format/yes-no :translate boolean-value}
              {:name "total_votes" :format format/all-digits}
              {:name "total_valid_votes" :format format/all-digits}
              {:name "overvotes" :format format/all-digits}
              {:name "blank_votes" :format format/all-digits}
              {:name "accepted_provisional_votes" :format format/all-digits}
              {:name "rejected_votes" :format format/all-digits}
              {:name "certification"}]}
   {:filename "custom_ballot.txt"
    :table :custom-ballots
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "heading" :required true}]}
   {:filename "custom_ballot_ballot_response.txt"
    :table :custom-ballot-ballot-responses
    :columns [{:name "custom_ballot_id" :required true :format format/all-digits :references :custom-ballots}
              {:name "ballot_response_id" :required true :format format/all-digits :references :ballot-responses}
              {:name "sort_order" :format format/all-digits}]}
   {:filename "early_vote_site.txt"
    :table :early-vote-sites
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name"}
              {:name "address_location_name"}
              {:name "address_line1" :required true}
              {:name "address_line2"}
              {:name "address_line3"}
              {:name "address_city" :required true}
              {:name "address_state" :required true}
              {:name "address_zip"}
              {:name "directions"}
              {:name "voter_services"}
              {:name "start_date" :format format/date}
              {:name "end_date" :format format/date}
              {:name "days_times_open"}]}
   {:filename "election.txt"
    :table :elections
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "date" :required true :format format/date}
              {:name "election_type" :format format/election-type}
              {:name "state_id" :required true :format format/all-digits :references :states}
              {:name "statewide" :format format/yes-no :translate boolean-value}
              {:name "registration_info" :format format/url}
              {:name "absentee_ballot_info" :format format/url}
              {:name "results_url" :format format/url}
              {:name "polling_hours"}
              {:name "election_day_registration" :format format/yes-no :translate boolean-value}
              {:name "registration_deadline" :format format/date}
              {:name "absentee_request_deadline" :format format/date}]}
   {:filename "election_administration.txt"
    :table :election-administrations
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name"}
              {:name "eo_id" :format format/all-digits :references :election-officials}
              {:name "ovc_id" :format format/all-digits}
              {:name "physical_address_location_name"}
              {:name "physical_address_line1"}
              {:name "physical_address_line2"}
              {:name "physical_address_line3"}
              {:name "physical_address_city"}
              {:name "physical_address_state"}
              {:name "physical_address_zip"}
              {:name "mailing_address_location_name"}
              {:name "mailing_address_line1"}
              {:name "mailing_address_line2"}
              {:name "mailing_address_line3"}
              {:name "mailing_address_city"}
              {:name "mailing_address_state"}
              {:name "mailing_address_zip"}
              {:name "elections_url" :format format/url}
              {:name "registration_url" :format format/url}
              {:name "am_i_registered_url" :format format/url}
              {:name "absentee_url" :format format/url}
              {:name "where_do_i_vote_url" :format format/url}
              {:name "what_is_on_my_ballot_url" :format format/url}
              {:name "rules_url" :format format/url}
              {:name "voter_services"}
              {:name "hours"}]}
   {:filename "election_official.txt"
    :table :election-officials
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "title"}
              {:name "phone" :format format/phone}
              {:name "fax" :format format/phone}
              {:name "email" :format format/email}]}
   {:filename "electoral_district.txt"
    :table :electoral-districts
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "type" :format format/electoral-district-type}
              {:name "number" :format format/all-digits}]}
   {:filename "locality.txt"
    :table :localities
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "state_id" :required true :format format/all-digits :references :states}
              {:name "type" :required true :format format/locality-type}
              {:name "election_administration_id" :format format/all-digits :references :election-administrations}]}
   {:filename "locality_early_vote_site.txt"
    :table :locality-early-vote-sites
    :columns [{:name "locality_id" :required true :format format/all-digits :references :localities}
              {:name "early_vote_site_id" :required true :format format/all-digits :references :early-vote-sites}]}
   {:filename "polling_location.txt"
    :table :polling-locations
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "address_location_name"}
              {:name "address_line1" :required true}
              {:name "address_line2"}
              {:name "address_line3"}
              {:name "address_city" :required true}
              {:name "address_state" :required true}
              {:name "address_zip"}
              {:name "directions"}
              {:name "polling_hours"}
              {:name "photo_url" :format format/url}]}
   {:filename "precinct.txt"
    :table :precincts
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "number"}
              {:name "locality_id" :required true :format format/all-digits :references :localities}
              {:name "ward"}
              {:name "mail_only" :format format/yes-no :translate boolean-value}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_split.txt"
    :table :precinct-splits
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "precinct_id" :required true :format format/all-digits :references :precincts}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_early_vote_site.txt"
    :table :precinct-early-vote-sites
    :columns [{:name "precinct_id" :required true :format format/all-digits :references :precincts}
              {:name "early_vote_site_id" :required true :format format/all-digits :references :early-vote-sites}]}
   {:filename "precinct_electoral_district.txt"
    :table :precinct-electoral-districts
    :columns [{:name "precinct_id" :required true :format format/all-digits :references :precincts}
              {:name "electoral_district_id" :required true :format format/all-digits :references :electoral-districts}]}
   {:filename "precinct_polling_location.txt"
    :table :precinct-polling-locations
    :columns [{:name "precinct_id" :required true :format format/all-digits :references :precincts}
              {:name "polling_location_id" :required true :format format/all-digits :references :polling-locations}]}
   {:filename "precinct_split_electoral_district.txt"
    :table :precinct-split-electoral-districts
    :columns [{:name "precinct_split_id" :required true :format format/all-digits :references :precinct-splits}
              {:name "electoral_district_id" :required true :format format/all-digits :references :electoral-districts}]}
   {:filename "precinct_split_polling_location.txt"
    :table :precinct-split-polling-locations
    :columns [{:name "precinct_split_id" :required true :format format/all-digits :references :precinct-splits}
              {:name "polling_location_id" :required true :format format/all-digits :references :polling-locations}]}
   {:filename "referendum.txt"
    :table :referendums
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "title" :required true}
              {:name "subtitle"}
              {:name "brief"}
              {:name "text" :required true}
              {:name "pro_statement"}
              {:name "con_statement"}
              {:name "passage_threshold"}
              {:name "effect_of_abstain"}]}
   {:filename "referendum_ballot_response.txt"
    :table :referendum-ballot-responses
    :columns [{:name "referendum_id" :required true :format format/all-digits :references :referendums}
              {:name "ballot_response_id" :required true :format format/all-digits :references :ballot-responses}
              {:name "sort_order" :format format/all-digits}]}
   {:filename "state_early_vote_site.txt"
    :table :state-early-vote-sites
    :columns [{:name "state_id" :required true :format format/all-digits :references :states}
              {:name "early_vote_site_id" :required true :format format/all-digits :references :early-vote-sites}]}
   {:filename "street_segment.txt"
    :table :street-segments
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "start_house_number" :required true}
              {:name "end_house_number" :required true}
              {:name "odd_even_both" :format format/odd-even-both}
              {:name "start_apartment_number" :format format/all-digits}
              {:name "end_apartment_number" :format format/all-digits}
              {:name "non_house_address_house_number" :format format/all-digits}
              {:name "non_house_address_house_number_prefix"}
              {:name "non_house_address_house_number_suffix"}
              {:name "non_house_address_street_direction" :format format/street-direction}
              {:name "non_house_address_street_name"}
              {:name "non_house_address_street_suffix"}
              {:name "non_house_address_address_direction" :format format/street-direction}
              {:name "non_house_address_apartment"}
              {:name "non_house_address_city"}
              {:name "non_house_address_state"}
              {:name "non_house_address_zip"}
              {:name "precinct_id" :required true :format format/all-digits :references :precincts}
              {:name "precinct_split_id" :format format/all-digits :references :precinct-splits}]}
   {:filename "source.txt"
    :table :sources
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "vip_id" :required true :format format/all-digits}
              {:name "datetime" :required true :format format/datetime}
              {:name "description"}
              {:name "organization_url" :format format/url}
              {:name "feed_contact_id" :format format/all-digits :references :election-officials}
              {:name "tou_url" :format format/url}]}
   {:filename "state.txt"
    :table :states
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "election_administration_id" :format format/all-digits :references :election-administrations}]}])
