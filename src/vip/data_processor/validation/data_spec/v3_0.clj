(ns vip.data-processor.validation.data-spec.v3-0
  (:require [vip.data-processor.validation.data-spec.value-format :as format]
            [vip.data-processor.validation.data-spec.coerce :as coerce]))

(def data-specs
  [{:filename "ballot.txt"
    :table :ballots
    :tag-name :ballot
    :stats true
    :ignore-duplicate-records true
    :xml-references [{:join-table :ballot-candidates
                      :id "ballot_id"
                      :joined-id "candidate_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "referendum_id" :format format/all-digits :references :referendums :coerce coerce/coerce-integer}
              {:name "custom_ballot_id" :format format/all-digits :references :custom-ballots :coerce coerce/coerce-integer}
              {:name "write_in" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "image_url" :format format/url}]}
   {:filename "ballot_candidate.txt"
    :table :ballot-candidates
    :columns [{:name "ballot_id" :required :critical :format format/all-digits :references :ballots :coerce coerce/coerce-integer}
              {:name "candidate_id" :required :critical :format format/all-digits :references :candidates :coerce coerce/coerce-integer}]}
   {:filename "ballot_line_result.txt"
    :table :ballot-line-results
    :tag-name :ballot_line_result
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits}
              {:name "contest_id" :required :critical :format format/all-digits :references :contests :coerce coerce/coerce-integer}
              {:name "jurisdiction_id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "entire_district" :required :critical :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "candidate_id" :format format/all-digits :references :candidates :coerce coerce/coerce-integer}
              {:name "ballot_response_id" :format format/all-digits :references :ballot-responses :coerce coerce/coerce-integer}
              {:name "votes" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "overvotes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "victorious" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "certification"}]}
   {:filename "ballot_response.txt"
    :table :ballot-responses
    :tag-name :ballot_response
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "text" :required :critical}
              {:name "sort_order" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "candidate.txt"
    :table :candidates
    :tag-name :candidate
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
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
              {:name "sort_order" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "contest.txt"
    :table :contests
    :tag-name :contest
    :stats true
    :ignore-unreferenced-rows true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "election_id" :required :critical :format format/all-digits :references :elections :coerce coerce/coerce-integer}
              {:name "electoral_district_id" :required :critical :format format/all-digits :references :electoral-districts :coerce coerce/coerce-integer}
              {:name "type" :required :critical}
              {:name "partisan" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "primary_party"}
              {:name "electorate_specifications"}
              {:name "special" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "office"}
              {:name "filing_closed_date" :format format/date :coerce coerce/coerce-date}
              {:name "number_elected" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "number_voting_for" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "ballot_id" :format format/all-digits :references :ballots :coerce coerce/coerce-integer}
              {:name "ballot_placement" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "contest_result.txt"
    :table :contest-results
    :tag-name :contest_result
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "contest_id" :required :critical :format format/all-digits :references :contests :coerce coerce/coerce-integer}
              {:name "jurisdiction_id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "entire_district" :required :critical :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "total_votes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "total_valid_votes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "overvotes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "blank_votes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "accepted_provisional_votes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "rejected_votes" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "certification"}]}
   {:filename "custom_ballot.txt"
    :table :custom-ballots
    :tag-name :custom_ballot
    :stats true
    :xml-references [{:join-table :custom-ballot-ballot-responses
                      :id "custom_ballot_id"
                      :joined-id "ballot_response_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "heading" :required :critical}]}
   {:filename "custom_ballot_ballot_response.txt"
    :table :custom-ballot-ballot-responses
    :columns [{:name "custom_ballot_id" :required :critical :format format/all-digits :references :custom-ballots :coerce coerce/coerce-integer}
              {:name "ballot_response_id" :required :critical :format format/all-digits :references :ballot-responses :coerce coerce/coerce-integer}
              {:name "sort_order" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "early_vote_site.txt"
    :table :early-vote-sites
    :tag-name :early_vote_site
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name"}
              {:name "address_location_name"}
              {:name "address_line1" :required :critical}
              {:name "address_line2"}
              {:name "address_line3"}
              {:name "address_city" :required :critical}
              {:name "address_state" :required :critical}
              {:name "address_zip" :required :errors}
              {:name "directions"}
              {:name "voter_services"}
              {:name "start_date" :format format/date :coerce coerce/coerce-date}
              {:name "end_date" :format format/date :coerce coerce/coerce-date}
              {:name "days_times_open"}]}
   {:filename "election.txt"
    :table :elections
    :tag-name :election
    :stats true
    :columns [{:name "id" :required :fatal :format format/all-digits :coerce coerce/coerce-integer}
              {:name "date" :required :fatal :format format/date :coerce coerce/coerce-date}
              {:name "election_type"}
              {:name "state_id" :required :fatal :format format/all-digits :references :states :coerce coerce/coerce-integer}
              {:name "statewide" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "registration_info" :format format/url}
              {:name "absentee_ballot_info" :format format/url}
              {:name "results_url" :format format/url}
              {:name "polling_hours"}
              {:name "election_day_registration" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "registration_deadline" :format format/date :coerce coerce/coerce-date}
              {:name "absentee_request_deadline" :format format/date :coerce coerce/coerce-date}]}
   {:filename "election_administration.txt"
    :table :election-administrations
    :tag-name :election_administration
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name"}
              {:name "eo_id" :format format/all-digits :references :election-officials :coerce coerce/coerce-integer}
              {:name "ovc_id" :format format/all-digits :coerce coerce/coerce-integer}
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
    :tag-name :election_official
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "title"}
              {:name "phone" :format format/phone}
              {:name "fax" :format format/phone}
              {:name "email" :format format/email}]}
   {:filename "electoral_district.txt"
    :table :electoral-districts
    :tag-name :electoral_district
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "type" :format format/electoral-district-type}
              {:name "number" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "locality.txt"
    :table :localities
    :tag-name :locality
    :stats true
    :xml-references [{:join-table :locality-early-vote-sites
                      :id "locality_id"
                      :joined-id "early_vote_site_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "state_id" :required :critical :format format/all-digits :references :states :coerce coerce/coerce-integer}
              {:name "type" :required :critical}
              {:name "election_administration_id" :format format/all-digits :references :election-administrations :coerce coerce/coerce-integer}]}
   {:filename "locality_early_vote_site.txt"
    :table :locality-early-vote-sites
    :columns [{:name "locality_id" :required :critical :format format/all-digits :references :localities :coerce coerce/coerce-integer}
              {:name "early_vote_site_id" :required :critical :format format/all-digits :references :early-vote-sites :coerce coerce/coerce-integer}]}
   {:filename "polling_location.txt"
    :table :polling-locations
    :tag-name :polling_location
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "address_location_name"}
              {:name "address_line1" :required :critical}
              {:name "address_line2"}
              {:name "address_line3"}
              {:name "address_city" :required :critical}
              {:name "address_state" :required :critical}
              {:name "address_zip" :required :errors}
              {:name "directions"}
              {:name "polling_hours"}
              {:name "photo_url" :format format/url}]}
   {:filename "precinct.txt"
    :table :precincts
    :tag-name :precinct
    :stats true
    :xml-references [{:join-table :precinct-polling-locations
                      :id "precinct_id"
                      :joined-id "polling_location_id"}
                     {:join-table :precinct-early-vote-sites
                      :id "precinct_id"
                      :joined-id "early_vote_site_id"}
                     {:join-table :precinct-electoral-districts
                      :id "precinct_id"
                      :joined-id "electoral_district_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "number"}
              {:name "locality_id" :required :critical :format format/all-digits :references :localities :coerce coerce/coerce-integer}
              {:name "ward"}
              {:name "mail_only" :format format/yes-no :translate coerce/boolean-value :coerce coerce/coerce-boolean}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_split.txt"
    :table :precinct-splits
    :tag-name :precinct_split
    :stats true
    :xml-references [{:join-table :precinct-split-electoral-districts
                      :id "precinct_split_id"
                      :joined-id "electoral_district_id"}
                     {:join-table :precinct-split-polling-locations
                      :id "precinct_split_id"
                      :joined-id "polling_location_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "precinct_id" :required :critical :format format/all-digits :references :precincts :coerce coerce/coerce-integer}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_early_vote_site.txt"
    :table :precinct-early-vote-sites
    :columns [{:name "precinct_id" :required :critical :format format/all-digits :references :precincts :coerce coerce/coerce-integer}
              {:name "early_vote_site_id" :required :critical :format format/all-digits :references :early-vote-sites :coerce coerce/coerce-integer}]}
   {:filename "precinct_electoral_district.txt"
    :table :precinct-electoral-districts
    :columns [{:name "precinct_id" :required :critical :format format/all-digits :references :precincts :coerce coerce/coerce-integer}
              {:name "electoral_district_id" :required :critical :format format/all-digits :references :electoral-districts :coerce coerce/coerce-integer}]}
   {:filename "precinct_polling_location.txt"
    :table :precinct-polling-locations
    :columns [{:name "precinct_id" :required :critical :format format/all-digits :references :precincts :coerce coerce/coerce-integer}
              {:name "polling_location_id" :required :critical :format format/all-digits :references :polling-locations :coerce coerce/coerce-integer}]}
   {:filename "precinct_split_electoral_district.txt"
    :table :precinct-split-electoral-districts
    :columns [{:name "precinct_split_id" :required :critical :format format/all-digits :references :precinct-splits :coerce coerce/coerce-integer}
              {:name "electoral_district_id" :required :critical :format format/all-digits :references :electoral-districts :coerce coerce/coerce-integer}]}
   {:filename "precinct_split_polling_location.txt"
    :table :precinct-split-polling-locations
    :columns [{:name "precinct_split_id" :required :critical :format format/all-digits :references :precinct-splits :coerce coerce/coerce-integer}
              {:name "polling_location_id" :required :critical :format format/all-digits :references :polling-locations :coerce coerce/coerce-integer}]}
   {:filename "referendum.txt"
    :table :referendums
    :tag-name :referendum
    :stats true
    :xml-references [{:join-table :referendum-ballot-responses
                      :id "referendum_id"
                      :joined-id "ballot_response_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "title" :required :critical}
              {:name "subtitle"}
              {:name "brief"}
              {:name "text" :required :critical}
              {:name "pro_statement"}
              {:name "con_statement"}
              {:name "passage_threshold"}
              {:name "effect_of_abstain"}]}
   {:filename "referendum_ballot_response.txt"
    :table :referendum-ballot-responses
    :columns [{:name "referendum_id" :required :critical :format format/all-digits :references :referendums :coerce coerce/coerce-integer}
              {:name "ballot_response_id" :required :critical :format format/all-digits :references :ballot-responses :coerce coerce/coerce-integer}
              {:name "sort_order" :format format/all-digits :coerce coerce/coerce-integer}]}
   {:filename "state_early_vote_site.txt"
    :table :state-early-vote-sites
    :columns [{:name "state_id" :required :critical :format format/all-digits :references :states :coerce coerce/coerce-integer}
              {:name "early_vote_site_id" :required :critical :format format/all-digits :references :early-vote-sites :coerce coerce/coerce-integer}]}
   {:filename "street_segment.txt"
    :table :street-segments
    :tag-name :street_segment
    :stats true
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "start_house_number" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "end_house_number" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "odd_even_both" :format format/odd-even-both}
              {:name "start_apartment_number" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "end_apartment_number" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "non_house_address_house_number" :format format/all-digits :coerce coerce/coerce-integer}
              {:name "non_house_address_house_number_prefix"}
              {:name "non_house_address_house_number_suffix"}
              {:name "non_house_address_street_direction" :format format/street-direction}
              {:name "non_house_address_street_name" :required :critical}
              {:name "non_house_address_street_suffix"}
              {:name "non_house_address_address_direction" :format format/street-direction}
              {:name "non_house_address_apartment"}
              {:name "non_house_address_city" :required :critical}
              {:name "non_house_address_state" :required :critical}
              {:name "non_house_address_zip" :required :critical}
              {:name "precinct_id" :required :critical :format format/all-digits :references :precincts :coerce coerce/coerce-integer}
              {:name "precinct_split_id" :format format/all-digits :references :precinct-splits :coerce coerce/coerce-integer}]}
   {:filename "source.txt"
    :table :sources
    :tag-name :source
    :stats true
    :columns [{:name "id" :required :fatal :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :fatal}
              {:name "vip_id" :required :fatal :format format/all-digits}
              {:name "datetime" :required :fatal :format format/datetime :coerce coerce/coerce-date}
              {:name "description"}
              {:name "organization_url" :format format/url}
              {:name "feed_contact_id" :format format/all-digits :references :election-officials :coerce coerce/coerce-integer}
              {:name "tou_url" :format format/url}]}
   {:filename "state.txt"
    :table :states
    :tag-name :state
    :xml-references [{:join-table :state-early-vote-sites
                      :id "state_id"
                      :joined-id "early_vote_site_id"}]
    :columns [{:name "id" :required :critical :format format/all-digits :coerce coerce/coerce-integer}
              {:name "name" :required :critical}
              {:name "election_administration_id" :format format/all-digits :references :election-administrations :coerce coerce/coerce-integer}]}])
