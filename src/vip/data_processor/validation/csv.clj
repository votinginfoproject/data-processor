(ns vip.data-processor.validation.csv
  (:require [clojure.set :as set]))

(def csv-filenames
  #{"ballot.txt"
    "ballot_candidate.txt"
    "ballot_line_result.txt"
    "ballot_response.txt"
    "candidate.txt"
    "contest.txt"
    "contest_result.txt"
    "custom_ballot.txt"
    "custom_ballot_ballot_response.txt"
    "early_vote_site.txt"
    "election.txt"
    "election_administration.txt"
    "election_official.txt"
    "electoral_district.txt"
    "locality.txt"
    "locality_early_vote_site.txt"
    "polling_location.txt"
    "precinct.txt"
    "precinct_early_vote_site.txt"
    "precinct_electoral_district.txt"
    "precinct_polling_location.txt"
    "precinct_split.txt"
    "precinct_split_electoral_district.txt"
    "precinct_split_polling_location.txt"
    "referendum.txt"
    "referendum_ballot_response.txt"
    "source.txt"
    "state.txt"
    "state_early_vote_site.txt"
    "street_segment.txt"})

(defn validate-filenames [ctx]
  (let [input (:input ctx)
        filenames (map #(.getName %) input)
        bad-filenames (seq (set/difference (set filenames) csv-filenames))]
    (if bad-filenames
      (assoc ctx :stop (apply str "Bad filenames: "
                                  (interpose ", " (sort bad-filenames))))
      ctx)))
