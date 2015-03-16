(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [vip.data-processor.db.sqlite :as sqlite]))

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

(defn file-name [file]
  (.getName file))

(defn good-filename? [file]
  (->> file
       file-name
       (contains? csv-filenames)))

(defn remove-bad-filenames [ctx]
  (let [input (:input ctx)
        {good-files true bad-files false} (group-by good-filename? input)]
    (if (seq bad-files)
      (-> ctx
          (assoc-in [:warnings :validate-filenames]
                    (apply str "Bad filenames: "
                           (interpose ", " (->> bad-files (map file-name) sort))))
          (assoc :input good-files))
      ctx)))

(defn read-csv-with-headers [file-handle]
  (let [raw-rows (csv/read-csv file-handle)
        headers (first raw-rows)
        rows (rest raw-rows)]
    {:headers headers :contents (map (partial zipmap headers) rows)}))

(defn booleanize [field]
  (fn [row] (assoc row field
                  (if (#{"YES" "yes"} (row field))
                    1 0))))

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (.getName %)))
       first))

(defn csv-loader
  "Generates a validation function that loads the specified file into
  the specified table, transforming each row by the
  row-tranform-fns. Ignores columns that don't exist in the
  database. Will not load files with missing required headers.

  Example:
  (csv-loader \"election.txt\" :elections (booleanize \"statewide\")"
  [filename table required-headers & row-transform-fns]
  (fn [ctx]
    (if-let [file-to-load (find-input-file ctx filename)]
      (with-open [in-file (io/reader file-to-load)]
        (let [sql-table (get-in ctx [:tables table])
              column-names (sqlite/column-names (:db ctx) (:name sql-table))
              select-columns (fn [row] (select-keys row column-names))
              {:keys [headers contents]} (read-csv-with-headers in-file)
              extraneous-headers (seq (set/difference (set headers) (set column-names)))
              ctx (if extraneous-headers
                    (update-in ctx [:warnings filename]
                               conj (str "Extraneous headers: " (str/join ", " extraneous-headers)))
                    ctx)]
          (if (empty? (set/intersection (set headers) (set column-names)))
            (update-in ctx [:critical filename] conj "No header row")
            (if-let [missing-headers (seq (set/difference (set required-headers) (set headers)))]
              (update-in ctx [:critical filename]
                         conj (str "Missing headers: " (str/join ", " missing-headers)))
              (let [transforms (apply comp select-columns row-transform-fns)
                    transformed-contents (map transforms contents)]
                (sqlite/bulk-import transformed-contents sql-table)
                ctx)))))
      ctx)))

(defn add-report-on-missing-file-fn
  "Generates a validation function generator that takes a filename and
  associates a report-type on the context if the filename is missing."
  [report-type]
  (fn [filename]
    (fn [ctx]
      (if (find-input-file ctx filename)
        ctx
        (assoc-in ctx [report-type filename] (str filename " is missing"))))))

(def ^{:doc "Generates a validation function that adds a warning when
  the given filename is missing from the input"}
  warn-on-missing-file
  (add-report-on-missing-file-fn :warnings))

(def ^{:doc "Generates a validation function that adds an error when
  the given filename is missing from the input"}
  error-on-missing-file
  (add-report-on-missing-file-fn :errors))

(def load-elections
  (csv-loader "election.txt" :elections
              ["date" "state_id" "id"]
              (booleanize "election_day_registration")
              (booleanize "statewide")))

(def load-sources
  (csv-loader "source.txt" :sources
              ["name" "vip_id" "datetime" "id"]))

(def load-states
  (csv-loader "state.txt" :states
              ["name" "id"]))

(def load-election-administrations
  (csv-loader "election_administration.txt" :election-administrations
              ["id"]))

(def load-election-officials
  (csv-loader "election_official.txt" :election-officials
              ["name" "id"]))

(def load-localities
  (csv-loader "locality.txt" :localities
              ["name" "state_id" "type" "id"]))

(def load-precincts
  (csv-loader "precinct.txt" :precincts
              ["name" "locality_id" "id"]
              (booleanize "mail_only")))

(def load-precinct-splits
  (csv-loader "precinct_split.txt" :precinct-splits
              ["name" "precinct_id" "id"]))

(def load-precinct-split-electoral-districts
  (csv-loader "precinct_split_electoral_district.txt" :precinct-split-electoral-districts
              ["precinct_split_id" "electoral_district_id"]))

(def load-early-vote-sites
  (csv-loader "early_vote_site.txt" :early-vote-sites
              ["address_line1" "address_city" "address_state" "id"]))

(def load-precinct-polling-locations
  (csv-loader "precinct_polling_location.txt" :precinct-polling-locations
              ["precinct_id" "polling_location_id"]))

(def load-polling-locations
  (csv-loader "polling_location.txt" :polling-locations
              ["address_line1" "address_city" "address_state" "id"]))

(def load-street-segments
  (csv-loader "street_segment.txt" :street-segments
              ["start_house_number" "end_house_number" "precinct_id" "id"]))

(def load-electoral-districts
  (csv-loader "electoral_district.txt" :electoral-districts
              ["name" "id"]))

(def load-contests
  (csv-loader "contest.txt" :contests
              ["election_id" "electoral_district_id" "type" "id"]
              (booleanize "partisan")
              (booleanize "special")))

(def load-ballots
  (csv-loader "ballot.txt" :ballots
              ["id"]
              (booleanize "write_in")))

(def load-ballot-responses
  (csv-loader "ballot_response.txt" :ballot-responses
              ["text" "id"]))

(def load-referendums
  (csv-loader "referendum.txt" :referendums
              ["title" "text" "id"]))

(def load-referendum-ballot-responses
  (csv-loader "referendum_ballot_response.txt" :referendum-ballot-responses
              ["referendum_id" "ballot_response_id"]))

(def load-candidates
  (csv-loader "candidate.txt" :candidates
              ["name" "id"]))

(def load-ballot-candidates
  (csv-loader "ballot_candidate.txt" :ballot-candidates
              ["ballot_id" "candidate_id"]))

(def load-state-early-vote-sites
  (csv-loader "state_early_vote_site.txt" :state-early-vote-sites
              ["state_id" "early_vote_site_id"]))

(def load-precinct-split-polling-locations
  (csv-loader "precinct_split_polling_location.txt" :precinct-split-polling-locations
              ["precinct_split_id" "polling_location_id"]))

(def load-precinct-electoral-districts
  (csv-loader "precinct_electoral_district.txt" :precinct-electoral-districts
              ["precinct_id" "electoral_district_id"]))

(def load-precinct-early-vote-sites
  (csv-loader "precinct_early_vote_site.txt" :precinct-early-vote-sites
              ["precinct_id" "early_vote_site_id"]))

(def load-locality-early-vote-sites
  (csv-loader "locality_early_vote_site.txt" :locality-early-vote-sites
              ["locality_id" "early_vote_site_id"]))

(def load-custom-ballot-ballot-responses
  (csv-loader "custom_ballot_ballot_response.txt" :custom-ballot-ballot-responses
              ["custom_ballot_id" "ballot_response_id"]))

(def load-custom-ballots
  (csv-loader "custom_ballot.txt" :custom-ballots
              ["heading" "id"]))

(def load-contest-results
  (csv-loader "contest_result.txt" :contest-results
              ["contest_id" "jurisdiction_id" "entire_district" "id"]
              (booleanize "entire_district")))

(def load-ballot-line-results
  (csv-loader "ballot_line_result.txt" :ballot-line-results
              ["contest_id" "jurisdiction_id" "entire_district" "votes" "id"]
              (booleanize "entire_district")
              (booleanize "victorious")))
