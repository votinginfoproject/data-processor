(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
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
    (map (partial zipmap headers) rows)))

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
  row-tranform-fns. Ignores columns that don't exist in the database.

  Example:
  (csv-loader \"election.txt\" :elections (booleanize \"statewide\")"
  [filename table & row-transform-fns]
  (fn [ctx]
    (when-let [file-to-load (find-input-file ctx filename)]
      (with-open [in-file (io/reader file-to-load)]
        (let [sql-table (get-in ctx [:tables table])
              column-names (sqlite/column-names (:db ctx) (:name sql-table))
              select-columns (fn [row] (select-keys row column-names))
              contents (read-csv-with-headers in-file)
              transforms (apply comp select-columns row-transform-fns)
              transformed-contents (map transforms contents)]
          (sqlite/bulk-import transformed-contents sql-table))))
    ctx))

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

(def load-elections (csv-loader "election.txt" :elections
                                (booleanize "election_day_registration")
                                (booleanize "statewide")))
(def load-sources (csv-loader "source.txt" :sources))
(def load-states (csv-loader "state.txt" :states))
(def load-election-administrations
  (csv-loader "election_administration.txt" :election-administrations))
(def load-election-officials (csv-loader "election_official.txt" :election-officials))
(def load-localities (csv-loader "locality.txt" :localities))
(def load-precincts (csv-loader "precinct.txt" :precincts
                                (booleanize "mail_only")))
(def load-precinct-splits (csv-loader "precinct_split.txt" :precinct-splits))
(def load-precinct-split-electoral-districts
  (csv-loader "precinct_split_electoral_district.txt" :precinct-split-electoral-districts))
(def load-early-vote-sites (csv-loader "early_vote_site.txt" :early-vote-sites))
(def load-precinct-polling-locations (csv-loader "precinct_polling_location.txt" :precinct-polling-locations))
(def load-polling-locations (csv-loader "polling_location.txt" :polling-locations))
(def load-street-segments (csv-loader "street_segment.txt" :street-segments))
(def load-electoral-districts (csv-loader "electoral_district.txt" :electoral-districts))
(def load-contests (csv-loader "contest.txt" :contests
                               (booleanize "partisan")
                               (booleanize "special")))
(def load-ballots (csv-loader "ballot.txt" :ballots
                              (booleanize "write_in")))
(def load-ballot-responses (csv-loader "ballot_response.txt" :ballot-responses))
(def load-referendums (csv-loader "referendum.txt" :referendums))
(def load-referendum-ballot-responses (csv-loader "referendum_ballot_response.txt" :referendum-ballot-responses))
(def load-candidates (csv-loader "candidate.txt" :candidates))
(def load-ballot-candidates (csv-loader "ballot_candidate.txt" :ballot-candidates))
(def load-state-early-vote-sites (csv-loader "state_early_vote_site.txt" :state-early-vote-sites))
(def load-precinct-split-polling-locations (csv-loader "precinct_split_polling_location.txt" :precinct-split-polling-locations))
