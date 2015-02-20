(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [korma.core :as korma]
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

(defn read-csv-with-headers [file]
  (let [raw-rows (with-open [in-file (io/reader file)]
                   (doall
                    (csv/read-csv in-file)))
        headers (first raw-rows)
        rows (rest raw-rows)]
    (map (partial zipmap headers) rows)))

(defn booleanize [field]
  (fn [row] (assoc row field
                  (if (= "yes" (row field))
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
      (let [sql-table (get-in ctx [:tables table])
            column-names (sqlite/column-names (:db ctx) (:name sql-table))
            select-columns (fn [row] (select-keys row column-names))
            contents (read-csv-with-headers file-to-load)
            transforms (apply comp select-columns row-transform-fns)
            transformed-contents (map transforms contents)]
        (korma/insert sql-table (korma/values transformed-contents))))
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
