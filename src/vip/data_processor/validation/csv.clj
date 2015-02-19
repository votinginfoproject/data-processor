(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [korma.core :as korma]))

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

(defn load-elections [ctx]
  (let [files (:input ctx)
        election-file (first (filter #(= "election.txt" (.getName %)) files))]
    (if election-file
      (let [elections-table (get-in ctx [:tables :elections])
            contents (read-csv-with-headers election-file)
            coerced-contents (->> contents
                                  (map (booleanize "election_day_registration"))
                                  (map (booleanize "statewide")))]
        (korma/insert elections-table (korma/values coerced-contents))
        ctx)
      (assoc-in ctx [:errors :load-elections] "election.txt missing"))))

(defn load-sources [ctx]
  (let [files (:input ctx)
        source-file (first (filter #(= "source.txt" (.getName %)) files))]
    (if source-file
      (let [sources-table (get-in ctx [:tables :sources])
            contents (read-csv-with-headers source-file)]
        (korma/insert sources-table (korma/values contents))
        ctx)
      (assoc-in ctx [:errors :load-sources] "source.txt missing"))))
