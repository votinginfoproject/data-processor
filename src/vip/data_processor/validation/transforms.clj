(ns vip.data-processor.validation.transforms
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.csv :as csv]))

(defn read-edn-sqs-message [ctx]
  (assoc ctx :input (edn/read-string (get-in ctx [:input :body]))))

(defn assert-filename [ctx]
  (if-let [filename (get-in ctx [:input :filename])]
    (assoc ctx :filename filename)
    (assoc ctx :stop "No filename!")))

(defn attach-sqlite-db [ctx]
  (merge ctx (sqlite/temp-db (:filename ctx))))

(defn download-from-s3 [ctx]
  (let [filename (get-in ctx [:input :filename])
        file (s3/download filename)]
    (assoc ctx :input file)))

(def xml-validations
  [(fn [ctx] (assoc ctx :stop "This is an XML feed"))])

(def csv-validations
  [csv/remove-bad-filenames
   (csv/error-on-missing-file "election.txt")
   (csv/error-on-missing-file "source.txt")
   (csv/warn-on-missing-file "state.txt")
   (csv/warn-on-missing-file "election_administration.txt")
   (csv/warn-on-missing-file "election_official.txt")
   (csv/warn-on-missing-file "locality.txt")
   (csv/warn-on-missing-file "precinct.txt")
   (csv/warn-on-missing-file "precinct_split.txt")
   (csv/warn-on-missing-file "precinct_split_electoral_district.txt")
   (csv/warn-on-missing-file "early_vote_site.txt")
   (csv/warn-on-missing-file "precinct_polling_location.txt")
   (csv/warn-on-missing-file "polling_location.txt")
   (csv/warn-on-missing-file "street_segment.txt")
   (csv/warn-on-missing-file "electoral_district.txt")
   (csv/warn-on-missing-file "contest.txt")
   (csv/warn-on-missing-file "ballot.txt")
   (csv/warn-on-missing-file "ballot_response.txt")
   (csv/warn-on-missing-file "referendum.txt")
   (csv/warn-on-missing-file "referendum_ballot_response.txt")
   (csv/warn-on-missing-file "candidate.txt")
   csv/load-elections
   csv/load-sources
   csv/load-states
   csv/load-election-administrations
   csv/load-election-officials
   csv/load-localities
   csv/load-precincts
   csv/load-precinct-splits
   csv/load-precinct-split-electoral-districts
   csv/load-early-vote-sites
   csv/load-precinct-polling-locations
   csv/load-polling-locations
   csv/load-street-segments
   csv/load-electoral-districts
   csv/load-contests
   csv/load-ballots
   csv/load-ballot-responses
   csv/load-referendums
   csv/load-referendum-ballot-responses
   csv/load-candidates])

(defn xml-csv-branch [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last))
                             set)
        filetype-validations (condp set/superset? file-extensions
                               #{"txt" "csv"} csv-validations
                               #{"xml"} xml-validations)]
    (update ctx :pipeline (partial concat filetype-validations))))
