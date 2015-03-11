(ns vip.data-processor.validation.csv.file-set
  (:require [vip.data-processor.validation.csv :as csv]
            [clojure.string :as str]
            [clojure.walk :as walk]))

(defn- build-dependency-pred [ctx-sym dependencies]
  (walk/postwalk
   (fn [dependencies]
     (if (string? dependencies)
       `(csv/find-input-file ~ctx-sym ~dependencies)
       dependencies))
   dependencies))

(defn- error-message-part [part]
  (if (sequential? part)
    (str "("
         (str/join (str " " (first part) " ") (map error-message-part (rest part)))
         ")")
    (str part)))

(defn error-message-for [dependencies]
  (str "File dependencies not met: " (error-message-part dependencies)))

(defmacro build-dependencies
  "Generate a map of files to validator functions appropriate for
  validate-dependencies. The value of each mappings-pair should be
  built with parentheses, `and` and `or`. For example:

    \"file-a.txt\" (and \"file-b.txt\"
                        (or \"file-c.txt\"
                            \"file-d.txt\"))"
  [& mappings]
  (when (seq mappings)
    (let [filename (first mappings)
          next-mappings (next (next mappings))]
      (if-let [dependencies (second mappings)]
        (let [ctx (gensym)
              pred (build-dependency-pred ctx dependencies)]
          `(let [validator# (fn [~ctx]
                              (if ~pred
                                ~ctx
                                (assoc-in ~ctx [:errors :file-dependencies ~filename]
                                          (error-message-for '~dependencies))))]
             (merge {~filename validator#} (build-dependencies ~@next-mappings))))
        (throw (IllegalArgumentException.
                "build-dependencies requires an even number of forms"))))))

(def file-dependencies
  (build-dependencies
   "ballot.txt" (and "contest.txt"
                     "electoral_district.txt"
                     (or "precinct_electoral_district.txt"
                         "precinct_split_electoral_district.txt")
                     (or "referendum.txt"
                         (and "candidate.txt"
                              "ballot_candidate.txt")))
   "ballot_candidate.txt" (and "contest.txt"
                               "electoral_district.txt"
                               (or "precinct_electoral_district.txt"
                                   "precinct_split_electoral_district.txt")
                               "ballot.txt"
                               "candidate.txt")
   "ballot_line_result.txt" (and "contest.txt"
                                 (or "electoral_district.txt"
                                     "locality.txt"
                                     "state.txt"
                                     "precinct.txt"
                                     "precinct_split.txt")
                                 "electoral_district.txt"
                                 (or "precinct_electoral_district.txt"
                                     "precinct_split_electoral_district.txt")
                                 "ballot.txt"
                                 (or (and "ballot_candidate.txt"
                                          "candidate.txt")
                                     (and "referendum.txt"
                                          "referendum_ballot_response.txt"
                                          "ballot_response.txt")))
   "ballot_response.txt" (and "contest.txt"
                              "electoral_district.txt"
                              (or "precinct_electoral_district.txt"
                                  "precinct_split_electoral_district.txt")
                              "ballot.txt"
                              "referendum.txt")
   "candidate.txt" (and "contest.txt"
                        "electoral_district.txt"
                        (or "precinct_electoral_district.txt"
                            "precinct_split_electoral_district.txt")
                        "ballot.txt"
                        "ballot_candidate.txt")
   "contest.txt" (and "electoral_district.txt"
                      (or "precinct_electoral_district.txt"
                          "precinct_split_electoral_district.txt")
                      "ballot.txt"
                      (or "ballot_candidate.txt"
                          "referendum.txt"))
   "contest_result.txt" (and "contest.txt"
                             (or "electoral_district.txt"
                                 "locality.txt"
                                 "state.txt"
                                 "precinct.txt"
                                 "precinct_split.txt")
                             "electoral_district.txt"
                             (or "precinct_electoral_district.txt"
                                 "precinct_split_electoral_district.txt")
                             "ballot.txt"
                             (or"ballot_candidate.txt"
                                "referendum.txt"))
   "custom_ballot.txt" (and "contest.txt"
                            "electoral_district.txt"
                            (or "precinct_electoral_district.txt"
                                "precinct_split_electoral_district.txt")
                            "ballot.txt")
   "custom_ballot_ballot_response" (and "contest.txt"
                                        "electoral_district.txt"
                                        (or "precinct_electoral_district.txt"
                                            "precinct_split_electoral_district.txt")
                                        "ballot.txt"
                                        "custom_ballot.txt")
   "early_vote_site.txt" (or "locality_early_vote_site.txt"
                             "precinct_early_vote_site.txt"
                             "state_early_vote_site.txt")
   "election_official.txt" "election_administration.txt"
   "electoral_district.txt" (or "precinct_electoral_district.txt"
                                "precinct_split_electoral_district.txt")
   "locality_early_vote_site.txt" (and "locality.txt"
                                       "early_vote_site.txt")
   "polling_location.txt" (or "precinct_polling_location.txt"
                              "precinct_polling_location.txt")
   "precinct_early_vote_site.txt" (and "precinct.txt"
                                       "early_vote_site.txt")
   "precinct_electoral_district.txt" (and "precinct.txt"
                                          "electoral_district.txt")
   "precinct_polling_location.txt" (and "precinct.txt"
                                        "polling_location.txt")
   "precinct_split.txt" "precinct.txt"
   "precinct_split_electoral_district.txt" (and "precinct_split.txt"
                                                "electoral_district.txt")
   "precinct_split_polling_location.txt" (and "precinct_split.txt"
                                              "polling_location.txt")
   "referendum.txt" (and "contest.txt"
                         "electoral_district.txt"
                         (or "precinct_electoral_district.txt"
                             "precinct_split_electoral_district.txt")
                         "ballot.txt")
   "referendum_ballot_response.txt" (and "contest.txt"
                                         "electoral_district.txt"
                                         (or "precinct_electoral_district.txt"
                                             "precinct_split_electoral_district.txt")
                                         "ballot.txt"
                                         "referendum.txt"
                                         "ballot_response.txt")
   "state_early_vote_site.txt" (and "state.txt"
                                    "early_vote_site.txt")
   "street_segment.txt" (or "precinct.txt"
                            "precinct_split.txt")))

(defn- validate-file-dependencies [ctx [file validator]]
  (if (csv/find-input-file ctx file)
    (validator ctx)
    ctx))

(defn validate-dependencies
  "Create a validator that validates that the dependencies of each
  file included in the context is met. file-dependencies should be
  built with build-dependencies."
  [file-dependencies]
  (fn [ctx]
    (reduce validate-file-dependencies ctx file-dependencies)))
