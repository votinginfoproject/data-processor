(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [vip.data-processor.validation.csv.value-format :as format]
            [vip.data-processor.db.sqlite :as sqlite]))

(defn boolean-value [x]
  (if (re-find #"\A(?i:yes)\z" x) 1 0))

(def csv-specs
  [{:filename "ballot.txt"
    :table :ballots
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "referendum_id" :format format/all-digits}
              {:name "custom_ballot_id" :format format/all-digits}
              {:name "write_in" :format format/yes-no :translate boolean-value}
              {:name "image_url" :format format/url}]}
   {:filename "ballot_candidate.txt"
    :table :ballot-candidates
    :columns [{:name "ballot_id" :required true :format format/all-digits}
              {:name "candidate_id" :required true :format format/all-digits}]}
   {:filename "ballot_line_result.txt"
    :table :ballot-line-results
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "contest_id" :required true :format format/all-digits}
              {:name "jurisdiction_id" :required true :format format/all-digits}
              {:name "entire_district" :required true :format format/yes-no :translate boolean-value}
              {:name "candidate_id" :format format/all-digits}
              {:name "ballot_response_id" :format format/all-digits}
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
              {:name "election_id" :required true :format format/all-digits}
              {:name "electoral_district_id" :required true :format format/all-digits}
              {:name "type" :required true :format format/contest-election-type}
              {:name "partisan" :format format/yes-no :translate boolean-value}
              {:name "primary_party"}
              {:name "electorate_specifications"}
              {:name "special" :format format/yes-no :translate boolean-value}
              {:name "office"}
              {:name "filing_closed_date" :format format/date}
              {:name "number_elected" :format format/all-digits}
              {:name "number_voting_for" :format format/all-digits}
              {:name "ballot_id" :format format/all-digits}
              {:name "ballot_placement" :format format/all-digits}]}
   {:filename "contest_result.txt"
    :table :contest-results
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "contest_id" :required true :format format/all-digits}
              {:name "jurisdiction_id" :required true :format format/all-digits}
              {:name "entire_district" :required true :format format/yes-no}
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
    :columns [{:name "custom_ballot_id" :required true :format format/all-digits}
              {:name "ballot_response_id" :required true :format format/all-digits}
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
              {:name "state_id" :required true :format format/all-digits}
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
              {:name "eo_id" :format format/all-digits}
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
              {:name "state_id" :required true :format format/all-digits}
              {:name "type" :required true :format format/locality-type}
              {:name "election_administration_id" :format format/all-digits}]}
   {:filename "locality_early_vote_site.txt"
    :table :locality-early-vote-sites
    :columns [{:name "locality_id" :required true :format format/all-digits}
              {:name "early_vote_site_id" :required true :format format/all-digits}]}
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
              {:name "locality_id" :required true :format format/all-digits}
              {:name "ward"}
              {:name "mail_only" :format format/yes-no :translate boolean-value}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_split.txt"
    :table :precinct-splits
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "precinct_id" :required true :format format/all-digits}
              {:name "ballot_style_image_url" :format format/url}]}
   {:filename "precinct_early_vote_site.txt"
    :table :precinct-early-vote-sites
    :columns [{:name "precinct_id" :required true :format format/all-digits}
              {:name "early_vote_site_id" :required true :format format/all-digits}]}
   {:filename "precinct_electoral_district.txt"
    :table :precinct-electoral-districts
    :columns [{:name "precinct_id" :required true :format format/all-digits}
              {:name "electoral_district_id" :required true :format format/all-digits}]}
   {:filename "precinct_polling_location.txt"
    :table :precinct-polling-locations
    :columns [{:name "precinct_id" :required true :format format/all-digits}
              {:name "polling_location_id" :required true :format format/all-digits}]}
   {:filename "precinct_split_electoral_district.txt"
    :table :precinct-split-electoral-districts
    :columns [{:name "precinct_split_id" :required true :format format/all-digits}
              {:name "electoral_district_id" :required true :format format/all-digits}]}
   {:filename "precinct_split_polling_location.txt"
    :table :precinct-split-polling-locations
    :columns [{:name "precinct_split_id" :required true :format format/all-digits}
              {:name "polling_location_id" :required true :format format/all-digits}]}
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
     :columns [{:name "referendum_id" :required true :format format/all-digits}
               {:name "ballot_response_id" :required true :format format/all-digits}
               {:name "sort_order" :format format/all-digits}]}
    {:filename "state_early_vote_site.txt"
     :table :state-early-vote-sites
     :columns [{:name "state_id" :required true :format format/all-digits}
               {:name "early_vote_site_id" :required true :format format/all-digits}]}
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
               {:name "precinct_id" :required true :format format/all-digits}
               {:name "precinct_split_id" :format format/all-digits}]}
   {:filename "source.txt"
    :table :sources
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "vip_id" :required true :format format/all-digits}
              {:name "datetime" :required true :format format/datetime}
              {:name "description"}
              {:name "organization_url" :format format/url}
              {:name "feed_contact_id" :format format/all-digits}
              {:name "tou_url" :format format/url}]}
   {:filename "state.txt"
    :table :states
    :columns [{:name "id" :required true :format format/all-digits}
              {:name "name" :required true}
              {:name "election_administration_id" :format format/all-digits}]}])

(def csv-filenames (set (map :filename csv-specs)))

(defn file-name [file]
  (.getName file))

(defn good-filename? [file]
  (let [filename (file-name file)]
    (contains? csv-filenames filename)))

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
    {:headers headers
     :contents (map (partial zipmap headers) rows)
     :bad-rows (remove (fn [[_ row]] (= (count headers) (count row)))
                       (map list (iterate inc 2) rows))}))

(defn report-bad-rows [ctx filename expected-number bad-rows]
  (if-not (empty? bad-rows)
    (reduce (fn [ctx [line-number row]]
              (assoc-in ctx [:critical filename line-number "Number of values"]
                        (str "Expected " expected-number " values, found " (count row))))
            ctx bad-rows)
    ctx))

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (.getName %)))
       first))

(defn create-format-rule
  "Create a function that applies a format check for a specific row of
  a CSV import."
  [filename {:keys [name required format]}]
  (let [{:keys [check message]} format
        test-fn (cond
                 (sequential? check) (fn [val] (some #{val} check))
                 (instance? clojure.lang.IFn check) check
                 (instance? java.util.regex.Pattern check) (fn [val] (re-find check val))
                 :else (constantly true))]
    (fn [ctx row line-number]
      (let [val (row name)]
        (if (empty? val)
          (if required
            (assoc-in ctx [:fatal filename line-number name] (str "Missing required column: " name))
            ctx)
          (if (test-fn val)
            ctx
            (assoc-in ctx [:errors filename line-number name] message)))))))

(defn create-format-rules [{:keys [filename columns]}]
  (map (partial create-format-rule filename) columns))

(defn apply-format-rules [rules ctx row line-number]
  (reduce (fn [ctx rule] (rule ctx row line-number)) ctx rules))

(defn validate-format-rules [ctx rows csv-spec]
  (let [format-rules (create-format-rules csv-spec)
        line-number (atom 1)]
    (reduce (fn [ctx row]
              (apply-format-rules format-rules ctx row (swap! line-number inc)))
            ctx rows)))

(defn create-translation-fn [{:keys [name translate]}]
  (fn [row]
    (if-let [cell (row name)]
      (assoc row name (translate cell))
      row)))

(defn translation-fns [columns]
  (->> columns
       (filter :translate)
       (map create-translation-fn)))

(defn load-csv [ctx {:keys [filename table columns] :as csv-spec}]
  (if-not (.endsWith filename ".txt")
    (assoc-in ctx [:critical filename "File extension"] "File is not a .txt file.")
    (if-let [file-to-load (find-input-file ctx filename)]
      (with-open [in-file (io/reader file-to-load)]
      (let [sql-table (get-in ctx [:tables table])
            column-names (map :name columns)
            required-header-names (->> columns (filter :required) (map :name))
            {:keys [headers contents bad-rows]} (read-csv-with-headers in-file)
            extraneous-headers (seq (set/difference (set headers) (set column-names)))
            ctx (if extraneous-headers
                  (assoc-in ctx [:warnings filename :extraneous-headers]
                            (str/join ", " extraneous-headers))
                  ctx)
            ctx (report-bad-rows ctx filename (count headers) bad-rows)
            contents (map #(select-keys % column-names) contents)]
        (if (empty? (set/intersection (set headers) (set column-names)))
          (assoc-in ctx [:critical filename :headers] "No header row")
          (if-let [missing-headers (seq (set/difference (set required-header-names) (set headers)))]
            (assoc-in ctx [:critical filename :headers]
                      (str "Missing headers: " (str/join ", " missing-headers)))
            (let [ctx (validate-format-rules ctx contents csv-spec)
                  transforms (apply comp (translation-fns columns))
                  transformed-contents (map transforms contents)]
              (sqlite/bulk-import transformed-contents sql-table)
              ctx)))))
      ctx)))

(defn load-csvs [csv-specs]
  (fn [ctx]
    (reduce load-csv ctx csv-specs)))

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
