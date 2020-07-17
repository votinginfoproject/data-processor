(ns vip.data-processor.db.translations.util
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [vip.data-processor.db.postgres :as postgres]))

(defn id-path [path]
  (str path ".id"))

(defn index-generator
  "Returns a function that counts up from `initial-value` by 1.

  λ (let [idx (index-generator 13)]
      (dotimes [i 5]
        (println \"Iteration\" i \"yields\" (idx))))
  Iteration 0 yields 13
  Iteration 1 yields 14
  Iteration 2 yields 15
  Iteration 3 yields 16
  Iteration 4 yields 17
  nil"
  [initial-value]
  (let [counter (atom (dec initial-value))]
    (fn []
      (swap! counter inc))))

(defn column->xml-elment [column-name]
  (as-> column-name ?
    (name ?)
    (str/split ? #"_")
    (map str/capitalize ?)
    (str/join ?)))

(defn path->simple-path [path]
  (str/join "." (str/split path #"\.\d+\.?")))

(defn simple-value->ltree
  ([column-name]
   (simple-value->ltree column-name (column->xml-elment column-name)))
  ([column-name xml-element]
   (fn [idx-fn base-path row]
     (let [value (str (get row column-name))]
       (when-not (str/blank? value)
         (let [path (str base-path "." xml-element "." (idx-fn))]
           (list
            {:path path
             :simple_path (path->simple-path path)
             :parent_with_id (id-path base-path)
             :value value}))))))
  ([column-name xml-element parent-with-id]
   (fn [idx-fn base-path row]
     (let [value (get row column-name)]
       (when-not (str/blank? value)
         (let [path (str base-path "." xml-element "." (idx-fn))]
           (list
            {:path path
             :simple_path (path->simple-path path)
             :parent_with_id parent-with-id
             :value value})))))))

(defn internationalized-text->ltree
  ([column-name]
   (fn [idx-fn base-path row]
     (let [parent-with-id (id-path base-path)
           ltree-fn (internationalized-text->ltree column-name parent-with-id)]
       (ltree-fn idx-fn base-path row))))
  ([column-name parent-with-id]
   (let [xml-element (column->xml-elment column-name)]
     (internationalized-text->ltree column-name xml-element parent-with-id)))
  ([column-name xml-element parent-with-id]
   (fn [idx-fn base-path row]
     (let [value (get row column-name)]
       (when-not (str/blank? value)
         (let [index (idx-fn)
               text-path (str base-path "." xml-element "." index ".Text.0")
               lang-path (str text-path ".language")]
           (list
            {:path lang-path
             :simple_path (path->simple-path lang-path)
             :parent_with_id parent-with-id
             :value "en"}
            {:path text-path
             :simple_path (path->simple-path text-path)
             :parent_with_id parent-with-id
             :value value})))))))

(defn external-identifiers->ltree
  [idx-fn parent-path row]
  (when-not (and (str/blank? (:external_identifier_type row))
                 (str/blank? (:external_identifier_othertype row))
                 (str/blank? (:external_identifier_value row)))
    (let [index (idx-fn)
          base-path (str parent-path
                         ".ExternalIdentifiers."
                         index
                         ".ExternalIdentifier.0")
          parent-with-id (id-path parent-path)
          sub-idx-fn (index-generator 0)]
      (mapcat #(% sub-idx-fn base-path row)
              [(simple-value->ltree :external_identifier_type "Type" parent-with-id)
               (simple-value->ltree :external_identifier_othertype "OtherType" parent-with-id)
               (simple-value->ltree :external_identifier_value "Value" parent-with-id)]))))

(defn latlng->ltree
  ([]
   (fn [idx-fn parent-path row]
     (let [parent-with-id (id-path parent-path)
           ltree-fn (latlng->ltree parent-with-id)]
       (ltree-fn idx-fn parent-path row))))
  ([parent-with-id]
   (latlng->ltree {} parent-with-id))
  ([field-mapping parent-with-id]
   (fn [idx-fn parent-path row]
     (let [lat-key (get field-mapping :latitude :latitude)
           lng-key (get field-mapping :longitude :longitude)
           source-key (get field-mapping :latlng_source :latlng_source)]
       (when-not (and (str/blank? (get row lat-key))
                      (str/blank? (get row lng-key)))
         (let [index (idx-fn)
               base-path (str parent-path ".LatLng." index)
               sub-idx-fn (index-generator 0)]
           (mapcat #(% sub-idx-fn base-path row)
                   [(simple-value->ltree lat-key "Latitude" parent-with-id)
                    (simple-value->ltree lng-key "Longitude" parent-with-id)
                    (simple-value->ltree source-key "Source" parent-with-id)]))))))
  ([idx-fn parent-path row]
   (let [ltree-fn (latlng->ltree)]
     (ltree-fn idx-fn parent-path row))))

(defn term->ltree
  [idx-fn parent-path row]
  (when-not (and (str/blank? (:term_type row))
                 (str/blank? (:term_start_date row))
                 (str/blank? (:term_end_date row)))
    (let [index (idx-fn)
          base-path (str parent-path ".Term." index)
          parent-with-id (id-path parent-path)
          sub-idx-fn (index-generator 0)]
      (mapcat #(% sub-idx-fn base-path row)
              [(simple-value->ltree :term_start_date "StartDate" parent-with-id)
               (simple-value->ltree :term_end_date "EndDate" parent-with-id)
               (simple-value->ltree :term_type "Type" parent-with-id)]))))

(defn election-notice->ltree
  [idx-fn parent-path row]
  (when-not (and (str/blank? (:election_notice_text row))
                 (str/blank? (:election_notice_uri row)))
    (let [index (idx-fn)
          base-path (str parent-path ".ElectionNotice." index)
          parent-with-id (id-path parent-path)
          sub-idx-fn (index-generator 0)]
      (mapcat #(% sub-idx-fn base-path row)
              [(internationalized-text->ltree :election_notice_text "NoticeText" parent-with-id)
               (simple-value->ltree :election_notice_uri "NoticeUri" parent-with-id)]))))

(defn ltreeify [row]
  (-> row
      (update :path postgres/path->ltree)
      (update :simple_path postgres/path->ltree)
      (update :parent_with_id postgres/path->ltree)))

(defn prep-for-insertion [import-id rows]
  (map (comp ltreeify
             #(assoc % :results_id import-id))
       rows))

(defn transformer
  "Translate a set of rows from `row-fn` using `ltree-fn`, then load
  that into the database. `row-fn` should be a function of import-id."
  [row-fn ltree-fn]
  (fn [{:keys [ltree-index import-id] :as ctx}]
    (let [idx-fn (index-generator ltree-index)
          ltree-rows (mapcat (partial ltree-fn idx-fn)
                             (row-fn import-id))
          rows (prep-for-insertion import-id ltree-rows)]
      (if (seq rows)
        (-> ctx
            (postgres/bulk-import postgres/xml-tree-values rows)
            (assoc :ltree-index (idx-fn)))
        ctx))))

(defn transformer-with-conn
  "Like `transformer`, but with a database connection passed to `row-fn`.
  Useful when `row-fn` needs a single connection, e.g., when using a
  database cursor."
  [row-fn ltree-fn]
  (fn [{:keys [ltree-index import-id] :as ctx}]
    (jdbc/with-db-connection [conn (postgres/db-spec)]
      (let [idx-fn (index-generator ltree-index)
            ltree-rows (mapcat (partial ltree-fn idx-fn)
                               (row-fn conn import-id))
            rows (prep-for-insertion import-id ltree-rows)]
        (if (seq rows)
          (-> ctx
              (postgres/bulk-import postgres/xml-tree-values rows)
              (assoc :ltree-index (idx-fn)))
          ctx)))))
