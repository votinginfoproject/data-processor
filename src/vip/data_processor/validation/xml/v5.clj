(ns vip.data-processor.validation.xml.v5
  (:require [clojure.data.xml :as xml]
            [clojure.string :as str]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as data-spec.v5-1]))

(defn element->database-key [element]
  (->> element
       :tag
       name
       (re-seq #"[A-Z][a-z0-9]*")
       (map str/lower-case)
       (str/join "_")
       keyword))

(defn ss->map [ss]
  (-> (->> ss
           :content
           (map (juxt element->database-key
                      (comp first :content)))
           (into {:id (get-in ss [:attrs :id])}))
      (select-keys [:id
                    :includes_all_addresses
                    :address_direction
                    :city
                    :odd_even_both
                    :precinct_id
                    :start_house_number
                    :end_house_number
                    :state
                    :street_direction
                    :street_name
                    :street_suffix
                    :zip])))

(defn load-xml-street-segments
  [{:keys [import-id] :as ctx}]
  (let [xml-file (first (:input ctx))]
    (with-open [reader (util/bom-safe-reader xml-file)]
      (->> reader
           xml/parse
           :content
           (filter #(= (:tag %) :StreetSegment))
           (map (fn [ss] (-> ss
                             ss->map
                             (assoc :results_id import-id))))
           (data-spec/coerce-rows (:columns data-spec.v5-1/street-segments))
           (postgres/bulk-import ctx postgres/v5-1-street-segments)))))
