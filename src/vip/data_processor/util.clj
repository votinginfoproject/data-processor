(ns vip.data-processor.util
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn flatten-keys* [a ks m]
  (if (map? m)
    (reduce into (map (fn [[k v]] (flatten-keys* a (conj ks k) v)) (seq m)))
    (assoc a ks m)))

(defn flatten-keys [m] (flatten-keys* {} [] m))

(defn format-date [date]
  ;; Currently, invalid date formats (eg. 8/7/2015) are somehow
  ;; making it through to the database. This fix will keep
  ;; the entire feed from breaking Metis while we investigate
  ;; the issue.
  (let [format-fn (fn [d] (try (->> d java.util.Date.
                                    (.format
                                     (java.text.SimpleDateFormat.
                                      "yyyy-MM-dd")))
                               (catch Throwable _
                                 nil)))]
    (if (and (seq date) (re-find #"\/" date))
      (format-fn date)
      date)))

(def BOM 0xFEFF)

(defn bom-safe-reader
  "Returns a reader from io/reader, which has advanced past a byte
  order marker if one exists."
  [x & opts]
  (let [reader (apply io/reader (io/as-file x) opts)]
    (.mark reader 10)
    (let [first-char (.read reader)]
      (if (= first-char BOM)
        (log/info x "had byte order marker")
        (.reset reader))
      reader)))

(defn find-csv-source-file
  [ctx filename]
  (letfn [(->file [f])])
  (->> ctx
       :csv-source-file-paths
       (filter #(= filename (str/lower-case (.getName (io/as-file %)))))
       first))

(defn version-without-patch
  "Strip the patch-level from `spec-version`. Assumes three or fewer version
  numbers, e.g., \"5.1.2\" becomes \"5.1\"."
  [version]
  (when-not (empty? version)
    (str/replace version #"^(\d+\.\d+)\.\d+$" "$1")))
