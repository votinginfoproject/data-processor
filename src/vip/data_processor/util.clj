(ns vip.data-processor.util)

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
  (let [format-fn (fn [d] (->> d java.util.Date.
                               (.format 
                                (java.text.SimpleDateFormat. 
                                 "yyyy-MM-dd"))))]
    (if (and (seq date) (re-find #"\/" date))
      (format-fn date) date)))
