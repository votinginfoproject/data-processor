(ns vip.data-processor.util)

(defn flatten-keys* [a ks m]
  (if (map? m)
    (reduce into (map (fn [[k v]] (flatten-keys* a (conj ks k) v)) (seq m)))
    (assoc a ks m)))

(defn flatten-keys [m] (flatten-keys* {} [] m))
