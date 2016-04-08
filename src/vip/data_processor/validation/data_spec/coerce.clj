(ns vip.data-processor.validation.data-spec.coerce)

(defn boolean-value [x]
  (cond
    (re-find #"\A(?i:yes)\z" x) 1
    (re-find #"\A(?i:no)\z" x) 0
    :else nil))

(defn postgres-boolean [x]
  (cond
    (re-find #"\A(?i:true)\z" x) true
    (re-find #"\A(?i:false)\z" x) false
    :else nil))

(defn unsafe-coerce-integer [v]
  (cond (= v "") nil
        (string? v) (Integer/parseInt v)
        (number? v) v
        :else nil))

(defn coerce-boolean [v]
  (condp = v
    1 true
    0 false
    nil))

(defn unsafe-coerce-date [v]
  (cond (= v "") nil
        (string? v) (-> v
                        org.joda.time.DateTime/parse
                        .getMillis
                        java.sql.Date.)
        (instance? java.sql.Date v) v
        :else nil))

(defn safe-coerce [coercion-fn]
  (fn [v]
    (try
      (coercion-fn v)
      (catch Throwable t
        nil))))

(def coerce-integer (safe-coerce unsafe-coerce-integer))
(def coerce-date (safe-coerce unsafe-coerce-date))
