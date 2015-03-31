(ns vip.data-processor.validation.db.util)

(defn column-name [table column]
  (keyword (str table "." column)))
