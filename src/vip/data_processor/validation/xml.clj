(ns vip.data-processor.validation.xml
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [vip.data-processor.db.sqlite :as sqlite]))

(defn parse-xml [ctx]
  (let [xml-file (first (:input ctx))
        parsed-xml (xml/parse (io/reader xml-file))]
    (assoc ctx :xml parsed-xml)))
