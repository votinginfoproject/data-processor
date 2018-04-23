(ns vip.data-processor.output.street-segments-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing is run-tests]]
   [me.raynes.fs :as fs]
   [vip.data-processor.errors :as errors]
   [vip.data-processor.output.street-segments :as ss]
   [vip.data-processor.validation.v5.util :refer [xml-name->keyword]])
  (:import
   [javax.xml.xpath XPathFactory XPathConstants]
   [org.xml.sax InputSource]))

(defn append-node-to-map
  [nodes m idx]
  (let [item (.item nodes idx)]
    (assoc m (xml-name->keyword (.getNodeName item)) (.getTextContent item))))

(defn street-segment-node->map
  [node]
  (let [ss-nodes (.getChildNodes node)]
    (->> ss-nodes 
         .getLength
         (range 0)
         (reduce (partial append-node-to-map ss-nodes) {}))))

(defn xpath-query
  [filepath query-string]
  (let [xpath (.newXPath (XPathFactory/newInstance))
        isource (InputSource. filepath)
        nodes (.evaluate xpath query-string isource XPathConstants/NODESET)]
    (->> (.getLength nodes)
         (range 0)
         (mapv #(street-segment-node->map (.item nodes %))))))

(defn copy-to-temp-file
  [resource-path]
  (let [src-tmp-copy (doto (fs/temp-file "test-output-xml-tmp-")
                       .deleteOnExit)]
    (-> (io/resource resource-path)
        io/file
        (fs/copy src-tmp-copy))))

(deftest inserts-street-segments
  ;; Someday I'll fix this so we don't have to call
  ;; `with-redefs`, but today is not that day
  (with-redefs [errors/record-error! (fn [& _])
                ss/load-precinct-ids (constantly #{"p001" "p002"})]

    (testing "Given a street segment csv input file and an xml output
              file, inserts street segments into the xml output file,
              returning a new copy."

      (let [output-file-copy (copy-to-temp-file "xml/v5_no_street_segments.xml")
            input-files [(io/file (io/resource "csv/5-1/street_segment.txt"))]
            {:keys [xml-output-file]} (ss/process-xml
                                       {:xml-output-file (.toPath output-file-copy)
                                        :input input-files})
            nodes (xpath-query (.toString xml-output-file) "/VipObject/StreetSegment")]

        (is (= 2 (count nodes)))
        (is (= #{"p001" "p002"} (set (map :precinct-id nodes))))
        (is (= #{"20001" "20002"} (set (map :zip nodes))))
        (is (= #{"DC"} (set (map :state nodes))))
        (is (= #{"Delaware" "Wisconsin"} (set (map :street-name nodes))))

        ))))
