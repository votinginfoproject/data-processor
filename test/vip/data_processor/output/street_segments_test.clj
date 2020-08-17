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

(defn conj-node-to-array
  [nodes a idx]
  (let [item (.item nodes idx)]
    (conj a [(xml-name->keyword (.getNodeName item)) (.getTextContent item)])))

(defn street-segment-node->array
  [node]
  (let [ss-nodes (.getChildNodes node)]
    (->> ss-nodes
         .getLength
         (range 0)
         (reduce (partial conj-node-to-array ss-nodes) []))))

(defn xpath-query
  [filepath query-string]
  (let [xpath (.newXPath (XPathFactory/newInstance))
        isource (InputSource. filepath)
        nodes (.evaluate xpath query-string isource XPathConstants/NODESET)]
    (->> (.getLength nodes)
         (range 0)
         (mapv #(street-segment-node->array (.item nodes %))))))

(defn copy-to-temp-file
  [resource-path]
  (let [src-tmp-copy (doto (fs/temp-file "test-output-xml-tmp-")
                       .deleteOnExit)]
    (-> (io/resource resource-path)
        io/file
        (fs/copy src-tmp-copy))))

(def well-ordered-children
  [:address-direction :city :includes-all-addresses
   :includes-all-streets :odd-even-both :precinct-id
   :start-house-number :end-house-number :state
   :street-direction :street-name :street-suffix
   :unit-number :zip])

(defn well-ordered-children?
  "Function to ensure that the child nodes are in the order as defined
   by the XML sequence structure for street segments (though there may be
   some missing children and that's fine in this instance)"
  [children]
  (letfn [(index-check [last-idx next-key]
            (let [current-idx (.indexOf well-ordered-children next-key)]
              (if (> current-idx last-idx)
                current-idx
                (throw (RuntimeException.
                        (str next-key " index is out of order, found at "
                             current-idx " and prior child key was found at "
                             last-idx))))))]
    (try (reduce index-check -1 children)
         true
         (catch RuntimeException ex
           (println (.getMessage ex))
           false))))

(deftest inserts-street-segments
  ;; Someday I'll fix this so we don't have to call
  ;; `with-redefs`, but today is not that day
  (let [errors (atom [])]
    (with-redefs [errors/record-error! (fn [_ error] (swap! errors conj error))
                  ss/load-precinct-ids (constantly #{"p001" "p002"})]

      (testing "Given a street segment csv input file and an xml output
              file, inserts street segments into the xml output file,
              returning a new copy."

        (let [output-file-copy (copy-to-temp-file "xml/v5_no_street_segments.xml")
              file-paths [(.toPath (io/file (io/resource "csv/5-2/street_segment.txt")))]
              {:keys [xml-output-file]} (ss/process-xml
                                         {:xml-output-file (.toPath output-file-copy)
                                          :csv-source-file-paths file-paths})
              nodes (xpath-query (.toString xml-output-file) "/VipObject/StreetSegment")
              nodes-as-maps (map #(into {} %) nodes)]

          (is (= 2 (count nodes)))
          (is (= #{"p001" "p002"} (set (map :precinct-id nodes-as-maps))))
          (is (= #{"20001" "20002"} (set (map :zip nodes-as-maps))))
          (is (= #{"DC"} (set (map :state nodes-as-maps))))
          (is (= #{"Delaware" "Wisconsin"} (set (map :street-name nodes-as-maps))))
          (let [children-keys (->> nodes first (map first))]
            (is (well-ordered-children? children-keys) (pr-str children-keys)))
          (let [children-keys (->> nodes second (map first))]
            (is (well-ordered-children? children-keys) (pr-str children-keys)))))

      (testing "Catches bad street segments, including those with missing
              required fields and non-existent precinct-ids"

        (let [output-file-copy (copy-to-temp-file "xml/v5_no_street_segments.xml")
              file-paths [(->> "csv/5-2/bad-street-segments/street_segment.txt"
                               io/resource
                               io/file
                               .toPath)]
              {:keys [xml-output-file]} (ss/process-xml
                                         {:xml-output-file (.toPath output-file-copy)
                                          :csv-source-file-paths file-paths})
              nodes (xpath-query (.toString xml-output-file) "/VipObject/StreetSegment")]

          (is (= #{"City" "State"}
                 (set (keep #(get-in % [:error-data 0 :missing-field]) @errors))))

          (is (= #{"p003"}
                 (set (keep #(get-in % [:error-data 0 :bad-precinct-id]) @errors))))

          )))))
