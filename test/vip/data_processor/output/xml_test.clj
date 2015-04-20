(ns vip.data-processor.output.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.output.xml :refer :all]
            [clojure.xml :as xml])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(deftest create-xml-file-test
  (testing "adds an :xml-output-file key to a context"
    (let [ctx {:filename "create-xml-test"}
          out-ctx (create-xml-file ctx)]
      (is (:xml-output-file out-ctx)))))

(deftest write-xml-test
  (testing "writes XML to :xml-output-file generated from :xml-children"
    (let [temp-file (Files/createTempFile "write-xml-test" ".xml" (into-array FileAttribute []))
          xml-children [{:tag :foo :attrs {:bar "baz"} :content ["test"]}]
          ctx {:xml-output-file temp-file
               :xml-children xml-children}
          out-ctx (write-xml ctx)
          xml (xml/parse (.toString temp-file))]
      (is (= :vip_object (:tag xml)))
      (is (= vip-object-attrs (:attrs xml)))
      (is (= xml-children (:content xml))))))
