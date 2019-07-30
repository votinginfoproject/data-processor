(ns vip.data-processor.validation.zip-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing is use-fixtures run-tests]]
   [vip.data-processor.validation.zip :as zip])
  (:import [java.nio.file Paths]))

(deftest assert-max-zip-size
  (let [zipfile (Paths/get (.toURI (io/resource "example.zip")))
        uncompressed-size (zip/get-uncompressed-size zipfile)
        max-zipfile-size 786432]
    (testing "Don't proceed if the zipfile is bigger than the `max-zipfile-size'"
      (is (= {:file zipfile
              :stop (zip/too-big-msg uncompressed-size max-zipfile-size)}
             (zip/assert-max-zip-size {:file zipfile} max-zipfile-size))))

    (testing "Doesn't fail if max-zipfile-size is passed in as a string"
      (is (= {:file zipfile
              :stop (zip/too-big-msg uncompressed-size (str max-zipfile-size))}
             (zip/assert-max-zip-size {:file zipfile} (str max-zipfile-size)))))))
