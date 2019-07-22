(ns vip.data-processor.validation.zip-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing is use-fixtures run-tests]]
   [vip.data-processor.validation.zip :as zip]))

(deftest wont-open-too-big-zipfile
  (let [zipfile (io/resource "example.zip")
        uncompressed-size (zip/get-uncompressed-size zipfile)
        max-zipfile-size 786432]
    (testing "Don't proceed if the zipfile is bigger than the `max-zipfile-size'"
      (is (= {:input zipfile
              :stop (zip/too-big-msg uncompressed-size max-zipfile-size)}
             (zip/assoc-file {:input zipfile} max-zipfile-size))))

    (testing "Doesn't fail if max-zipfile-size is passed in as a string"
      (is (= {:input zipfile
              :stop (zip/too-big-msg uncompressed-size (str max-zipfile-size))}
             (zip/assoc-file {:input zipfile} (str max-zipfile-size)))))))
