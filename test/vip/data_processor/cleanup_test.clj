(ns vip.data-processor.cleanup-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.cleanup :refer :all]))

(deftest cleanup-test
  (testing "Removes files in :to-be-cleaned"
    (let [file-that-exists (java.io.File/createTempFile (name (gensym)) ".tmp")
          file-that-does-not-exist (java.io.File. (str file-that-exists ".nope"))
          path-that-exists (.toPath (java.io.File/createTempFile (name (gensym)) ".tmp"))
          path-that-does-not-exist (.toPath (java.io.File. (str path-that-exists ".nope")))
          ctx {:to-be-cleaned [file-that-exists
                               file-that-does-not-exist
                               path-that-exists
                               path-that-does-not-exist]}]
      (is (.exists file-that-exists))
      (is (not (.exists file-that-does-not-exist)))
      (is (.exists (.toFile path-that-exists)))
      (is (not (.exists (.toFile path-that-does-not-exist))))
      (testing "after cleanup"
        (cleanup ctx)
        (testing "removes files that had existed"
          (is (not (.exists file-that-exists))))
        (testing "removes paths that had existed"
          (is (not (.exists (.toFile path-that-exists)))))))))
