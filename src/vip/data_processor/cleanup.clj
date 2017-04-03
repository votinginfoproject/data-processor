(ns vip.data-processor.cleanup
  (:require [clojure.tools.logging :as log]))

(defn cleanup [{:keys [to-be-cleaned] :as ctx}]
  (doseq [file to-be-cleaned]
    (let [file (if (instance? java.nio.file.Path file)
                 (.toFile file)
                 file)]
      (when (.exists file)
        (log/info "Removing" (.toString file))
        (.delete file))))
  ctx)
