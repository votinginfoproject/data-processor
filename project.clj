(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "0.1.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.logging "0.5.0-alpha.1"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [joda-time "2.10.2"]
                 [clj-time "0.15.1"]
                 [amazonica "0.3.143"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client]]
                 [com.amazonaws/aws-java-sdk-core "1.11.569"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.569"]
                 [com.cognitect.aws/api "0.8.273"]
                 [com.cognitect.aws/endpoints "1.1.11.507"]
                 [com.cognitect.aws/sns "697.2.391.0"]
                 [democracyworks/squishy "3.0.2"
                    :exclusions [joda-time
                                 org.slf4j/slf4j-simple
                                 org.slf4j/slf4j-api]]
                 [org.clojure/core.async "0.4.500"]
                 [democracyworks/utility-fns "0.2.0"]
                 [net.lingala.zip4j/zip4j "1.3.3"]
                 [turbovote.resource-config "0.2.1"]
                 [joplin.jdbc "0.3.11"
                  :exclusions [ragtime/ragtime.jdbc]]
                 [ragtime/ragtime.jdbc "0.6.4"]
                 [korma "0.4.3"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.5"
                  :exclusions [org.slf4j/slf4j-simple
                               org.slf4j/slf4j-api]]
                 [org.xerial/sqlite-jdbc "3.27.2.1"]
                 [commons-lang/commons-lang "2.6"]
                 [xerces/xercesImpl "2.12.0"]
                 [com.fasterxml.woodstox/woodstox-core "5.2.1"]
                 [me.raynes/fs "1.4.6"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[com.github.kyleburton/clj-xpath "1.4.11"]]}
             :dev {:source-paths ["dev-src"]
                   :resource-paths ["dev-resources"]
                   :main dev.core}}
  :test-selectors {:default (complement :postgres)
                   :postgres :postgres
                   :all (constantly true)}
  :repl-options {:init (set! *print-length* 50)}
  :prep-tasks ["javac" "compile"]
  :main vip.data-processor)
