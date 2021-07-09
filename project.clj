(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/data.json "1.0.0"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.logging "1.1.0"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [joda-time "2.10.6"]
                 [clj-time "0.15.2"]
                 [amazonica "0.3.152"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client]]
                 [com.amazonaws/aws-java-sdk-core "1.11.821"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.821"]
                 [com.cognitect.aws/api "0.8.469"]
                 [com.cognitect.aws/endpoints "1.1.11.820"]
                 [com.cognitect.aws/sns "802.2.713.0"]
                 [democracyworks/squishy "3.1.0"
                    :exclusions [joda-time
                                 org.slf4j/slf4j-simple
                                 org.slf4j/slf4j-api]]
                 [org.clojure/core.async "1.2.603"]
                 [democracyworks/utility-fns "0.2.0"]
                 [net.lingala.zip4j/zip4j "2.6.1"]
                 [turbovote.resource-config "0.2.1"]
                 [joplin.jdbc "0.3.11"
                  :exclusions [ragtime/ragtime.jdbc]]
                 [ragtime/ragtime.jdbc "0.6.4"]
                 [korma "0.4.3"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.postgresql/postgresql "42.2.14"
                  :exclusions [org.slf4j/slf4j-simple
                               org.slf4j/slf4j-api]]
                 [org.xerial/sqlite-jdbc "3.28.0"]
                 [commons-lang/commons-lang "2.6"]
                 [xerces/xercesImpl "2.12.0"]
                 [com.fasterxml.woodstox/woodstox-core "6.2.1"]
                 [me.raynes/fs "1.4.6"]]
  :managed-dependencies [[com.mchange/c3p0 "0.9.5.5"]
                         [com.fasterxml.jackson.core/jackson-databind "2.11.1"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[com.github.kyleburton/clj-xpath "1.4.11"]]}
             :dev {:source-paths ["dev-src"]
                   :resource-paths ["dev-resources"]
                   :main dev.core}}
  :test-selectors {:default (complement :postgres)
                   :postgres :postgres
                   :all (constantly true)}
  :aliases {"migrate" ["with-profile" "+dev" "run" "-m" "dev.core/migrate"]
            "rollback" ["with-profile" "+dev" "run" "-m" "dev.core/rollback"]
            "pending" ["with-profile" "+dev" "run" "-m" "dev.core/pending"]
            "create" ["with-profile" "+dev" "run" "-m" "dev.core/create"]}
  :repl-options {:init (set! *print-length* 50)}
  :prep-tasks ["javac" "compile"]
  :main vip.data-processor)
