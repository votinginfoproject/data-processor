FROM clojure:lein-2.8.1-alpine

RUN mkdir -p /usr/src/data-processor
WORKDIR /usr/src/data-processor

COPY project.clj /usr/src/data-processor/
RUN lein deps

COPY . /usr/src/data-processor

RUN lein test
RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" data-processor-standalone.jar

CMD java -Xmx8g -jar data-processor-standalone.jar
