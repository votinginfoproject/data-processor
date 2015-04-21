(ns vip.data-processor.output.xml-helpers)

(defmacro xml-node [name]
  (let [tag (keyword name)]
    `{:tag ~tag :content [(str ~name)]}))

(defmacro boolean-xml-node [name]
  (let [tag (keyword name)]
    `{:tag ~tag :content [(condp = ~name
                            1 "yes"
                            0 "no"
                            nil)]}))

(defn empty-content? [c]
  (or (nil? c)
      (empty? (first (:content c)))))

(defn remove-empties [content]
  (remove empty-content? content))

(defn simple-xml
  ([tag content]
   {:tag tag :content (remove-empties content)})
  ([tag id content]
   {:tag tag :attrs {:id id} :content (remove-empties content)}))
