(ns vip.data-processor.validation.data-spec
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [vip.data-processor.errors :as errors]))

(def version-specs
  {"3.0" v3-0/data-specs
   "5.1" v5-1/data-specs})

(defn add-data-specs [data-specs]
  (fn [ctx]
    (assoc ctx :data-specs data-specs)))

(defn invalid-utf-8? [string]
  (.contains string "�"))

(defn create-format-rule
  "Create a function that applies a format check for a specific
  element of an import."
  [scope {name :name
          required :required
          {:keys [check message severity]} :format
          severity-override :severity}]
  (let [severity (or severity-override severity :errors)
        test-fn (cond
                  (sequential? check) (fn [val]
                                        (let [lower-case-val (str/lower-case val)]
                                          (some #{lower-case-val} check)))
                  (instance? clojure.lang.IFn check) check
                  (instance? java.util.regex.Pattern check) (fn [val] (re-find check val))
                  :else (constantly true))]
    (fn [ctx element id-or-line-number]
      (let [identifier (or (get element "id") id-or-line-number)
            val (element name)]
        (cond
          (empty? val)
          (if required
            (errors/add-errors ctx required scope identifier name (str "Missing " name))
            ctx)

          (invalid-utf-8? val)
          (errors/add-errors ctx :errors scope identifier name "Is not valid UTF-8.")

          (not (test-fn val))
          (errors/add-errors ctx severity scope identifier name (str message ": " val))

          :else ctx)))))

(defn create-format-rules [data-specs scope columns]
  (let [table (-> #(= scope (:filename %))
                  (filter data-specs)
                  first :table)
        scope (if table table scope)]
    (map (partial create-format-rule scope) columns)))

(defn apply-format-rules [rules ctx element id]
  (reduce (fn [ctx rule] (rule ctx element id)) ctx rules))

(defn create-translation-fn [{:keys [name translate]}]
  (fn [row]
    (if-let [cell (row name)]
      (assoc row name (translate cell))
      row)))

(defn translation-fns [columns]
  (->> columns
       (filter :translate)
       (map create-translation-fn)))

(defn map-col-coercion
  "From a data-spec column definition generate a transducer to coerce
  values for that column."
  [{:keys [name coerce]}]
  (map (fn [row]
         (let [col (if (contains? row name) name (keyword name))
               v (get row col)]
           (if v
             (assoc row col (coerce v))
             row)))))

(defn coerce-rows
  "Given the column definitions from a data-spec and a sequence of
  rows, coerce values in the rows needing coercion."
  [cols rows]
  (let [coercable-cols (filter :coerce cols)
        coercions (map map-col-coercion coercable-cols)
        coercion-xf (apply comp coercions)
        application (sequence coercion-xf rows)]
    application))

(defn filename->table [data-specs filename]
  (->> data-specs
       (filter #(= (:filename %) filename))
       first
       :table))
