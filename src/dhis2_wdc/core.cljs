(ns dhis2-wdc.core
  (:require [cljs-http.client :as http]
            [cljs.core.async :as async]
            [dhis2-wdc.wdc :as wdc]
            [reagent.core :as r])
  (:require-macros
   [cljs.core.async.macros :as async]))

; (set! *warn-on-infer* true)

(enable-console-print!)
(declare render)

(defn deep-merge-with [f & maps]
  (apply
   (fn m [& maps]
     (if (every? map? maps)
       (apply merge-with m maps)
       (reduce f maps)))
   maps))

(defn deep-merge [& maps]
  (apply deep-merge-with (fn [x y] (or y x)) maps))

(defn wrap-baseurl [baseurl [endpoint opts]]
  [(str baseurl endpoint) opts])

(defn wrap-cors [cors-proxy [url opts]]
  (if cors-proxy
    [(clojure.string/replace-first url "://" (str "://" cors-proxy "/")) opts]
    [url opts]))

(defn wrap-auth [authorization [url opts]]
  [url (merge-with merge opts {:with-credentials? false
                               :headers {"Authorization" authorization}})])

(defn wrap-basic-auth [username password [url opts]]
  [url (merge-with merge opts {:basic-auth {:username username :password password}
                               :with-credentials? false})])

(defn wrap-accept [[url opts]]
  [url (merge-with merge opts {:headers {"Accept" "application/json"}})])

(defonce wdc-state
  (r/atom
   {:connection-data {:cors-proxy "dtreskunov-cors-anywhere.herokuapp.com"}
    :username nil
    :password nil}))

(defn request [endpoint & [req]]
  (let [username (:username @wdc-state)
        password (:password @wdc-state)
        {:keys [baseurl cors-proxy]} (:connection-data @wdc-state)]
    (->> [endpoint req]
         (wrap-baseurl baseurl)
         (wrap-cors cors-proxy)
         (wrap-basic-auth username password)
         (wrap-accept)
         (apply http/get))))

(defn paginate [out xform endpoint & [req]]
  (async/go-loop [req req]
    (when-let [response (async/<! (request endpoint req))]
      (async/>! out (xform response))
      (let [page  (-> response :body :pager :page)
            count (-> response :body :pager :pageCount)
            next (when (< page count) (assoc-in req [:query-params :page] (inc page)))]
        (if next
          (recur next)
          (async/close! out))))))

(defn indexed [f coll]
  (into {} (map (fn [x] [(f x) x]) coll)))

(def ou-table
  {:endpoint "/api/26/organisationUnits"
   :req {:query-params {:fields "id,level,featureType,displayName,coordinates"}}

   :table-info
   {:id "ou"
    :alias   "Organisation Units"
    :columns [{:id         "id"
               :dataType   "string"
               :filterable true}
              {:id         "level"
               :dataType   "int"
               :columnType "discrete"}
              {:id         "displayName"
               :dataType   "string"}
              {:id         "featureType"
               :dataType   "string"}
              {:id         "lat"
               :dataType   "float"
               :columnRole "dimension"}
              {:id         "lon"
               :dataType   "float"
               :columnRole "dimension"}]}

   :response->rows
   (fn [response]
     (->> response
          :body
          :organisationUnits
          (map (fn [{:keys [id level featureType displayName coordinates]}]
                 (let [[lon lat] (when (= featureType "POINT") (js->clj (.parse js/JSON coordinates)))]
                   {:id id
                    :level level
                    :featureType featureType
                    :displayName displayName
                    :lat lat
                    :lon lon})))))})

(defn find-table-by-id [tables id]
  (first (filter #(= id (-> % :table-info :id)) tables)))

(defn indices [pred coll]
  (keep-indexed #(when (pred %2) %1) coll))

(defn index-of "First index of item mapping k to v" [coll k v]
  (first (indices #(= v (k %)) coll)))

(defn get-analytics-table [{:keys [period org-unit-level dimensions-map]}]
  (let [dimensions (vals dimensions-map)
        endpoint "/api/26/analytics"
        query-params {:dimension ["co"
                                  (str "ou:LEVEL-" (:level org-unit-level))
                                  (str "pe:" (:id period))
                                  (str "dx:" (clojure.string/join ";" (map :id dimensions)))]}]
    {:endpoint endpoint
     :req {:query-params query-params}
     
     :table-info
     {:id "analytics"
      :alias "Selected data"
      :description (str "Source endpoint: " endpoint " query params: " query-params)
      :columns (concat
                [{:id "co"
                  :dataType "string"
                  :alias "Category option combo"}
                 {:id "co_name"
                  :dataType "string"
                  :alias "Category option combo (name)"}
                 {:id "ou"
                  :dataType "string"
                  :alias "Organisation unit"
                  :foreignKey {:tableId "ou" :columnId "id"}}
                 {:id "ou_name"
                  :dataType "string"
                  :alias "Organisation unit (name)"}
                 {:id "pe"
                  :dataType "string"
                  :alias "Period"}
                 {:id "pe_name"
                  :dataType "string"
                  :alias "Period (name)"}]
                (map
                 (fn [{:keys [id aggregationType displayName valueType] :as dimension}]
                   (let [agg-type (get {"AVERAGE" "avg"
                                        "COUNT" "count"
                                        "SUM" "sum"}
                                       aggregationType)
                         data-type (get {"BOOLEAN" "bool"
                                         "DATE" "date"
                                         "DATETIME" "datetime"
                                         "INTEGER" "int"
                                         "INTEGER_NEGATIVE" "int"
                                         "INTEGER_POSITIVE" "int"
                                         "INTEGER_ZERO_OR_POSITIVE" "int"
                                         "NUMBER" "float"
                                         "PERCENTAGE" "float"}
                                        valueType "string")]
                     {:id id
                      :dataType data-type
                      :alias displayName
                      :aggType agg-type}))
                 dimensions))}

     :response->rows
     (fn [response]
       (let [headers (-> response :body :headers)
             meta-items (-> response :body :metaData :items)
             with-names (fn [row column-id name-column-id]
                          (let [index (index-of headers :name (str column-id))
                                value (nth row index)
                                name (:name (get meta-items (keyword value)))]
                            {column-id value
                             name-column-id name}))
             dx-index (index-of headers :name "dx")
             value-index (index-of headers :name "value")]
         (->> response
              :body
              :rows
              (map (fn [row]
                     (merge (with-names row "co" "co_name")
                            (with-names row "ou" "ou_name")
                            (with-names row "pe" "pe_names")
                            {(nth row dx-index) (nth row value-index)}))))))}))

(defn get-tables [connection-data]
  [ou-table (get-analytics-table connection-data)])

(defn get-standard-connections "Join other table(s) to the ou table"
  [connection-data]
  (let [tables (get-tables connection-data)
        ou-table (find-table-by-id tables "ou")
        ou-alias (-> ou-table :table-info :alias)]
    (when ou-table
      (mapcat
       (fn [{:keys [table-info]}]
         (let [{:keys [id alias]} table-info]
           (when-not (= "ou" id)
             [{:alias (str alias ", joined to " ou-alias)
               :tables [{:id id :alias alias}
                        {:id "ou" :alias ou-alias}]
               :joins [{:left {:tableAlias alias :columnId "ou"}
                        :right {:tableAlias ou-alias :columnId "id"}
                        :joinType :left}]}])))
       tables))))

(defonce app-state
  (r/atom
   {:show-ui? true
    :called-by-tableau? false
    :user nil}))

(deftype DHIS2WDC []
  wdc/IWebDataConnector
  (-get-auth-type [this] "basic")
  (-get-name [this]
    (str "DHIS2 Connection to " (get-in @wdc-state [:connection-data :baseurl])))
  (-get-table-infos [this]
    (map :table-info (get-tables (:connection-data @wdc-state))))
  (-get-standard-connections [this]
    (get-standard-connections (:connection-data @wdc-state)))
  (-get-rows! [this rows-chan table-info inc-val filter-values]
    (let [id (:id table-info)
          tables (get-tables (:connection-data @wdc-state))]
      (when-let [{:keys [endpoint req response->rows]} (find-table-by-id tables id)]
        (paginate rows-chan response->rows endpoint req))))
  (-shutdown [this]
    @wdc-state)
  (-init [this phase preserved-state]
    (swap! wdc-state deep-merge preserved-state)
    (swap! app-state merge {:show-ui? (#{"auth" "interactive"} phase)
                            :auth-only? (= "auth" phase)
                            :called-by-tableau? true})))

(def wdc (DHIS2WDC.))
(wdc/register! wdc)

(defn has-blanks? [coll & ks]
  (some #(clojure.string/blank? (if (vector? %) (get-in coll %) (coll %))) ks))

(defn bind [state & {:keys [js-> ->js] :or {js-> identity ->js identity} :as attrs}]
  (letfn [(get-value [event]
            (-> event
                (aget "target")
                (aget "value")
                js->))
          (on-change [event]
            (reset! state (get-value event)))]
    (assoc attrs
           :value (->js @state)
           :on-change on-change)))

(defn sign-in! []
  (let [req (request "/api/26/me" {:query-params {:fields ["id" "displayName"]}})]
    (async/go
      (when-let [{:keys [status body]} (async/<! req)]
        (when (= 200 status)
          (if (:auth-only? @app-state)
            (wdc/go! wdc)
            (swap! app-state assoc :user body)))))))

(defn sign-in-component []
  [:div.panel.panel-primary
   [:div.panel-heading "Please sign in..."]
   [:div.panel-body
    [:form.form-horizontal
     [:div.form-group
      [:label.col-sm-2.control-label "Server"]
      [:div.col-sm-10
       [:input.form-control (bind (r/cursor wdc-state [:connection-data :baseurl]) :type "url" :placeholder "Server URL")]]]
     [:div.form-group
      [:label.col-sm-2.control-label "Username"]
      [:div.col-sm-10
       [:input.form-control (bind (r/cursor wdc-state [:username]) :type "text" :placeholder "Username")]]]
     [:div.form-group
      [:label.col-sm-2.control-label "Password"]
      [:div.col-sm-10
       [:input.form-control (bind (r/cursor wdc-state [:password]) :type "password" :placeholder "Password")]]]
     [:div.form-group
      [:a.col-sm-2.control-label {:role "button"
                                  :on-click #(swap! wdc-state deep-merge {:connection-data {:baseurl "https://play.dhis2.org/dev"}
                                                                          :username "admin"
                                                                          :password "district"})}
       "Demo"]
      [:div.col-sm-10
       [:button.btn.btn-default {:on-click sign-in!
                                 :type "button"
                                 :disabled (has-blanks? @wdc-state [:connection-data :baseurl] :username :password)}
        "Sign In"]]]]]])

; not used - the rawData API seems to ignore startDate/endDate
(defn select-date-component [a-selection {:keys [validate] :or {validate #(= NaN (.getTime %))}}]
  (let [invalid? (r/atom nil)
        zero-pad #(if (< % 10) (str "0" %) (str %))
        format (fn [date] (str (.getUTCFullYear date) "-" (zero-pad (inc (.getUTCMonth date))) "-" (zero-pad (.getUTCDate date))))
        on-change (fn [e]
                    (let [text (-> e (aget "target") (aget "value"))
                          date (js/Date. text)]
                      (reset! invalid? (validate date))
                      (when-not @invalid?
                        (reset! a-selection date))))]
    (fn []
      [:form-group {:class (if @invalid? "has-error" "has-success")}
       [:input.form-control {:type "date" :value (if @a-selection (format @a-selection) "") :on-change on-change}]
       [:span.help-block.small (when @a-selection (format @a-selection))]])))

(defn select-component
  "A <select> (combo) component.
  
  The first argument is an atom holding the selected item or index of selected items (map id -> item).
  The second argument is an atom holding items available to be selected.

  Optional keyword arguments:
  :display-by function that takes an item and returns a string to be displayed
  :index-by function that takes an item and returns a string id
  :size how many lines to show
  :multiple? whether to allow multiple selection
  :keywordize? whether the keys of the index of selected items need to be keywordized"
  [a-selection a-items {:keys [display-by index-by size multiple? keywordize?]
                        :or {display-by :displayName index-by :id size 5 multiple? true keywordize? true}}]
  (let [get-id (fn [v] (if keywordize? (keyword v) v))
        a-items-index (r/track #(indexed (comp get-id index-by) @a-items))
        on-select-change (fn [e]
                           (let [selection
                                 (if multiple?
                                   (let [js-options (-> e (aget "target") (aget "options"))
                                         options (for [i (range (aget js-options "length"))] (aget js-options i))
                                         items (->> options
                                                    (filter (fn [option] (aget option "selected")))
                                                    (map (fn [option] (aget option "value")))
                                                    (map get-id)
                                                    (select-keys @a-items-index))]
                                     items)
                                   (let [id (-> e (aget "target") (aget "value") get-id)
                                         item (get @a-items-index id)]
                                     item))]
                             (reset! a-selection selection)))
        default-value (if multiple?
                        (or (keys @a-selection) [])
                        (or (index-by @a-selection) ""))]
    (fn []
      (when-not multiple?
        (when-not @a-selection
          (reset! a-selection (first @a-items))))
      [:select.form-control {:default-value default-value :on-change on-select-change :size size :multiple multiple?}
       (for [item @a-items]
         ^{:key item} [:option {:value (index-by item)} (display-by item)])])))

(defn select-filtered-component
  "A component combining a filter string input box and a <select> combo

  The first argument is an atom holding the selected item or index of selected items (see docs for select-component).
  The second argument is a function that takes the filter string and an atom, and resets the atom to filtered items.
  The third argument is passed through to select-component."
  [a-selection get-items! opts]
  (let [a-filter (r/atom "")
        a-items (r/atom nil)]
    (fn []
      (get-items! @a-filter a-items)
      [:div.container
       [:div.row
        [:input.form-control (bind a-filter :type "text" :placeholder "Filter")]]
       [:div.row
        [select-component a-selection a-items opts]]])))

(defn get-dimensions-map! [s a]
  (let [req {:query-params (into {:fields ["id" "displayName" "valueType" "aggregationType"]}
                                 (when-not (clojure.string/blank? s)
                                   {:filter (str "displayName:ilike:" s)}))}]
    (async/go
      (when-let [{:keys [body]} (async/<! (request "/api/26/dataElements" req))]
        (reset! a (:dataElements body))))))

(defn dimensions-map-select-component [a-selection]
  (let [a-top (r/atom nil)
        a-bottom (r/atom nil)
        add! (fn [] (swap! a-selection merge @a-top))
        remove! (fn [] (reset! a-selection (apply dissoc @a-selection (keys @a-bottom))))]
    (fn []
      [:div
       [:div.form-group
        [:label.col-sm-3.control-label "Available dimensions"]
        [:div.col-sm-9
         [select-filtered-component a-top get-dimensions-map! {:filter? true :size 25}]]]
       [:div.form-group
        [:div.col-sm-offset-3.col-sm-9
         [:div.btn-group
          [:button.btn.btn-default {:type "button" :on-click add!}
           [:span.glyphicon.glyphicon-arrow-down]]
          [:button.btn.btn-default {:type "button" :on-click remove!}
           [:span.glyphicon.glyphicon-arrow-up]]]]]
       [:div.form-group
        [:label.col-sm-3.control-label "Selected dimensions"]
        [:div.col-sm-9
         [select-component a-bottom (r/track #(sort-by :displayName (vals @a-selection))) {:size 5}]]]])))

(defn get-org-unit-levels! [a]
  (async/go
    (when-let [{:keys [body]} (async/<! (request "/api/26/filledOrganisationUnitLevels"
                                                {:query-params {:fields ["id" "displayName" "level"]}}))]
      (reset! a body))))

(defn org-unit-level-select-component [a-selection]
  (let [a-items (r/atom nil)]
    (get-org-unit-levels! a-items)
    (fn []
      [select-component a-selection a-items {:multiple? false :size 1}])))

(def periods
  (map (fn [id] {:id id :displayName (-> id (clojure.string/replace "_" " ") (clojure.string/capitalize))})
       ["THIS_MONTH" "LAST_MONTH" "THIS_BIMONTH" "LAST_BIMONTH" "THIS_QUARTER" "LAST_QUARTER"
        "THIS_SIX_MONTH" "LAST_SIX_MONTH" "MONTHS_THIS_YEAR" "QUARTERS_THIS_YEAR"
        "THIS_YEAR" "MONTHS_LAST_YEAR" "QUARTERS_LAST_YEAR" "LAST_YEAR" "LAST_5_YEARS" "LAST_12_MONTHS"
        "LAST_3_MONTHS" "LAST_6_BIMONTHS" "LAST_4_QUARTERS" "LAST_2_SIXMONTHS" "THIS_FINANCIAL_YEAR"
        "LAST_FINANCIAL_YEAR" "LAST_5_FINANCIAL_YEARS"
        "THIS_WEEK" "LAST_WEEK" "LAST_4_WEEKS" "LAST_12_WEEKS" "LAST_52_WEEKS"]))

(defn period-select-component [a-selection]
  (let [a-items (r/atom periods)]
    [select-component a-selection a-items {:multiple? false :size 1}]))

(defn data-missing? []
  (let [data (:connection-data @wdc-state)]
    (some nil? [(first (:dimensions-map data))
                (:period data)
                (:org-unit-level data)])))

(defn choose-data-component []
  (let [disabled (data-missing?)]
    [:div.panel.panel-primary
     [:div.panel-heading (str "Welcome, " (get-in @app-state [:user :displayName]) "!")]
     [:div.panel-body
      [:p.lead "Please choose the data you'd like to import:"]
      [:form.form-horizontal
       [:div.form-group
        [:label.col-sm-3.control-label "Period"]
        [:div.col-sm-9
         [period-select-component (r/cursor wdc-state [:connection-data :period])]
         [:small "Week-level periods may not be reported."]]]
       [:div.form-group
        [:label.col-sm-3.control-label "Organisation Unit Level"]
        [:div.col-sm-9
         [org-unit-level-select-component (r/cursor wdc-state [:connection-data :org-unit-level])]]]
       [dimensions-map-select-component (r/cursor wdc-state [:connection-data :dimensions-map])]
       [:div.form-group
        [:div.col-sm-offset-3.col-sm-9
         [:button.btn.btn-default {:type "button" :disabled disabled :on-click #(wdc/go! wdc)} "Go!"]
         (when disabled [:div.small.error "Please select a period, organisation unit level, and some dimensions."])]]]]]))

(defn ui-component []
  (if (:user @app-state)
    [choose-data-component]
    [sign-in-component]))

(defn learn-more-component []
  [:div
   [:p "Please open this page inside Tableau to connect to your data. "
    [:a
     {:href "https://onlinehelp.tableau.com/current/pro/desktop/en-us/examples_web_data_connector.html"
      :target "_blank" :role "button"}
     "Learn more..."]] 
   [:p
    [:a.btn.btn-primary.btn-lg {:href "tableau://do-something-cool"} "Launch Tableau"]]])

(defn root-component []
  (when (:show-ui? @app-state)
    [:div.container
     [:div.jumbotron
      [:h2 "DHIS2 Tableau Connector"]
      (if (:called-by-tableau? @app-state)
        [ui-component]
        [learn-more-component])]]))

(r/render-component
 [root-component]
 (. js/document (getElementById "app")))

(defn on-js-reload []
  ;; optionally touch your app-state to force rerendering depending on
  ;; your application
  (swap! app-state update-in [:__figwheel_counter] inc)
)


