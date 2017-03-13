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
               :dataType   "string"}
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

(defn get-tables [connection-data]
  (into
   [ou-table]
   (for [{:keys [id aggregationType displayName valueType]} (vals (:dimensions connection-data))
         :let [agg-type (get {"AVERAGE" "avg"
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
                              valueType "string")]]
     {:endpoint "/api/26/analytics/rawData.json"
      :req {:query-params {:dimension ["co" "ou:LEVEL-1" "pe:LAST_YEAR" (str "dx:" id)]}}
       
      :table-info
      {:id id
       :alias displayName
       :columns [{:id "co"
                  :dataType "string"
                  :alias "Category option combo ID"}
                 {:id "co_val"
                  :dataType "string"
                  :alias "Category option combo"}
                 {:id "ou"
                  :dataType "string"
                  :alias "Organisation unit ID"}
                 {:id "pe"
                  :dataType "string"
                  :alias "Period"}
                 {:id "value"
                  :dataType data-type
                  :alias (str "Value of " displayName)
                  :aggType agg-type}]}
      :response->rows
      (fn [response]
        (let [headers (-> response :body :headers)
              meta-items (-> response :body :metaData :items)
              co-index (index-of headers :name "co")
              ou-index (index-of headers :name "ou")
              pe-index (index-of headers :name "pe")
              value-index (index-of headers :name "value")]
          (->> response
               :body
               :rows
               (map (fn [row]
                      {:co (nth row co-index)
                       :co_val (:name (get meta-items (keyword (nth row co-index))))
                       :ou (nth row ou-index)
                       :pe (nth row pe-index)
                       :value (nth row value-index)})))))})))

(defn get-standard-connections "Join each dimension to the ou table"
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
  (-get-rows! [this rows-chan table-info inc-val]
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
(defn select-date-component [selection-a {:keys [validate] :or {validate #(= NaN (.getTime %))}}]
  (let [invalid? (r/atom nil)
        zero-pad #(if (< % 10) (str "0" %) (str %))
        format (fn [date] (str (.getUTCFullYear date) "-" (zero-pad (inc (.getUTCMonth date))) "-" (zero-pad (.getUTCDate date))))
        on-change (fn [e]
                    (let [text (-> e (aget "target") (aget "value"))
                          date (js/Date. text)]
                      (reset! invalid? (validate date))
                      (when-not @invalid?
                        (reset! selection-a date))))]
    (fn []
      [:form-group {:class (if @invalid? "has-error" "has-success")}
       [:input.form-control {:type "date" :value (if @selection-a (format @selection-a) "") :on-change on-change}]
       [:span.help-block.small (when @selection-a (format @selection-a))]])))

(defn select-component
  "A <select> (combo) component.
  
  The first argument is an atom holding the selected item or index of selected items (map id -> item).
  The second argument is an atom holding items available to be selected.

  Optional keyword arguments:
  :display-by function that takes an item and returns a string to be displayed
  :index-by function that takes an item and returns a string id
  :size how many lines to show
  :multiple? whether to allow multiple selection"
  [selection-a items-a {:keys [display-by index-by size multiple?]
                        :or {display-by :displayName index-by :id size 5 multiple? true}}]
  (let [items-index-a (r/track #(indexed index-by @items-a))
        on-select-change (fn [e]
                           (let [selection
                                 (if multiple?
                                   (let [js-options (-> e (aget "target") (aget "options"))
                                         options (for [i (range (aget js-options "length")) :let [option (aget js-options i)]] option)
                                         selected (filter (fn [option] (aget option "selected")) options)
                                         ids (map (fn [option] (aget option "value")) selected)
                                         items (select-keys @items-index-a ids)]
                                     items)
                                   (let [id (-> e (aget "target") (aget "value"))
                                         item (get @items-index-a id)]
                                     item))]
                             (reset! selection-a selection)))
        default-value (if multiple?
                        (or (keys @selection-a) [])
                        (or (index-by @selection-a) ""))]
    (fn []
      [:select.form-control {:default-value default-value :on-change on-select-change :size size :multiple multiple?}
       (for [item @items-a]
         ^{:key item} [:option {:value (index-by item)} (display-by item)])])))

(defn select-filtered-component
  "A component combining a filter string input box and a <select> combo

  The first argument is an atom holding the selected item or index of selected items (see docs for select-component).
  The second argument is a function that takes the filter string and an atom, and resets the atom to filtered items.
  The third argument is passed through to select-component."
  [selection-a get-items! opts]
  (let [filter-a (r/atom "")
        items-a (r/atom nil)]
    (fn []
      (get-items! @filter-a items-a)
      [:div.container
       [:div.row
        [:input.form-control (bind filter-a :type "text" :placeholder "Filter")]]
       [:div.row
        [select-component selection-a items-a opts]]])))

(defn get-dimensions! [s a]
  (let [req {:query-params (into {:fields ["id" "displayName" "valueType" "aggregationType"]}
                                 (when-not (clojure.string/blank? s)
                                   {:filter (str "displayName:ilike:" s)}))}]
    (async/go
      (when-let [{:keys [body]} (async/<! (request "/api/26/dataElements" req))]
        (reset! a (:dataElements body))))))

(defn dimensions-select-component [selection-a]
  (let [top-a (r/atom nil)
        bottom-a (r/atom nil)
        add! (fn [] (swap! selection-a merge @top-a))
        remove! (fn [] (reset! selection-a (apply dissoc @selection-a (keys @bottom-a))))]
    (fn []
      [:div
       [:div.form-group
        [:label.col-sm-3.control-label "Available dimensions"]
        [:div.col-sm-9
         [select-filtered-component top-a get-dimensions! {:filter? true :size 25}]]]
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
         [select-component bottom-a (r/track #(sort-by :displayName (vals @selection-a))) {:size 5}]]]])))

(defn data-missing? []
  (empty? (get-in @wdc-state [:connection-data :dimensions])))

(defn choose-data-component []
  [:div.panel.panel-primary
   [:div.panel-heading (str "Welcome, " (get-in @app-state [:user :displayName]) "!")]
   [:div.panel-body
    [:form.form-horizontal
     [dimensions-select-component (r/cursor wdc-state [:connection-data :dimensions])]
     [:div.form-group
      [:div.col-sm-offset-3.col-sm-9
       [:button.btn.btn-default {:type "button" :disabled (data-missing?) :on-click #(wdc/go! wdc)} "Go!"]]]]]])

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


