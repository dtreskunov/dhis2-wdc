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

(defn request [endpoint {:keys [connection-data username password]}]
  (let [{:keys [baseurl cors-proxy]} connection-data]
    (->> [endpoint nil]
         (wrap-baseurl baseurl)
         (wrap-cors cors-proxy)
         (wrap-basic-auth username password)
         (wrap-accept)
         (apply http/get))))

(defn set-query-param [url k v]
  (let [re (re-pattern (str "([?&])" k "=.*?(&|$)"))
        sep (if (clojure.string/includes? url "?") "&" "?")
        kv (when v (str k "=" v))]
    (if (re-find re url)
      (clojure.string/replace url re (str "$1" kv "$2"))
      (str url sep kv))))

(defn paginate [endpoint wdc-state out xform]
  (async/go-loop [endpoint endpoint]
    (when-let [response (async/<! (request endpoint wdc-state))]
      (async/>! out (xform response))
      (let [page  (-> response :body :pager :page)
            count (-> response :body :pager :pageCount)
            next (when (< page count) (set-query-param endpoint "page" (inc page)))]
        (if next
          (recur next)
          (async/close! out))))))

(def tables
  {"ou"
   {:endpoint
    "/api/26/organisationUnits?fields=id,level,featureType,displayName,coordinates"

    :table-info
    {:alias   "Organisation Units"
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
                     :lon lon})))))}})

(defonce wdc-state
  (r/atom
   {:connection-data {:cors-proxy "dtreskunov-cors-anywhere.herokuapp.com"}
    :username nil
    :password nil}))

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
    (map (fn [[id table]] (assoc (:table-info table) :id id)) tables))
  (-get-standard-connections [this]
    [])
  (-get-rows! [this rows-chan table-info inc-val]
    (when-let [{:keys [endpoint response->rows]} (get tables (:id table-info))]
      (paginate endpoint @wdc-state rows-chan response->rows)))
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
  (let [req (request "/api/26/me?fields=id,displayName" @wdc-state)]
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

(defn indexed [f coll]
  (into {} (map (fn [x] [(f x) x]) coll)))

; not uesd - the rawData API seems to ignore startDate/endDate
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

(defn select-multi-component [selection-a get-items-a & {:keys [filter? display-by index-by size]
                                                   :or {filter? false display-by :displayName index-by :id size 5}}]
  "Shows a multi-line multiple select element. Optionally adds a filter text field.

   selection-a is an atom wrapping a map of id to item
   load! is a 1-2 arity fn which takes an atom and a search string and updates the atom with available items
   :filter? is an optional keyword arg that makes the filter text field show up
   :display-by is an optional keyword arg
   :index-by is an optional keyword arg"
  (let [filter-a (r/atom "")
        items-aa (r/track #(get-items-a @filter-a))
        items-index-a (r/track #(indexed index-by @@items-aa))
        select! (fn [ids] (reset! selection-a (select-keys @items-index-a ids)))
        on-option-click (fn [e]
                          (let [js-options (-> e (aget "target") (aget "parentElement") (aget "options"))
                                options (for [i (range (aget js-options "length")) :let [option (aget js-options i)]] option)
                                selected (filter (fn [option] (aget option "selected")) options)
                                ids (map (fn [option] (aget option "value")) selected)]
                            (select! ids)))]
    (fn []
      (let [select-markup
            [:select.form-control {:default-value (or (keys @selection-a) []) :multiple true :size size}
             (for [item @@items-aa]
               ^{:key item} [:option {:value (index-by item) :on-click on-option-click} (display-by item)])]]
        (if filter?
          [:div.container
           [:div.row
            [:input.form-control (bind filter-a :type "text" :placeholder "Filter")]]
           [:div.row
            select-markup]]
          select-markup)))))

(defn get-dimensions [s]
  (println (str "get-dimensions " s))
  (let [a (r/atom nil)
        query-param (when-not (clojure.string/blank? s) (str "displayName:ilike:" s))
        endpoint (set-query-param "/api/26/dataElements?fields=id,displayName,valueType,aggregationType" "filter" query-param)]
    (async/go
      (when-let [{:keys [body]} (async/<! (request endpoint @wdc-state))]
        (reset! a (:dataElements body))))
    a))

(defn dimensions-select-component [selection-a]
  (let [left-a (r/atom nil)
        right-a (r/atom nil)
        add! (fn [] (swap! selection-a merge @left-a))
        remove! (fn [] (reset! selection-a (apply dissoc @selection-a (keys @right-a))))]
    (fn []
      [:div
       [:div.form-group
        [:label.col-sm-3.control-label "Available dimensions"]
        [:div.col-sm-9
         [select-multi-component left-a
          get-dimensions
          :filter? true
          :size 25]]]
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
         [select-multi-component right-a
          #(r/track sort-by :displayName (vals @selection-a))
          :size 5]]]])))

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


