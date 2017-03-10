(ns dhis2-wdc.core
  (:require-macros [cljs.core.async.macros :as async])
  (:require [dhis2-wdc.wdc :as wdc]
            [reagent.core :as r]
            [cljs-http.client :as http]
            [cljs.core.async :as async]))

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

(defn paginate [endpoint wdc-state out xform]
  (async/go-loop [endpoint endpoint]
    (when-let [response (async/<! (request endpoint wdc-state))]
      (async/>! out (xform response))
      (let [page  (-> response :body :pager :page)
            count (-> response :body :pager :pageCount)
            has-params? (clojure.string/includes? endpoint "?")
            has-page? (clojure.string/includes? endpoint "page=")
            next (cond
                   (>= page count) nil
                   (not has-params?) (str endpoint "?page=" (inc page))
                   (and has-params? has-page?) (clojure.string/replace endpoint (str "page=" page) (str "page=" (inc page)))
                   (and has-params? (not has-page?)) (str endpoint "&page=" (inc page)))]
        (if next
          (recur next)
          (async/close! out))))))

(def tables
  {"ou"
   {:endpoint
    "/api/26/organisationUnits?fields=id,level,featureType,displayName,coordinates"

    :table-info
    {:alias   "Organisation Units"
     :columns [{:id       "id"
                :dataType "string"}
               {:id       "level"
                :dataType "int"}
               {:id       "displayName"
                :dataType "string"}
               {:id       "featureType"
                :dataType "string"}
               {:id       "lat"
                :dataType "float"}
               {:id       "lon"
                :dataType "float"}]}

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
   {:connection-data {:cors-proxy "cors-anywhere.herokuapp.com"}
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

(defn bind [state ks]
  (letfn [(get-value [event]
            (-> event
                (aget "target")
                (aget "value")))
          (on-change [event]
            (swap! state assoc-in ks (get-value event)))]
    {:value (get-in @state ks)
     :on-change on-change}))

(defn sign-in! []
  (let [req (request "/api/26/me" @wdc-state)]
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
      [:label.col-sm-2.control-label "Server URL"]
      [:div.col-sm-10
       [:input.form-control (merge {:type "url":placeholder "Server URL"}
                                   (bind wdc-state [:connection-data :baseurl]))]]]
     [:div.form-group
      [:label.col-sm-2.control-label "Username"]
      [:div.col-sm-10
       [:input.form-control (merge {:type "text" :placeholder "Username"}
                                   (bind wdc-state [:username]))]]]
     [:div.form-group
      [:label.col-sm-2.control-label "Password"]
      [:div.col-sm-10
       [:input.form-control (merge {:type "password" :placeholder "Password"}
                                   (bind wdc-state [:password]))]]]
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

(defn choose-data-component []
  [:div.panel.panel-primary
   [:div.panel-heading (str "Welcome, " (get-in @app-state [:user :name]) "!")]
   [:div.panel-body
    [:input {:type "button" :value "Go!" :on-click #(wdc/go! wdc)}]]])

(defn ui-component []
  (if-not (:user @app-state)
    [sign-in-component]
    [choose-data-component]))

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
    [:div.container {:style {"marginTop" "2em"}}
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


