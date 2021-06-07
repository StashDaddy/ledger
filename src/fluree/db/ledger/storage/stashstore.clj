; Module for using STASH as default data store for ledger and group data
;
(ns fluree.db.ledger.storage.stashstore
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [stash.api.stashapi :as api]
            [cheshire.core :refer [parse-string]]
            [fluree.db.ledger.storage :refer [key->unix-path]]
            [fluree.db.ledger.storage.crypto :as crypto]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream)))

(defn read
  "Reads a file 'f' from the STASH vault and returns a byte-array of the file's contents"
  [stash-conn f]
  (let [options {:srcId (str "folderNames=My Home|" (:ledger-prefix stash-conn) ",fileName=" f)
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)
                 :filename f
                 :filekey (:filekey stash-conn)
                 :username (:username stash-conn)}
        response (api/getbytes options)]
        ;retcode (:code (parse-string response true))]
    (if (not (bytes? response))
        (ex-info "STASH read failed" response)
        response
    )
  )
)

(defn connection-storage-read
  "Returns a function to read a file to the storage location"
  [stash-conn base-path]
  (fn [f]
    (async/thread
      (read stash-conn f))))

; ToDo how to handle overwrites?
; ToDo what format is 'f' - a full path or just a filename?
(defn write
  "Writes a byte-array 'data' to a file 'f' to the STASH vault"
  [stash-conn f data]
  (let [options {:dstId (str "destFolderNames=My Home|" (:ledger-prefix stash-conn) ",destFileName=" f)
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)
                 :filename f
                 :filekey (:filekey stash-conn)
                 :username (:username stash-conn)
                 }
        response (api/putbytes options data)
        retcode (:code (parse-string response true))]
     (if (not= 200 retcode)
        (ex-info "STASH write failed" response)
        response)
  )
)

(defn connection-storage-write
  "Returns a function to write a byte-array 'data' to a file 'f' in the storage location"
  [stash-conn base-path]
  (fn [f data]
    (async/thread
      (write stash-conn f data))))

(defn list
  "Returns a sequence of data maps with keys `#{:name :size :url}` representing the files in this STASH vault"
  [stash-conn base-path path]
  (let [options {:srcId (str "folderNames=My Home|" (:ledger-prefix stash-conn) ",outputType=1")
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        response (api/listfiles options)
        retcode (:code (parse-string response true))]
     (if (not= 200 retcode)
        (throw (Exception. "Error Retrieving File List from Vault")))
     (:files (parse-string response true))
  )
)

(defn connection-storage-list
  "Returns a function to async list files in the storage location"
  [stash-conn base-path]
  (fn [path]
    (async/thread
      (list stash-conn))))

(defn exists
  "Determines if a file 'f' exists in the storage location"
  [stash-conn f]
  (let [options {:srcId (str "folderNames=My Home|" (:ledger-prefix stash-conn) ",fileName=" f)
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        response (api/getfileinfo options)
        retcode (:code (parse-string response true))]
     (if (or (not= 200 retcode) (not= 404 retcode))
        (throw (Exception. "Error Determining if File Exists")))
     (not (= 404 retcode))
  )
)

(defn connection-storage-exists?
  "Returns an async function to determine if a file 'f' exists in the storage location"
  [stash-conn base-path]
  (fn [f]
    (async/thread
      (exists stash-conn f))))

(defn delete
  "Deletes the specified file 'f'"
  [stash-conn f]
  (let [options {:srcId (str "folderNames=My Home|" (:ledger-prefix stash-conn) ",fileName=" f) :id (:id stash-conn)  :pw (:pw stash-conn) :endpoint (:endpoint stash-conn) :verbose (:verbose stash-conn)}]
    (api/deletefile options)
  )
)

(defn connection-storage-delete
  "Returns an async function to delete the specified file 'f'"
  [stash-conn base-path]
  (fn [f]
    (async/thread
      (delete stash-conn f))))

(defn rename
  "Renames the specified file 'old-f' to 'new-f'"
  [stash-conn old-f new-f]
  (let [options {:srcId (str "folderNames=My Home|" (:ledger-prefix stash-conn) ",fileName=" old-f)
                 :dstId (str "destFileName=" new-f)
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}]
    (api/renamefile options)
  )
)

(defn connection-storage-rename
  "Renames a file in the storage location"
  [stash-conn base-path]
  (fn [old-f new-f]
    (async/thread
      (rename stash-conn old-f new-f))))

(defn connect
  "Connects to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)

(defn close
  "Closes the connection to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)
