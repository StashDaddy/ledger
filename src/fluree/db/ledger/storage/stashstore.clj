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
  [stash-conn base-path f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)
                 :filename f
                 :filekey (:filekey stash-conn)
                 :username (:username stash-conn)}
        -        (log/info (str "Reading data from STASH file: " (:srcId options)))
        response (api/getbytes options)
        -        (log/info (str "Reading data from STASH file response: " response))]
    (if (not (bytes? response))
        (do
            (log/error (str "Writing data to STASH failed, response: " response))
            (ex-info "STASH read failed" (parse-string response true))
        )
        response
    )
  )
)

(defn connection-storage-read
  "Returns a function to read a file to the storage location"
  [stash-conn base-path]
  (fn [f]
    (async/thread
      (do
        (log/info (str "connection-storage-read base-path: " base-path " file: " f))
        (read stash-conn base-path f)))))

; ToDo how to handle overwrites?
; ToDo what format is 'f' - a full path or just a filename?
(defn write
  "Writes a byte-array 'data' to a file 'f' to the STASH vault
   Returns an exception (doesn't throw one) if writing fails"
  [stash-conn base-path f data]
  ;(let [options {:dstId (str "destFolderNames=" (:ledger-prefix stash-conn) "destFolderCreate=1,destFileName=" f)
  (let [options {:dstId (str "destFilePath=" (key->unix-path base-path f) ",destFolderCreate=1")
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)
                 :filename f
                 :filekey (:filekey stash-conn)
                 :username (:username stash-conn)
                 }
        -        (log/info "Writing data to STASH file: " (:dstId options))
        response (api/putbytes options data)
        -        (log/info "Write data to STASH file response: " response)
        retcode (:code (parse-string response true))]
     (if (not= 200 retcode)
        (do
            (log/error "Writing data to STASH failed, response: " response)
            (ex-info "STASH write failed" (parse-string response true))
        )
        response)
  )
)

(defn connection-storage-write
  "Returns a function to write a byte-array 'data' to a file 'f' in the storage location"
  [stash-conn base-path]
  (fn [f data]
    (async/thread
      (do
        (log/info "connection-storage-write() base-path: " base-path " file: " f)
        (write stash-conn base-path f data)))))

(defn list
  "Returns a sequence of data maps with keys `#{:name :size :url}` representing the files in this STASH vault"
  [stash-conn base-path path]
  (let [options {:srcId (str "folderNames=" base-path path ",outputType=1")
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        - (log/info "STASH list files in : " (:srcId options))
        response (api/listfiles options)
        retcode (:code (parse-string response true))
        - (log/info "STASH list files: " response)]
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
        (do
            (log/info "connection-storage-list(), base-path: " base-path "path: " path)
            (list stash-conn base-path path)))))

(defn exists
  "Determines if a file 'f' exists in the storage location"
  [stash-conn base-path f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        - (log/info "STASH file exists: " (:srcId options))
        response (api/getfileinfo options)
        retcode (:code (parse-string response true))
        - (log/info "STASH file exists response: " response)]
     (if (or (not= 200 retcode) (not= 404 retcode))
        (throw (Exception. "Error Determining if File Exists")))
     (not (= 404 retcode))
  )
)

(defn connection-storage-exists?
  "Returns an async function to determine if a file 'f' exists in the storage location"
  [stash-conn base-path]
  (fn [f]
    ;(async/thread
        (do
            (log/info "connection-storage-exists?, base-path: " base-path " file: " f)
            (exists stash-conn base-path f))))

(defn delete
  "Deletes the specified file 'f'"
  [stash-conn base-path f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path f)) :id (:id stash-conn)  :pw (:pw stash-conn) :endpoint (:endpoint stash-conn) :verbose (:verbose stash-conn)}]
    (log/info "STASH Delete file: " (:srcId options))
    (api/deletefile options)
  )
)

(defn connection-storage-delete
  "Returns an async function to delete the specified file 'f'"
  [stash-conn base-path]
  (fn [f]
    ;(async/thread
        (do
            (log/info "connection-storage-delete, base-path: " base-path " file: " f)
            (delete stash-conn base-path f))))

(defn rename
  "Renames the specified file 'old-f' to 'new-f'"
  [stash-conn base-path old-f new-f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path old-f))
                 :dstId (str "destFilePath=" (key->unix-path base-path new-f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
         - (log/info (str "STASH rename file: " (:srcId options) " to " (:dstId options)))]
    (api/renamefile options)
  )
)

(defn connection-storage-rename
  "Renames a file in the storage location"
  [stash-conn base-path]
  (fn [old-f new-f]
    ;(async/thread
      (do
        (log/info (str "connection-storage-rename base-path: " base-path " old-file " old-f " new-file " new-f))
        (rename stash-conn base-path old-f new-f))))

(defn connect
  "Connects to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)

(defn close
  "Closes the connection to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)
