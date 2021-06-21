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
  ;(log/info "API LOG: " (api/testfn))
  ;(log/info "Ledger LOG: " (clojure.tools.logging.impl/get-logger log/*logger-factory* *ns*))
  (let [options {:srcId (str "filePath=" (key->unix-path base-path f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)
                 :filename f
                 :filekey (:filekey stash-conn)
                 :username (:username stash-conn)}
        randint  (rand-int 100)
        -        (log/info (str randint " - Reading data from STASH file: " (:srcId options)))
        response (api/getbytes options)]
        ;-        (log/info (str randint " - Response: " (String. response)))
    (if (= (String. (byte-array (->> response (take 7)))) "{\"code\"")
      ; Error condition - API returned a JSON error code
      (do
        (let [parseResponse (parse-string (String. response) true)]
          (log/error (str randint " - Reading data from STASH failed, response: " (String. response)))
          (if (= 404 (:code parseResponse))
            nil     ; File Not Found - by convention in filestore.clj, return nil
            (ex-info "STASH read failed" parseResponse)      ; If error other than file not found, return (but not throw) an exception
          )
        )
      )
      ; If not an error condition, return the response as a byte array
      (do
        (log/info (str randint " - Reading data from STASH file response: " response))
        response
      )
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
        (read stash-conn base-path f))))
    )

(defn list
  "Returns a sequence of data maps with keys `#{:name :size :url}` representing the files in this STASH vault"
  [stash-conn base-path path]
  ;(log/info "API LOG: " (api/testfn))
  ;(log/info "Ledger LOG: " (clojure.tools.logging.impl/get-logger log/*logger-factory* *ns*))
  (let [options {:srcId (str "filePath=" base-path path ",outputType=5")
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        - (log/info "STASH list files in : " (:srcId options))
        response (api/listfiles options)
        retcode (:code (parse-string response true))
        files (:files (parse-string response true))
        - (log/info "STASH list files: " files)]
     ; Reformat response to {:name, :size :url}
     (if (not= 200 retcode)
        (throw (Exception. "Error Retrieving File List from Vault")))
     (map (fn [f] {:name (:name f) :url "" :size (:size f)}) files)
  )
)

(defn connection-storage-list
  "Returns a function to async list files in the storage location"
  [stash-conn base-path]
  (fn [path]
    (async/thread
        (do
            (log/info "-connection-storage-list(), base-path: " base-path "path: " path)
            (list stash-conn base-path path)))))

(defn exists
  "Determines if a file 'f' exists in the storage location, returns T/F"
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
     (if (and (not= 200 retcode) (not= 404 retcode))
        (throw (Exception. "Error Determining if File Exists")))
     (not (= 404 retcode))
  )
)

(defn connection-storage-exists?
  "Returns an async function to determine if a file 'f' exists in the storage location"
  [stash-conn base-path]
  (fn [f]
    (async/thread
        (do
            (log/info "connection-storage-exists?, base-path: " base-path " file: " f)
            (exists stash-conn base-path f)))))

(defn delete
  "Deletes the specified file 'f', returns T if successful, F otherwise"
  [stash-conn base-path f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
        - (log/info "STASH Delete file: " (:srcId options))
        response (api/deletefile options)
        parseResponse (parse-string response true)]
    (if (= 200 (:code parseResponse))     ; Return T if successful delete, F otherwise
      true
      (do
        (log/error "Error Deleting STASH File: " response)
        false
      )
    )
  )
)

(defn connection-storage-delete
  "Returns an async function to delete the specified file 'f'"
  [stash-conn base-path]
  (fn [f]
    (async/thread
        (do
            (log/info "connection-storage-delete, base-path: " base-path " file: " f)
            (delete stash-conn base-path f)))))

(defn rename
  "Renames the specified file 'old-f' to 'new-f'"
  [stash-conn base-path old-f new-f]
  (let [options {:srcId (str "filePath=" (key->unix-path base-path old-f))
                 :dstId (str "destFilePath=" (key->unix-path base-path new-f))
                 :id (:id stash-conn)
                 :pw (:pw stash-conn)
                 :endpoint (:endpoint stash-conn)
                 :verbose (:verbose stash-conn)}
         - (log/info (str "STASH rename file: " (:srcId options) " to " (:dstId options)))
         response (api/renamefile options)
         parseResponse (parse-string response true)]
    (if (= 200 (:code parseResponse))     ; Return T if successful rename, F otherwise
      true
      (do
        (log/error "Error Renaming STASH File: " response)
        false
      )
    )
  )
)

(defn connection-storage-rename
  "Renames a file in the storage location"
  [stash-conn base-path]
  (fn [old-f new-f]
    (async/thread
      (do
        (log/info (str "connection-storage-rename base-path: " base-path " old-file " old-f " new-file " new-f))
        (rename stash-conn base-path old-f new-f)))))


(defn write
  "Writes a byte-array 'data' to a file 'f' to the STASH vault
   Returns an exception (doesn't throw one) if writing fails"
  [stash-conn base-path f data]
  ;(log/info "API LOG: " (api/testfn))
  ;(log/info "Ledger LOG: " (clojure.tools.logging.impl/get-logger log/*logger-factory* *ns*))
  ;(let [options {:dstId (str "destFolderNames=" (:ledger-prefix stash-conn) "destFolderCreate=1,destFileName=" f)
  (if (nil? data)
    ; If data is nil, delete the file (consistent with filestore.clj:storage-write())
    (delete stash-conn base-path f)
    ; Otherwise, write data to file
    (let [options {:dstId (str "destFilePath=" (key->unix-path base-path f) ",destFolderCreate=1")
                   :id (:id stash-conn)
                   :pw (:pw stash-conn)
                   :endpoint (:endpoint stash-conn)
                   :verbose (:verbose stash-conn)
                   :filename f
                   :filekey (:filekey stash-conn)
                   :username (:username stash-conn)
                   }
          randint  (rand-int 100)
          -        (log/info randint " - Writing data to STASH file: " (:dstId options))
          response (api/putbytes options data)
          -        (log/info randint " - Write data to STASH file response: " response)
          retcode (:code (parse-string response true))]
     (if (not= 200 retcode)
        (do
            (log/error randint " - Writing data to STASH failed, response: " response)
            (ex-info "STASH write failed" (parse-string (:body response) true))
        )
       ;(do
       ;  (log/info randint " - fieldstosign: " (:fieldstosign response))
       ;  (log/info randint " - stringtosign: " (:stringtosign response))
       ; (:body response))
       ;)
       response
     )
    )
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

(defn connect
  "Connects to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)

(defn close
  "Closes the connection to the storage location - for STASH this isn't necessary and only a placeholder to fulfill the Fluree storage interface design"
  [stash-conn]
  true)
