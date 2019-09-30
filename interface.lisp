;;;;
;;;; Copyright (c) 2008, 2015 Zachary Beane, All Rights Reserved
;;;;
;;;; Redistribution and use in source and binary forms, with or without
;;;; modification, are permitted provided that the following conditions
;;;; are met:
;;;;
;;;;   * Redistributions of source code must retain the above copyright
;;;;     notice, this list of conditions and the following disclaimer.
;;;;
;;;;   * Redistributions in binary form must reproduce the above
;;;;     copyright notice, this list of conditions and the following
;;;;     disclaimer in the documentation and/or other materials
;;;;     provided with the distribution.
;;;;
;;;; THIS SOFTWARE IS PROVIDED BY THE AUTHOR 'AS IS' AND ANY EXPRESSED
;;;; OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
;;;; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;;; ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
;;;; DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
;;;; DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
;;;; GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;;; INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
;;;; WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;;; NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;;; SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;;
;;;; interface.lisp

(in-package #:zs3)

(defparameter *canned-access-policies*
  '((:private . "private")
    (:public-read . "public-read")
    (:public-read-write . "public-read-write")
    (:authenticated-read . "authenticated-read")))

(defun canned-access-policy (access-policy)
  (let ((value (assoc access-policy *canned-access-policies*)))
    (unless value
      (error "~S is not a supported access policy.~%Supported policies are ~S"
             access-policy
             (mapcar 'first *canned-access-policies*)))
    (list (cons "acl" (cdr value)))))

(defun access-policy-header (access-policy public)
  (cond ((and access-policy public)
         (error "Only one of ~S and ~S should be provided"
                :public :access-policy))
        (public
         (canned-access-policy :public-read))
        (access-policy
         (canned-access-policy access-policy))))

(defun head (&key bucket key parameters
               ((:credentials *credentials*) *credentials*)
               ((:backoff *backoff*) *backoff*))
  "Return three values: the HTTP status, an alist of Drakma-style HTTP
headers, and the HTTP phrase, with the results of a HEAD request for
the object specified by the optional BUCKET and KEY arguments."
  (let* ((security-token (security-token *credentials*))
         (response
          (submit-request (make-instance 'request
                                         :method :head
                                         :bucket bucket
                                         :key key
                                         :amz-headers
                                         (when security-token
                                           (list (cons "security-token" security-token)))
                                         :parameters parameters))))

    (values (http-headers response)
            (http-code response)
            (http-phrase response))))

;;; Operations on buckets

(defun all-buckets (&key ((:credentials *credentials*) *credentials*)
                      ((:backoff *backoff*) *backoff*))
  "Return a vector of all BUCKET objects associated with *CREDENTIALS*."
  (let ((response (submit-request (make-instance 'request
                                                 :method :get))))
    (buckets response)))

(defun bucket-location (bucket &key
                                 ((:credentials *credentials*) *credentials*)
                                 ((:backoff *backoff*) *backoff*))
  "If BUCKET was created with a LocationConstraint, return its
constraint."
  (let* ((request (make-instance 'request
                                  :method :get
                                  :sub-resource "location"
                                  :extra-http-headers
                                  `(,(when (security-token *credentials*)
                                       (cons "x-amz-security-token"
                                             (security-token *credentials*))))
                                  :bucket bucket))
         (response (submit-request request))
         (location (location response)))
    (when (plusp (length location))
      location)))

(defun bucket-region (bucket
                      &key ((:credentials *credentials*) *credentials*)
                        ((:backoff *backoff*) *backoff*))
  (or (bucket-location bucket)
      "us-east-1"))

(defun region-endpoint (region)
  (if (string= region "us-east-1")
      (or *s3-endpoint* "s3.amazonaws.com")
      (format nil "s3-~A.amazonaws.com" region)))

(defun query-bucket (bucket &key prefix marker max-keys delimiter
                              ((:credentials *credentials*) *credentials*)
                              ((:backoff *backoff*) *backoff*))
  (submit-request (make-instance 'request
                                 :method :get
                                 :bucket bucket
                                 :parameters
                                 (parameters-alist
                                  :prefix prefix
                                  :marker marker
                                  :max-keys max-keys
                                  :delimiter delimiter))))

(defun continue-bucket-query (response)
  (when response
    (let ((request (successive-request response)))
      (when request
        (submit-request request)))))

(defun all-keys (bucket &key prefix
                          ((:credentials *credentials*) *credentials*)
                          ((:backoff *backoff*) *backoff*))
  "Reutrn a vector of all KEY objects in BUCKET."
  (let ((response (query-bucket bucket :prefix prefix))
        (results '()))
    (loop
     (unless response
       (return))
     (push (keys response) results)
     (setf response (continue-bucket-query response)))
    (let ((combined (make-array (reduce #'+ results :key #'length)))
          (start 0))
      (dolist (keys (nreverse results) combined)
        (replace combined keys :start1 start)
        (incf start (length keys))))))

(defun bucket-exists-p (bucket &key
                                 ((:credentials *credentials*) *credentials*)
                                 ((:backoff *backoff*) *backoff*))
  (let ((code (nth-value 1 (head :bucket bucket
                                 :parameters
                                 (parameters-alist :max-keys 0)))))
    (not (<= 400 code 599))))

(defun create-bucket (name &key
                      access-policy
                      public
                      location
                             ((:credentials *credentials*) *credentials*)
                             ((:backoff *backoff*) *backoff*))
  (let ((policy-header (access-policy-header access-policy public)))
    (submit-request (make-instance 'request
                                   :method :put
                                   :bucket name
                                   :content (and location
                                                 (location-constraint-xml
                                                  location))
                                   :amz-headers policy-header))))

(defun delete-bucket (bucket &key
                               ((:credentials *credentials*) *credentials*)
                               ((:backoff *backoff*) *backoff*))
  (let* ((request (make-instance 'request
                                 :method :delete
                                 :bucket bucket))
         (endpoint (endpoint request))
         (bucket (bucket request)))
    (prog1
        (submit-request request)
      (setf (redirection-data endpoint bucket) nil))))


;;; Getting objects as vectors, strings, or files

(defun check-request-success (response)
  (let ((code (http-code response)))
    (cond ((= code 304)
           (throw 'not-modified (values nil (http-headers response))))
          ((not (<= 200 code 299))
           (setf response (specialize-response response))
           (maybe-signal-error response)))))

(defun make-file-writer-handler (file &key (if-exists :supersede))
  (lambda (response)
    (check-request-success response)
    (let ((input (body response)))
      (with-open-file (output file :direction :output
                                   :if-exists if-exists
                                   :element-type '(unsigned-byte 8))
        (copy-n-octets (content-length response) input output)))
    (setf (body response) (probe-file file))
    response))

(defun vector-writer-handler (response)
  (check-request-success response)
  (let ((buffer (make-octet-vector (content-length response))))
    (setf (body response)
          (let ((input (body response)))
            (read-sequence buffer input)
            buffer))
    response))

(defun stream-identity-handler (response)
  (check-request-success response)
  response)

(defun make-string-writer-handler (external-format)
  (lambda (response)
    (setf response (vector-writer-handler response))
    (setf (body response)
          (flexi-streams:octets-to-string (body response)
                                    :external-format external-format))
    response))



(defun get-object (bucket key &key
                                when-modified-since
                                unless-modified-since
                                when-etag-matches
                                unless-etag-matches
                                start end
                                (output :vector)
                                (if-exists :supersede)
                                (string-external-format :utf-8)
                                ((:credentials *credentials*) *credentials*)
                                ((:backoff *backoff*) *backoff*))
  (flet ((range-argument (start end)
           (when start
             (format nil "bytes=~D-~@[~D~]" start (and end (1- end)))))
         (maybe-date (time)
           (and time (http-date-string time))))
    (when (and end (not start))
      (setf start 0))
    (when (and start end (<= end start))
      (error "START must be less than END."))
    (let* ((security-token (security-token *credentials*))
           (request (make-instance 'request
                                   :method :get
                                   :bucket bucket
                                   :key key
                                   :amz-headers
                                   (when security-token
                                     (list (cons "security-token" security-token)))
                                   :extra-http-headers
                                   (parameters-alist
                                    ;; nlevine 2016-06-15 -- not only is this apparently
                                    ;; unnecessary, it also sends "connection" in the
                                    ;; signed headers, which results in a
                                    ;; SignatureDoesNotMatch error.
                                    ;;  :connection (unless *use-keep-alive* "close")
                                    :if-modified-since
                                    (maybe-date when-modified-since)
                                    :if-unmodified-since
                                    (maybe-date unless-modified-since)
                                    :if-match when-etag-matches
                                    :if-none-match unless-etag-matches
                                    :range (range-argument start end))))
           (handler (cond ((eql output :vector)
                           'vector-writer-handler)
                          ((eql output :string)
                           (make-string-writer-handler string-external-format))
                          ((eql output :stream)
                           'stream-identity-handler)
                          ((or (stringp output)
                               (pathnamep output))
                           (make-file-writer-handler output :if-exists if-exists))
                          (t
                           (error "Unknown ~S option ~S -- should be ~
                                  :VECTOR, :STRING, :STREAM, or a pathname"
                                 :output output)))))
      (catch 'not-modified
        (handler-case
            (let ((response (submit-request request
                                            :keep-stream (or (eql output :stream)
                                                             *use-keep-alive*)
                                            :body-stream t
                                            :handler handler)))
              (values (body response) (http-headers response)))
          (precondition-failed (c)
            (throw 'not-modified
              (values nil
                      (http-headers (request-error-response c))))))))))

(defun get-vector (bucket key
                   &key start end
                   when-modified-since unless-modified-since
                   when-etag-matches unless-etag-matches
                   (if-exists :supersede)
                     ((:credentials *credentials*) *credentials*)
                     ((:backoff *backoff*) *backoff*))
  (get-object bucket key
              :output :vector
              :start start
              :end end
              :when-modified-since when-modified-since
              :unless-modified-since unless-modified-since
              :when-etag-matches when-etag-matches
              :unless-etag-matches unless-etag-matches
              :if-exists if-exists))

(defun get-string (bucket key
                   &key start end
                   (external-format :utf-8)
                   when-modified-since unless-modified-since
                   when-etag-matches unless-etag-matches
                   (if-exists :supersede)
                     ((:credentials *credentials*) *credentials*)
                     ((:backoff *backoff*) *backoff*))
  (get-object bucket key
              :output :string
              :string-external-format external-format
              :start start
              :end end
              :when-modified-since when-modified-since
              :unless-modified-since unless-modified-since
              :when-etag-matches when-etag-matches
              :unless-etag-matches unless-etag-matches
              :if-exists if-exists))

(defun get-file (bucket key file
                 &key start end
                 when-modified-since unless-modified-since
                 when-etag-matches unless-etag-matches
                 (if-exists :supersede)
                   ((:credentials *credentials*) *credentials*)
                   ((:backoff *backoff*) *backoff*))
  (get-object bucket key
              :output (pathname file)
              :start start
              :end end
              :when-modified-since when-modified-since
              :unless-modified-since unless-modified-since
              :when-etag-matches when-etag-matches
              :unless-etag-matches unless-etag-matches
              :if-exists if-exists))


;;; Putting objects

(defun format-tagging-header (tagging)
  (format nil "~{~a=~a~^&~}"
          (mapcan #'(lambda (kv)
                      (list
                       (drakma:url-encode (car kv) :iso-8859-1)
                       (drakma:url-encode (cdr kv) :iso-8859-1)))
                  tagging)))

(defun put-object (object bucket key &key
                                       access-policy
                                       public
                                       metadata
                                       (string-external-format :utf-8)
                                       cache-control
                                       content-encoding
                                       content-disposition
                                       expires
                                       content-type
                                       (storage-class "STANDARD")
                                       tagging
                                       ((:credentials *credentials*) *credentials*)
                                       ((:backoff *backoff*) *backoff*))
  (let ((content
         (etypecase object
           (string
            (flexi-streams:string-to-octets object
                                            :external-format
                                            string-external-format))
           ((or vector pathname) object)))
        (content-length t)
        (policy-header (access-policy-header access-policy public))
        (security-token (security-token *credentials*)))
    (setf storage-class (or storage-class "STANDARD"))
    (submit-request (make-instance 'request
                                   :method :put
                                   :bucket bucket
                                   :key key
                                   :metadata metadata
                                   :amz-headers
                                   (append policy-header
                                           (list (cons "storage-class" storage-class))
                                           (when security-token
                                             (list (cons "security-token" security-token)))
                                           (when tagging
                                             (list
                                              (cons "tagging" (format-tagging-header tagging)))))
                                   :extra-http-headers
                                   (parameters-alist
                                    :cache-control cache-control
                                    :content-encoding content-encoding
                                    :content-disposition content-disposition
                                    :expires (and expires
                                                  (http-date-string expires)))
                                   :content-type content-type
                                   :content-length content-length
                                   :content content))))


(defun put-vector (vector bucket key &key
                   start end
                   access-policy
                   public
                   metadata
                   cache-control
                   content-encoding
                   content-disposition
                   (content-type "binary/octet-stream")
                   expires
                   storage-class
                   tagging
                   ((:credentials *credentials*) *credentials*)
                   ((:backoff *backoff*) *backoff*))
  (when (or start end)
    (setf vector (subseq vector (or start 0) end)))
  (put-object vector bucket key
              :access-policy access-policy
              :public public
              :metadata metadata
              :cache-control cache-control
              :content-encoding content-encoding
              :content-disposition content-disposition
              :content-type content-type
              :expires expires
              :storage-class storage-class
              :tagging tagging))

(defun put-string (string bucket key &key
                   start end
                   access-policy
                   public
                   metadata
                   (external-format :utf-8)
                   cache-control
                   content-encoding
                   content-disposition
                   (content-type "text/plain")
                   expires
                   storage-class
                   tagging
                   ((:credentials *credentials*) *credentials*)
                   ((:backoff *backoff*) *backoff*))
  (when (or start end)
    (setf string (subseq string (or start 0) end)))
  (put-object string bucket key
              :access-policy access-policy
              :public public
              :metadata metadata
              :expires expires
              :content-disposition content-disposition
              :content-encoding content-encoding
              :content-type content-type
              :cache-control cache-control
              :string-external-format external-format
              :storage-class storage-class
              :tagging tagging))


(defun put-file (file bucket key &key
                 start end
                 access-policy
                 public
                 metadata
                 cache-control
                 content-disposition
                 content-encoding
                 (content-type "binary/octet-stream")
                 expires
                 storage-class
                 tagging
                 ((:credentials *credentials*) *credentials*)
                 ((:backoff *backoff*) *backoff*))
  (when (eq key t)
    (setf key (file-namestring file)))
  (let ((content (pathname file)))
    (when (or start end)
      ;;; FIXME: integrate with not-in-memory file uploading
      (setf content (file-subset-vector file start end)))
    (put-object content bucket key
                :access-policy access-policy
                :public public
                :metadata metadata
                :cache-control cache-control
                :content-disposition content-disposition
                :content-encoding content-encoding
                :content-type content-type
                :expires expires
                :storage-class storage-class
                :tagging tagging)))

(defun put-stream (stream bucket key &key
                   (start 0) end
                   access-policy
                   public
                   metadata
                   cache-control
                   content-disposition
                   content-encoding
                   (content-type "binary/octet-stream")
                   expires
                   storage-class
                   tagging
                   ((:credentials *credentials*) *credentials*)
                   ((:backoff *backoff*) *backoff*))
  (let ((content (stream-subset-vector stream start end)))
    (put-object content bucket key
                :access-policy access-policy
                :public public
                :metadata metadata
                :cache-control cache-control
                :content-disposition content-disposition
                :content-encoding content-encoding
                :content-type content-type
                :expires expires
                :storage-class storage-class
                :tagging tagging)))


;;; Delete & copy objects

(defun delete-object (bucket key &key
                                   ((:credentials *credentials*) *credentials*)
                                   ((:backoff *backoff*) *backoff*))
  "Delete one object from BUCKET identified by KEY."
  (let ((security-token (security-token *credentials*)))
    (submit-request (make-instance 'request
                                   :method :delete
                                   :bucket bucket
                                   :key key
                                   :amz-headers
                                   (when security-token
                                     (list (cons "security-token" security-token)))))))

(defun bulk-delete-document (keys)
  (coerce
   (cxml:with-xml-output (cxml:make-octet-vector-sink)
     (cxml:with-element "Delete"
       (map nil
            (lambda (key)
              (cxml:with-element "Object"
                (cxml:with-element "Key"
                  (cxml:text (name key)))))
            keys)))
   'octet-vector))

(defbinder delete-objects-result
  ("DeleteResult"
   (sequence :results
             (alternate
              ("Deleted"
               ("Key" (bind :deleted-key)))
              ("Error"
               ("Key" (bind :error-key))
               ("Code" (bind :error-code))
               ("Message" (bind :error-message)))))))

(defun delete-objects (bucket keys
                       &key
                         ((:credentials *credentials*) *credentials*)
                         ((:backoff *backoff*) *backoff*))
  "Delete the objects in BUCKET identified by the sequence KEYS."
  (let ((deleted 0)
        (failed '())
        (subseqs (floor (length keys) 1000)))
    (flet ((bulk-delete (keys)
             (unless (<= 1 (length keys) 1000)
               (error "Can only delete 1 to 1000 objects per request ~
                       (~D attempted)."
                      (length keys)))
             (let* ((content (bulk-delete-document keys))
                    (md5 (vector-md5/b64 content)))
               (let* ((response
                       (submit-request (make-instance 'request
                                                      :method :post
                                                      :sub-resource "delete"
                                                      :bucket bucket
                                                      :content content
                                                      :content-md5 md5)))
                      (bindings (xml-bind 'delete-objects-result
                                          (body response)))
                      (results (bvalue :results bindings)))
                 (dolist (result results (values deleted failed))
                   (if (bvalue :deleted-key result)
                       (incf deleted)
                       (push result failed)))))))
      (loop for start from 0 by 1000
            for end = (+ start 1000)
            repeat subseqs do
            (bulk-delete (subseq keys start end)))
      (let ((remainder (subseq keys (* subseqs 1000))))
        (when (plusp (length remainder))
          (bulk-delete (subseq keys (* subseqs 1000)))))
      (values deleted failed))))

(defun delete-all-objects (bucket &key
                                    ((:credentials *credentials*) *credentials*)
                                    ((:backoff *backoff*) *backoff*))
  "Delete all objects in BUCKET."
  ;; FIXME: This should probably bucket-query and incrementally delete
  ;; instead of fetching all keys upfront.
  (delete-objects bucket (all-keys bucket)))

(defun copy-object (&key
                    from-bucket from-key
                    to-bucket to-key
                    when-etag-matches
                    unless-etag-matches
                    when-modified-since
                    unless-modified-since
                    (metadata nil metadata-supplied-p)
                    access-policy
                    public
                    precondition-errors
                    (storage-class "STANDARD")
                    (tagging nil tagging-supplied-p)
                      ((:credentials *credentials*) *credentials*)
                      ((:backoff *backoff*) *backoff*))
  "Copy the object identified by FROM-BUCKET/FROM-KEY to
TO-BUCKET/TO-KEY.

If TO-BUCKET is NIL, uses FROM-BUCKET as the target. If TO-KEY is NIL,
uses TO-KEY as the target.

If METADATA is provided, it should be an alist of metadata keys and
values to set on the new object. Otherwise, the source object's
metadata is copied.

If TAGGING is provided, it should be an alist of tag keys and values
to be set on the new object's tagging resource. Otherwise, the source
object's tagging is copied.

Optional precondition variables are WHEN-ETAG-MATCHES,
UNLESS-ETAG-MATCHES, WHEN-MODIFIED-SINCE, UNLESS-MODIFIED-SINCE. The
etag variables use an etag as produced by the FILE-ETAG function,
i.e. a lowercase hex representation of the file's MD5 digest,
surrounded by quotes. The modified-since variables should use a
universal time.

If PUBLIC is T, the new object is visible to all
users. Otherwise, a default ACL is present on the new object.
"
  (unless from-bucket
    (error "FROM-BUCKET is required"))
  (unless from-key
    (error "FROM-KEY is required"))
  (setf to-bucket (or to-bucket from-bucket))
  (setf to-key (or to-key from-key))
  (handler-bind ((precondition-failed
                  (lambda (condition)
                    (unless precondition-errors
                      (return-from copy-object
                        (values nil (request-error-response condition)))))))
    (let ((headers
           (parameters-alist :copy-source (format nil "~A/~A"
                                                  (url-encode (name from-bucket))
                                                  (url-encode (name from-key)))
                             :storage-class storage-class
                             :metadata-directive
                             (if metadata-supplied-p "REPLACE" "COPY")
                             :tagging-directive
                             (if tagging-supplied-p "REPLACE" "COPY")
                             :copy-source-if-match when-etag-matches
                             :copy-source-if-none-match unless-etag-matches
                             :copy-source-if-modified-since
                             (and when-modified-since
                                  (http-date-string when-modified-since))
                             :copy-source-if-unmodified-since
                             (and unless-modified-since
                                  (http-date-string unless-modified-since))))
          (policy-header (access-policy-header access-policy public))
          (tagging-header (when tagging-supplied-p
                            (list (cons "tagging" (format-tagging-header tagging))))))
      (submit-request (make-instance 'request
                                     :method :put
                                     :bucket to-bucket
                                     :key to-key
                                     :metadata metadata
                                     :amz-headers
                                     (nconc headers
                                            policy-header
                                            tagging-header))))))


(defun object-metadata (bucket key
                        &key
                          ((:credentials *credentials*) *credentials*)
                          ((:backoff *backoff*) *backoff*))
  "Return the metadata headers as an alist, with keywords for the keys."
  (let* ((prefix "X-AMZ-META-")
         (plen (length prefix)))
    (flet ((metadata-symbol-p (k)
             (and (< plen (length (symbol-name k)))
                  (string-equal k prefix :end1 plen)
                  (intern (subseq (symbol-name k) plen)
                          :keyword))))
      (let ((headers (head :bucket bucket :key key)))
        (loop for ((k . value)) on headers
              for meta = (metadata-symbol-p k)
              when meta
              collect (cons meta value))))))


;;; Convenience bit for storage class

(defun set-storage-class (bucket key storage-class
                          &key
                            ((:credentials *credentials*) *credentials*)
                            ((:backoff *backoff*) *backoff*))
  "Set the storage class of the object identified by BUCKET and KEY to
STORAGE-CLASS."
  (copy-object :from-bucket bucket :from-key key
               :storage-class storage-class))


;;; ACL twiddling

(defparameter *public-read-grant*
  (make-instance 'grant
                 :permission :read
                 :grantee *all-users*)
  "This grant is added to or removed from an ACL to grant or revoke
  read access for all users.")

(defun get-acl (&key bucket key
                  ((:credentials *credentials*) *credentials*)
                  ((:backoff *backoff*) *backoff*))
  (let* ((request (make-instance 'request
                                 :method :get
                                 :bucket bucket
                                 :key key
                                 :sub-resource "acl"))
         (response (submit-request request))
         (acl (acl response)))
    (values (owner acl)
            (grants acl))))

(defun put-acl (owner grants &key bucket key
                               ((:credentials *credentials*) *credentials*)
                               ((:backoff *backoff*) *backoff*))
  (let* ((acl (make-instance 'access-control-list
                             :owner owner
                             :grants grants))
         (request (make-instance 'request
                                 :method :put
                                 :bucket bucket
                                 :key key
                                 :sub-resource "acl"
                                 :content (acl-serialize acl))))
  (submit-request request)))


(defun make-public (&key bucket key
                      ((:credentials *credentials*) *credentials*)
                      ((:backoff *backoff*) *backoff*))
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket :key key)
    (put-acl owner
             (cons *public-read-grant* grants)
             :bucket bucket
             :key key)))

(defun make-private (&key bucket key
                       ((:credentials *credentials*) *credentials*)
                       ((:backoff *backoff*) *backoff*))
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket :key key)
    (setf grants
          (remove *all-users* grants
                  :test #'acl-eqv :key #'grantee))
    (put-acl owner grants :bucket bucket :key key)))


;;; Logging

(defparameter *log-delivery-grants*
  (list (make-instance 'grant
                       :permission :write
                       :grantee *log-delivery*)
        (make-instance 'grant
                       :permission :read-acl
                       :grantee *log-delivery*))
  "This list of grants is used to allow the Amazon log delivery group
to write logfile objects into a particular bucket.")

(defun enable-logging-to (bucket &key
                                   ((:credentials *credentials*) *credentials*)
                                   ((:backoff *backoff*) *backoff*))
  "Configure the ACL of BUCKET to accept logfile objects."
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket)
    (setf grants (append *log-delivery-grants* grants))
    (put-acl owner grants :bucket bucket)))

(defun disable-logging-to (bucket &key
                                    ((:credentials *credentials*) *credentials*)
                                    ((:backoff *backoff*) *backoff*))
  "Configure the ACL of BUCKET to remove permissions for the log
delivery group."
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket)
    (setf grants (remove-if (lambda (grant)
                              (acl-eqv (grantee grant) *log-delivery*))
                            grants))
    (put-acl owner grants :bucket bucket)))

(defun enable-logging (bucket target-bucket target-prefix
                       &key
                       target-grants
                         ((:credentials *credentials*) *credentials*)
                         ((:backoff *backoff*) *backoff*))
  "Enable logging of requests to BUCKET, putting logfile objects into
TARGET-BUCKET with a key prefix of TARGET-PREFIX."
  (let* ((setup (make-instance 'logging-setup
                               :target-bucket target-bucket
                               :target-prefix target-prefix
                               :target-grants target-grants))
         (request (make-instance 'request
                                 :method :put
                                 :sub-resource "logging"
                                 :bucket bucket
                                 :content (log-serialize setup)))
         (retried nil))
    (loop
     (handler-case
         (return (submit-request request))
       (invalid-logging-target (condition)
         (when (starts-with "You must give the log-delivery group"
                            (message (request-error-response condition)))
           (unless retried
             (setf retried t)
             (enable-logging-to target-bucket))))))))


(defparameter *empty-logging-setup*
  (log-serialize (make-instance 'logging-setup))
  "An empty logging setup; putting this into the logging setup of a
  bucket effectively disables logging.")

(defun disable-logging (bucket &key
                                 ((:credentials *credentials*) *credentials*)
                                 ((:backoff *backoff*) *backoff*))
  "Disable the creation of access logs for BUCKET."
  (submit-request (make-instance 'request
                                 :method :put
                                 :sub-resource "logging"
                                 :bucket bucket
                                 :content *empty-logging-setup*)))

(defun logging-setup (bucket &key
                               ((:credentials *credentials*) *credentials*)
                               ((:backoff *backoff*) *backoff*))
  (let ((setup (setup
                (submit-request (make-instance 'request
                                               :bucket bucket
                                               :sub-resource "logging")))))
    (values (target-bucket setup)
            (target-prefix setup)
            (target-grants setup))))



;;; Creating unauthorized and authorized URLs for a resource

(defclass url-based-request (request)
  ((expires
    :initarg :expires
    :accessor expires))
  (:default-initargs
   :expires 0))

(defmethod date-string ((request url-based-request))
  (format nil "~D" (expires request)))

(defun resource-url (&key bucket key vhost ssl sub-resource)
  (ecase vhost
    (:cname
     (format nil "http~@[s~*~]://~A/~@[~A~]~@[?~A~]"
             ssl bucket (url-encode key) sub-resource))
    (:amazon
     (format nil "http~@[s~*~]://~A.s3.amazonaws.com/~@[~A~]~@[?~A~]"
             ssl bucket (url-encode key) sub-resource))
    ((nil)
     (format nil "http~@[s~*~]://s3.amazonaws.com/~@[~A/~]~@[~A~]~@[?~A~]"
             ssl
             (url-encode bucket)
             (url-encode key :encode-slash nil)
             sub-resource))))

(defun authorized-url (&key bucket key vhost expires ssl sub-resource content-disposition content-type
                         ((:credentials *credentials*) *credentials*))
  (unless (and expires (integerp expires) (plusp expires))
    (error "~S option must be a positive integer" :expires))
  (let* ((region (bucket-region bucket))
         (region-endpoint (region-endpoint region))
         (endpoint (case vhost
                     (:cname bucket)
                     (:amazon (format nil "~A.~A" bucket region-endpoint))
                     (:wasabi (format nil "~a.s3.wasabisys.com" bucket))
                     ((nil) region-endpoint)))
         (extra-parameters (append (if content-disposition
                                       (list (cons "response-content-disposition" content-disposition)))
                                   (if content-type
                                       (list (cons "response-content-type" content-type)))))
         (request (make-instance 'url-based-request
                                 :method :get
                                 :bucket bucket
                                 :region region
                                 :endpoint endpoint
                                 :sub-resource sub-resource
                                 :key key
                                 :expires (unix-time expires)
                                 :parameters extra-parameters)))
    (setf (amz-headers request) nil)
    (setf (parameters request)
          (parameters-alist "X-Amz-Algorithm" "AWS4-HMAC-SHA256"
                            "X-Amz-Credential"
                            (format nil "~A/~A/~A/s3/aws4_request"
                                    (access-key *credentials*)
                                    (iso8601-basic-date-string (date request))
                                    (region request))
                            "X-Amz-Date" (iso8601-basic-timestamp-string (date request))
                            "X-Amz-Expires" (- expires (get-universal-time))
                            "X-Amz-SignedHeaders"
                            (format nil "~{~A~^;~}" (signed-headers request))))
    (push (cons "X-Amz-Signature" (request-signature request))
          (parameters request))
    (let ((parameters (alist-to-url-encoded-string (parameters request))))
      (case vhost
        (:cname
         (format nil "http~@[s~*~]://~A/~@[~A~]?~@[~A&~]~A"
                 ssl
                 bucket
                 (url-encode key :encode-slash nil)
                 sub-resource
                 parameters))
        (:amazon
         (format nil "http~@[s~*~]://~A/~@[~A~]?~@[~A&~]~A"
                 ssl
                 endpoint
                 (url-encode key :encode-slash nil)
                 sub-resource
                 parameters))
        (:wasabi
         (format nil "http~@[s~*~]://~A/~@[~A~]?~@[~A&~]~A"
                 ssl
                 endpoint
                 (url-encode key :encode-slash nil)
                 sub-resource
                 parameters))
        ((nil)
         (format nil "http~@[s~*~]://~A/~@[~A/~]~@[~A~]?~@[~A&~]~A"
                 ssl
                 endpoint
                 (url-encode bucket)
                 (url-encode key :encode-slash nil)
                 sub-resource
                 parameters))))))

;;; Miscellaneous operations

(defparameter *me-cache*
  (make-hash-table :test 'equal)
  "A cache for the result of the ME function. Keys are Amazon access
  key strings.")

(defun me (&key
             ((:credentials *credentials*) *credentials*)
             ((:backoff *backoff*) *backoff*))
  "Return a PERSON object corresponding to the current credentials. Cached."
  (or (gethash (access-key *credentials*) *me-cache*)
      (setf
       (gethash (access-key *credentials*) *me-cache*)
       (let ((response (submit-request (make-instance 'request))))
         (owner response)))))

(defun make-post-policy (&key expires conditions
                           ((:credentials *credentials*) *credentials*))
  "Return an encoded HTTP POST policy string and policy signature as
multiple values."
  (unless expires
    (error "~S is required" :expires))
  (let ((policy (make-instance 'post-policy
                               :expires expires
                               :conditions conditions)))
    (values (policy-string64 policy)
            (policy-signature (secret-key *credentials*) policy))))

;;; Tagging

(defbinder get-tagging-result
  ("Tagging"
   ("TagSet"
    (sequence :tag-set
              ("Tag"
               ("Key" (bind :key))
               ("Value" (bind :value)))))))

(defun get-tagging (&key bucket key
                      ((:credentials *credentials*) *credentials*)
                      ((:backoff *backoff*) *backoff*))
  "Returns the current contents of the object's tagging resource as an alist."
  (let* ((request (make-instance 'request
                                 :method :get
                                 :bucket bucket
                                 :key key
                                 :sub-resource "tagging"))
         (response (submit-request request))
         (tagging (xml-bind 'get-tagging-result (body response))))
    (mapcar #'(lambda (tag)
                (cons (bvalue :key tag)
                      (bvalue :value tag)))
            (bvalue :tag-set tagging))))

(defun put-tagging (tag-set &key bucket key
                              ((:credentials *credentials*) *credentials*)
                              ((:backoff *backoff*) *backoff*))
  "Sets the tag set, given as an alist, to the object's tagging resource."
  (let* ((content (with-xml-output
                    (with-element "Tagging"
                      (with-element "TagSet"
                        (dolist (tag tag-set)
                          (with-element "Tag"
                            (with-element "Key" (cxml:text (car tag)))
                            (with-element "Value" (cxml:text (cdr tag)))))))))
         (request (make-instance 'request
                                 :method :put
                                 :bucket bucket
                                 :key key
                                 :sub-resource "tagging"
                                 :content content)))
    (submit-request request)))

(defun delete-tagging (&key bucket key
                         ((:credentials *credentials*) *credentials*)
                         ((:backoff *backoff*) *backoff*))
  "Deletes the object's tagging resource."
  (let* ((request (make-instance 'request
                                 :method :delete
                                 :bucket bucket
                                 :key key
                                 :sub-resource "tagging")))
    (submit-request request)))
