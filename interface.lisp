;;;;
;;;; Copyright (c) 2008 Zachary Beane, All Rights Reserved
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
             ((:credentials *credentials*) *credentials*))
  "Return three values: the HTTP status, an alist of Drakma-style HTTP
headers, and the HTTP phrase, with the results of a HEAD request for
the object specified by the optional BUCKET and KEY arguments."
  (let ((response
         (submit-request (make-instance 'request
                                        :method :head
                                        :bucket bucket
                                        :key key
                                        :parameters parameters))))

    (values (http-headers response)
            (http-code response)
            (http-phrase response))))

;;; Operations on buckets

(defun all-buckets (&key ((:credentials *credentials*) *credentials*))
  "Return a vector of all BUCKET objects associated with *CREDENTIALS*."
  (let ((response (submit-request (make-instance 'request
                                                 :method :get))))
    (buckets response)))

(defun bucket-location (bucket &key
                        ((:credentials *credentials*) *credentials*))
  "If BUCKET was created with a LocationConstraint, return its
constraint."
  (let* ((request (make-instance 'request
                                  :method :get
                                  :sub-resource "location"
                                  :bucket bucket))
         (response (submit-request request))
         (location (location response)))
    (when (plusp (length location))
      location)))

(defun query-bucket (bucket &key prefix marker max-keys delimiter
                     ((:credentials *credentials*) *credentials*))
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
                 ((:credentials *credentials*) *credentials*))
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
                        ((:credentials *credentials*) *credentials*))
  (let ((code (nth-value 1 (head :bucket bucket
                                 :parameters
                                 (parameters-alist :max-keys 0)))))
    (not (<= 400 code 599))))

(defun create-bucket (name &key
                      access-policy
                      public
                      location
                      ((:credentials *credentials*) *credentials*))
  (let ((policy-header (access-policy-header access-policy public)))
    (submit-request (make-instance 'request
                                   :method :put
                                   :bucket name
                                   :content (and location
                                                 (location-constraint-xml
                                                  location))
                                   :amz-headers policy-header))))

(defun delete-bucket (bucket &key
                      ((:credentials *credentials*) *credentials*))
  (let* ((request (make-instance 'request
                                 :method :delete
                                 :bucket bucket))
         (endpoint (endpoint request))
         (bucket (bucket request)))
    (prog1
        (submit-request request)
      (setf (redirected-endpoint endpoint bucket) nil))))


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
    (with-open-stream (input (body response))
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
          (with-open-stream (input (body response))
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
                   ((:credentials *credentials*) *credentials*))
  (flet ((range-argument (start end)
           (when start
             (format nil "bytes=~D-~@[~D~]" start (and end (1- end)))))
         (maybe-date (time)
           (and time (http-date-string time))))
    (when (and end (not start))
      (setf start 0))
    (when (and start end (<= end start))
      (error "START must be less than END."))
    (let ((request (make-instance 'request
                                  :method :get
                                  :bucket bucket
                                  :key key
                                  :extra-http-headers
                                  (parameters-alist
                                   :connection "close"
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
                                :VECTOR, :STRING, or a pathname"
                                 :output output)))))
      (catch 'not-modified
        (handler-case
            (let ((response (submit-request request
                                            :keep-stream (eql output :stream)
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
                   ((:credentials *credentials*) *credentials*))
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
                   ((:credentials *credentials*) *credentials*))
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
                 ((:credentials *credentials*) *credentials*))
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
                   ((:credentials *credentials*) *credentials*))
  (let ((content
         (etypecase object
           (string
            (flexi-streams:string-to-octets object
                                            :external-format
                                            string-external-format))
           ((or vector pathname) object)))
        (content-length t)
        (policy-header (access-policy-header access-policy public)))
    (submit-request (make-instance 'request
                                   :method :put
                                   :bucket bucket
                                   :key key
                                   :metadata metadata
                                   :amz-headers policy-header
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
                   ((:credentials *credentials*) *credentials*))
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
              :expires expires))

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
                   ((:credentials *credentials*) *credentials*))
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
              :string-external-format external-format))


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
                 ((:credentials *credentials*) *credentials*))
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
                :expires expires)))

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
                   ((:credentials *credentials*) *credentials*))
  (let ((content (stream-subset-vector stream start end)))
    (put-object content bucket key
                :access-policy access-policy
                :public public
                :metadata metadata
                :cache-control cache-control
                :content-disposition content-disposition
                :content-encoding content-encoding
                :content-type content-type
                :expires expires)))


;;; Delete & copy objects

(defun delete-object (bucket key &key
                      ((:credentials *credentials*) *credentials*))
  "Delete one object from BUCKET identified by KEY."
  (submit-request (make-instance 'request
                                 :method :delete
                                 :bucket bucket
                                 :key key)))

(defun delete-objects (bucket keys &key
                       ((:credentials *credentials*) *credentials*))
  "Delete the objects in BUCKET identified by KEYS."
  (map nil
       (lambda (key)
         (delete-object bucket key))
       keys)
  (length keys))

(defun delete-all-objects (bucket &key
                           ((:credentials *credentials*) *credentials*))
  "Delete all objects in BUCKET."
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
                    ((:credentials *credentials*) *credentials*))
  "Copy the object identified by FROM-BUCKET/FROM-KEY to
TO-BUCKET/TO-KEY.

If TO-BUCKET is NIL, uses FROM-BUCKET as the target. If TO-KEY is NIL,
uses TO-KEY as the target.

If METADATA is provided, it should be an alist of metadata keys and
values to set on the new object. Otherwise, the source object's
metadata is copied.

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
  (unless (or to-bucket to-key)
    (error "Can't copy an object to itself."))
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
                             :metadata-directive
                             (if metadata-supplied-p "REPLACE" "COPY")
                             :copy-source-if-match when-etag-matches
                             :copy-source-if-none-match unless-etag-matches
                             :copy-source-if-modified-since
                             (and when-modified-since
                                  (http-date-string when-modified-since))
                             :copy-source-if-unmodified-since
                             (and unless-modified-since
                                  (http-date-string unless-modified-since))))
          (policy-header (access-policy-header access-policy public)))
      (submit-request (make-instance 'request
                                     :method :put
                                     :bucket to-bucket
                                     :key to-key
                                     :metadata metadata
                                     :amz-headers
                                     (nconc headers policy-header))))))


(defun object-metadata (bucket key &key
                        ((:credentials *credentials*) *credentials*))
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


;;; ACL twiddling

(defparameter *public-read-grant*
  (make-instance 'grant
                 :permission :read
                 :grantee *all-users*)
  "This grant is added to or removed from an ACL to grant or revoke
  read access for all users.")

(defun get-acl (&key bucket key
                ((:credentials *credentials*) *credentials*))
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
                ((:credentials *credentials*) *credentials*))
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
                    ((:credentials *credentials*) *credentials*))
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket :key key)
    (put-acl owner
             (cons *public-read-grant* grants)
             :bucket bucket
             :key key)))

(defun make-private (&key bucket key
                     ((:credentials *credentials*) *credentials*))
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
                          ((:credentials *credentials*) *credentials*))
  "Configure the ACL of BUCKET to accept logfile objects."
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket)
    (setf grants (append *log-delivery-grants* grants))
    (put-acl owner grants :bucket bucket)))

(defun disable-logging-to (bucket &key
                           ((:credentials *credentials*) *credentials*))
  "Configure the ACL of BUCKET to remove permissions for the log
delivery group."
  (multiple-value-bind (owner grants)
      (get-acl :bucket bucket)
    (setf grants (remove-if (lambda (grant)
                              (acl-eqv (grantee grant) *log-delivery*))
                            grants))
    (put-acl owner grants :bucket bucket)))

(defun enable-logging (bucket target-bucket target-prefix &key
                       target-grants
                       ((:credentials *credentials*) *credentials*))
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
                        ((:credentials *credentials*) *credentials*))
  "Disable the creation of access logs for BUCKET."
  (submit-request (make-instance 'request
                                 :method :put
                                 :sub-resource "logging"
                                 :bucket bucket
                                 :content *empty-logging-setup*)))

(defun logging-setup (bucket &key
                      ((:credentials *credentials*) *credentials*))
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
  (case vhost
    (:cname
     (format nil "http~@[s~*~]://~A/~@[~A~]~@[?~A~]"
             ssl bucket (url-encode key) sub-resource))
    (:amazon
     (format nil "http~@[s~*~]://~A.s3.amazonaws.com/~@[~A~]~@[?~A~]"
             ssl bucket (url-encode key) sub-resource))
    ((nil)
     (format nil "http~@[s~*~]://s3.amazonaws.com/~@[~A/~]~@[~A~]~@[?~A~]"
             ssl (url-encode bucket) (url-encode key) sub-resource))))

(defun authorized-url (&key bucket key vhost expires ssl sub-resource
                       ((:credentials *credentials*) *credentials*))
  (unless (and expires (integerp expires) (plusp expires))
    (error "~S option must be a positive integer" :expires))
  (let* ((request (make-instance 'url-based-request
                                 :method :get
                                 :bucket bucket
                                 :sub-resource sub-resource
                                 :key key
                                 :expires (unix-time expires)))
         (parameters
          (alist-to-url-encoded-string
           (list (cons "AWSAccessKeyId" (access-key *credentials*))
                 (cons "Expires" (format nil "~D" (expires request)))
                 (cons "Signature"
                       (signature request))))))
    (case vhost
      (:cname
       (format nil "http~@[s~*~]://~A/~@[~A~]?~@[~A&~]~A"
               ssl bucket (url-encode key) sub-resource parameters))
      (:amazon
       (format nil "http~@[s~*~]://~A.s3.amazonaws.com/~@[~A~]?~@[~A&~]~A"
               ssl bucket (url-encode key) sub-resource parameters))
      ((nil)
       (format nil "http~@[s~*~]://s3.amazonaws.com/~@[~A/~]~@[~A~]?~@[~A&~]~A"
               ssl (url-encode bucket) (url-encode key) sub-resource
                   parameters)))))


;;; Miscellaneous operations

(defparameter *me-cache*
  (make-hash-table :test 'equal)
  "A cache for the result of the ME function. Keys are Amazon access
  key strings.")

(defun me (&key
           ((:credentials *credentials*) *credentials*))
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
