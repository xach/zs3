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
;;;; request.lisp

(in-package #:zs3)

(defvar *s3-endpoint* "s3.amazonaws.com")
(defvar *s3-region* "us-east-1")
(defvar *use-ssl* nil)
(defvar *use-content-md5* t)
(defvar *signed-payload* nil
  "When true, compute the SHA256 hash for the body of all requests
  when submitting to AWS.")

(defvar *use-keep-alive* nil
  "When set to t, this library uses the drakma client with
   keep alive enabled. This means that a stream will be reused for multiple
   requests. The stream itself will be bound to *keep-alive-stream*")


(defvar *keep-alive-stream* nil
  "When using http keep-alive, this variable is bound to the stream
   which is being kept open for repeated usage.  It is up to client code
   to ensure that only one thread at a time is making requests that
   could use the same stream object concurrently.  One way to achive
   this would be to create a separate binding per thread.  The
   with-keep-alive macro can be useful here.")


(defmacro with-keep-alive (&body body)
  "Create thread-local bindings of the zs3 keep-alive variables around a
   body of code.  Ensure the stream is closed at exit."
  `(let ((*use-keep-alive* t)
         (*keep-alive-stream* nil))
     (unwind-protect
         (progn ,@body)
       (when *keep-alive-stream*
         (ignore-errors (close *keep-alive-stream*))))))


(defclass request ()
  ((credentials
    :initarg :credentials
    :accessor credentials
    :documentation "An object that has methods for ACCESS-KEY and
    SECRET-KEY. A list of two strings (the keys) suffices.")
   (endpoint
    :initarg :endpoint
    :accessor endpoint)
   (region
    :initarg :region
    :accessor region)
   (ssl
    :initarg :ssl
    :accessor ssl)
   (method
    :initarg :method
    :accessor method
    :documentation "e.g. :GET, :PUT, :DELETE")
   (bucket
    :initarg :bucket
    :accessor bucket
    :documentation
    "A string naming the bucket to address in the request. If NIL,
    request is not directed at a specific bucket.")
   (key
    :initarg :key
    :accessor key
    :documentation
    "A string naming the key to address in the request. If NIL,
    request is not directed at a specific key.")
   (sub-resource
    :initarg :sub-resource
    :accessor sub-resource
    :documentation "A sub-resource to address as part of the request,
    without a leading \"?\", e.g. \"acl\", \"torrent\". If PARAMETERS
    is set, this must be NIL.")
   (parameters
    :initarg :parameters
    :accessor parameters
    :documentation
    "An alist of string key/value pairs to send as CGI-style GET
    parameters with the request. If SUB-RESOURCE is set, these must be
    NIL.")
   (content-type
    :initarg :content-type
    :accessor content-type)
   (content-md5
    :initarg :content-md5
    :accessor content-md5)
   (content-length
    :initarg :content-length
    :accessor content-length)
   (content
    :initarg :content
    :accessor content)
   (metadata
    :initarg :metadata
    :accessor metadata
    :documentation
    "An alist of Amazon metadata to attach to a request. These should
    be straight string key/value pairs, WITHOUT any \"x-amz-meta-\"
    prefix.")
   (amz-headers
    :initarg :amz-headers
    :accessor amz-headers
    :documentation
    "An alist of extra Amazon request headers. These should be
    straight string key/value pairs, WITHOUT any \"x-amz-\" prefix.")
   (date
    :initarg :date
    :accessor date)
   (signed-string
    :initarg :signed-string
    :accessor signed-string)
   (extra-http-headers
    :initarg :extra-http-headers
    :accessor extra-http-headers
    :documentation "An alist of extra HTTP headers to include in the request."))
  (:default-initargs
   ;; :date and :content-md5 are specially treated, should not be nil
   :credentials *credentials*
   :method :get
   :endpoint *s3-endpoint*
   :region *s3-region*
   :ssl *use-ssl*
   :bucket nil
   :key nil
   :sub-resource nil
   :parameters nil
   :content-type nil
   :content-length t
   :content nil
   :metadata nil
   :amz-headers nil
   :extra-http-headers nil))

(defmethod slot-unbound ((class t) (request request) (slot (eql 'date)))
  (setf (date request) (get-universal-time)))

(defmethod slot-unbound ((class t) (request request) (slot (eql 'content-md5)))
  (setf (content-md5 request)
        (when *use-content-md5*
          (let ((content (content request)))
            (cond ((pathnamep content) (file-md5/b64 content))
                  ((stringp content)
                   (vector-md5/b64
                    (flexi-streams:string-to-octets content)))
                  ((vectorp content) (vector-md5/b64 content)))))))

(defmethod slot-unbound ((class t) (request request) (slot (eql 'signed-string)))
  (setf (signed-string request)
        (format nil "~{~A~^~%~}" (string-to-sign-lines request))))

(defgeneric amz-header-value (request name)
  (:method (request name)
    (cdr (assoc name (amz-headers request) :test 'string=))))

(defgeneric ensure-amz-header (request name value)
  (:method (request name value)
    (unless (amz-header-value request name)
      (push (cons name value) (amz-headers request)))))

(defmethod initialize-instance :after ((request request)
                                       &rest initargs &key
                                       &allow-other-keys)
  (declare (ignore initargs))
  (when (eql (method request) :head)
    ;; https://forums.aws.amazon.com/thread.jspa?messageID=340398 -
    ;; when using the bare endpoint, the 301 redirect for a HEAD
    ;; request does not include enough info to actually redirect. Use
    ;; the bucket endpoint pre-emptively instead
    (setf (endpoint request) (format nil "~A.~A"
                                     (bucket request)
                                     *s3-endpoint*)))
  (ensure-amz-header request "date"
                     (iso8601-basic-timestamp-string (date request)))
  (ensure-amz-header request "content-sha256"
                     (payload-sha256 request))
  (let ((target-region (redirected-region (endpoint request)
                                          (bucket request))))
    (when target-region
      (setf (region request) target-region)))
  (when (content-md5 request)
    (push (cons "Content-MD5" (content-md5 request)) (extra-http-headers request)))
  (unless (integerp (content-length request))
    (let ((content (content request)))
      (setf (content-length request)
            (etypecase content
              (null 0)
              (pathname (file-size content))
              (vector (length content)))))))

(defgeneric host (request)
  (:method ((request request))
    (or (redirected-endpoint (endpoint request) (bucket request))
        (endpoint request))))

(defgeneric http-method (request)
  (:method (request)
    (string-upcase (method request))))

(defun puri-canonicalized-path (path)
  (let ((parsed (puri:parse-uri (format nil "http://dummy~A" path))))
    (with-output-to-string (stream)
      (if (puri:uri-path parsed)
          (write-string (puri:uri-path parsed) stream)
          (write-string "/" stream))
      (when (puri:uri-query parsed)
        (write-string "?" stream)
        (write-string (puri:uri-query parsed) stream)))))

(defgeneric signed-path (request)
  (:method (request)
    (let ((*print-pretty* nil))
      (puri-canonicalized-path
       (with-output-to-string (stream)
         (write-char #\/ stream)
         (when (bucket request)
           (write-string (url-encode (name (bucket request))) stream)
           (write-char #\/ stream))
         (when (key request)
           (write-string (url-encode (name (key request)) :encode-slash nil)
                         stream))
         (when (sub-resource request)
           (write-string "?" stream)
           (write-string (url-encode (sub-resource request)) stream)))))))

(defgeneric request-path (request)
  (:method (request)
    (let ((*print-pretty* nil))
      (with-output-to-string (stream)
        (write-char #\/ stream)
        (when (and (bucket request)
                   (string= (endpoint request)
                            (region-endpoint (region request))))
          (write-string (url-encode (name (bucket request))) stream)
          (write-char #\/ stream))
        (when (key request)
          (write-string (url-encode (name (key request))
                                    :encode-slash nil) stream))
        (when (sub-resource request)
          (write-string "?" stream)
          (write-string (url-encode (sub-resource request)) stream))))))

(defgeneric all-amazon-headers (request)
  (:method (request)
    (nconc
     (loop for ((key . value)) on (amz-headers request)
           collect (cons (format nil "x-amz-~(~A~)" key)
                         value))
     (loop for ((key . value)) on (metadata request)
           collect (cons (format nil "x-amz-meta-~(~A~)" key)
                         value)))))

(defgeneric date-string (request)
  (:method (request)
    (http-date-string (date request))))

;;; AWS 4 authorization

(defun headers-for-signing (request)
  (append (all-amazon-headers request)
          (extra-http-headers request)
          (parameters-alist "host" (host request)
                            "content-type" (content-type request))))

(defun canonical-headers (headers)
  (flet ((trim (string)
           (string-trim " " string)))
    (let ((encoded
           (loop for (name . value) in headers
                 collect (cons (string-downcase name)
                               (trim value)))))
      (sort encoded #'string< :key 'car))))

(defun signed-headers (request)
  (mapcar 'first (canonical-headers (headers-for-signing request))))

(defun parameters-for-signing (request)
  (cond ((sub-resource request)
         (list (cons (sub-resource request) "")))
        (t
         (parameters request))))

(defun canonical-parameters (parameters)
  (let ((encoded
         (loop for (name . value) in parameters
               collect (cons
                        (url-encode name)
                        (url-encode value)))))
    (sort encoded #'string< :key 'car)))

(defun canonical-parameters-string (request)
  (format nil "~{~A=~A~^&~}"
                (alist-plist (canonical-parameters
                              (parameters-for-signing request)))))

(defun path-to-sign (request)
  "Everything in the PATH of the request, up to the first ?"
  (let ((path (request-path request)))
    (subseq path 0 (position #\? path))))

(defun canonicalized-request-lines (request)
  "Return a list of lines canonicalizing the request according to
http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html."
  (let* ((headers (headers-for-signing request))
         (canonical-headers (canonical-headers headers)))
    (alexandria:flatten
     (list (http-method request)
           (path-to-sign request)
           (canonical-parameters-string request)
           (loop for (name . value) in canonical-headers
                 collect (format nil "~A:~A" name value))
           ""
           (format nil "~{~A~^;~}" (signed-headers request))
           (or (amz-header-value request "content-sha256")
               "UNSIGNED-PAYLOAD")))))

(defun string-to-sign-lines (request)
  "Return a list of strings to sign to construct the Authorization header."
  (list "AWS4-HMAC-SHA256"
        (iso8601-basic-timestamp-string (date request))
        (with-output-to-string (s)
          (format s "~A/~A/s3/aws4_request"
                  (iso8601-basic-date-string (date request))
                  (region request)))
        (strings-sha256/hex (canonicalized-request-lines request))))

(defun make-signing-key (credentials &key region service)
  "The signing key is derived from the credentials, region, date, and
service. A signing key could be saved, shared, and reused, but ZS3 just recomputes it all the time instead."
  (let* ((k1 (format nil "AWS4~A" (secret-key credentials)))
         (date-key (hmac-sha256 k1 (iso8601-basic-date-string)))
         (region-key (hmac-sha256 date-key region))
         (service-key (hmac-sha256 region-key service)))
    (hmac-sha256 service-key "aws4_request")))

(defun payload-sha256 (request)
  (if *signed-payload*
      (let ((payload (content request)))
        (etypecase payload
          ((or null empty-vector)
           *empty-string-sha256*)
          (vector
           (vector-sha256/hex payload))
          (pathname
           (file-sha256/hex payload))))
      "UNSIGNED-PAYLOAD"))

(defun request-signature (request)
  (let ((key (make-signing-key *credentials*
                               :region (region request)
                               :service "s3")))
    (strings-hmac-sha256/hex key (string-to-sign-lines request) )))

(defmethod authorization-header-value ((request request))
  (let ((key (make-signing-key *credentials*
                               :region (region request)
                               :service "s3"))
        (lines (string-to-sign-lines request)))
    (with-output-to-string (s)
      (write-string "AWS4-HMAC-SHA256" s)
      (format s " Credential=~A/~A/~A/s3/aws4_request"
              (access-key *credentials*)
              (iso8601-basic-date-string (date request))
              (region request))
      (format s ",SignedHeaders=~{~A~^;~}" (signed-headers request))
      (format s ",Signature=~A"
              (strings-hmac-sha256/hex key lines)))))

(defgeneric drakma-headers (request)
  (:method (request)
    (let ((base
           (list* (cons "Date" (http-date-string (date request)))
                  (cons "Authorization"
                        (authorization-header-value request))
                  (all-amazon-headers request))))
      (append (extra-http-headers request) base))))

(defgeneric url (request)
  (:method (request)
    (format nil "http~@[s~*~]://~A~A"
            (ssl request)
            (endpoint request)
            (request-path request))))

(defun send-file-content (fun request)
  (with-open-file (stream (content request)
                          :element-type '(unsigned-byte 8))
    (let* ((buffer-size 8000)
           (buffer (make-octet-vector buffer-size)))
      (flet ((read-exactly (size)
               (assert (= size (read-sequence buffer stream)))))
        (multiple-value-bind (loops rest)
            (truncate (content-length request) buffer-size)
          (dotimes (i loops)
            (read-exactly buffer-size)
            (funcall fun buffer t))
          (read-exactly rest)
          (funcall fun (subseq buffer 0 rest) nil))))))

(defgeneric send (request &key want-stream stream)
  (:method (request &key want-stream stream)
    (let ((continuation
           (drakma:http-request (url request)
                                :redirect nil
                                :want-stream want-stream
                                :stream stream
                                :keep-alive *use-keep-alive*
                                :close (not *use-keep-alive*)
                                :content-type (content-type request)
                                :additional-headers (drakma-headers request)
                                :method (method request)
                                :force-binary t
                                :content-length (content-length request)
                                :parameters (parameters request)
                                :content :continuation)))
      (let ((content (content request)))
        (if (pathnamep content)
            (send-file-content continuation request)
            (funcall continuation content nil))))))

(defmethod access-key ((request request))
  (access-key (credentials request)))

(defmethod secret-key ((request request))
  (secret-key (credentials request)))

(defmethod security-token ((request request))
  (security-token (credentials request)))
