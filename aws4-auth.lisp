;;;; aws4-auth.lisp

(in-package #:zs3)

;;; http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
;;; can be used to map endpoint to region, maybe?

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

(defun hashed-payload (request)
  *empty-string-sha256*)

(defun path-to-sign (request)
  "Everything in the PATH of the request, up to the first ?"
  (let ((path (request-path request)))
    (subseq path 0 (position #\? path))))

(defun canonicalized-request-strings (request)
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
           (format nil "~{~A~^;~}" (mapcar 'first canonical-headers))
           (hashed-payload request)))))

(defun string-to-sign-lines (request)
  "Return a list of strings to sign to construc"
  (list "AWS4-HMAC-SHA256"
        (iso8601-basic-timestamp-string)
        (with-output-to-string (s)
          (format s "~A/~A/s3/aws4_request"
                  (iso8601-basic-date-string)
                  (region request)))
        (strings-sha256/hex (canonicalized-request-strings request))))

(defun make-signing-key (credentials &key region service)
  (let* ((k1 (format nil "AWS4~A" (secret-key credentials)))
         (date-key (hmac-sha256 k1 (iso8601-basic-date-string)))
         (region-key (hmac-sha256 date-key region))
         (service-key (hmac-sha256 region-key service)))
    (hmac-sha256 service-key "aws4_request")))

(defclass aws4-auth-request (request)
  ((region
    :accessor region
    :initarg :region))
  (:default-initargs
   :region "us-east-1"))

(defmethod authorization-header-value ((request aws4-auth-request))
  (let ((key (make-signing-key *credentials*
                               :region (region request)
                               :service "s3")))
    (with-output-to-string (s)
      (write-string "AWS4-HMAC-SHA256" s)
      (format s " Credential=~A/~A/~A/s3/aws4_request"
              (access-key *credentials*)
              (iso8601-basic-date-string)
              (region request))
      (format s ",SignedHeaders=~{~A~^;~}" (signed-headers request))
      (format s ",Signature=~A"
              (strings-hmac-sha256/hex key (string-to-sign-lines request))))))

(defun test-aws4 (&key (region "us-east-1"))
  (let ((request (make-instance 'aws4-auth-request
                                :amz-headers
                                (parameters-alist :content-sha256 *empty-string-sha256*
                                                  :date (iso8601-basic-timestamp-string))
                                :region region)))
    request))
