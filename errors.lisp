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
;;;; errors.lisp

(in-package #:zs3)

(defbinder error
  ("Error"
   ("Code" (bind :code))
   ("Message" (bind :message))
   (elements-alist :data)))

(defclass amazon-error (response)
  ((code
    :initarg :code
    :accessor code)
   (message
    :initarg :message
    :accessor message)
   (error-data
    :initarg :error-data
    :accessor error-data)))

(set-element-class "Error" 'amazon-error)

(defgeneric error-data-value (name instance)
  (:method (name (response amazon-error))
    (cdr (assoc name (error-data response) :test #'equalp))))

(defmethod specialized-initialize ((response amazon-error) source)
  (let ((bindings (xml-bind 'error source)))
    (setf (code response) (bvalue :code bindings))
    (setf (message response) (bvalue :message bindings))
    (setf (error-data response) (bvalue :data bindings))))

(defmethod specialized-initialize ((response amazon-error) (source null))
  (setf (code response) "InternalError"
        (message response) nil
        (error-data response) nil))

(defmethod print-object ((response amazon-error) stream)
  (print-unreadable-object (response stream :type t)
    (prin1 (code response) stream)))

;;; Further specializing error messages/conditions

(defun report-request-error (condition stream)
  (format stream "~A~@[: ~A~]"
          (code (request-error-response condition))
          (message (request-error-response condition))))

(define-condition request-error (error)
  ((request
    :initarg :request
    :reader request-error-request)
   (response
    :initarg :response
    :reader request-error-response)
   (data
    :initarg :data
    :reader request-error-data))
  (:report report-request-error))

(defparameter *specific-errors* (make-hash-table :test 'equalp))

(defun specific-error (amazon-code)
  (gethash amazon-code *specific-errors* 'request-error))

(defgeneric signal-specific-error (response condition-name)
  (:method (response (condition-name t))
    (error 'request-error
           :request (request response)
           :response response
           :data (error-data response))))

(defgeneric maybe-signal-error (response)
  (:method ((response t))
    t)
  (:method ((response amazon-error))
    (signal-specific-error response (specific-error (code response)))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun error-reader-name (suffix)
    (intern (concatenate 'string (symbol-name 'request-error)
                         "-"
                         (symbol-name suffix))
            :zs3)))

(defmacro define-specific-error ((condition-name code)
                                 superclasses
                                 slots &rest options)
  (labels ((slot-name (slot)
             (first slot))
           (slot-code (slot)
             (second slot))
           (slot-keyword (slot)
             (keywordify (slot-name slot)))
           (slot-definition (slot)
             `(,(slot-name slot)
               :initarg ,(slot-keyword slot)
               :reader ,(error-reader-name (slot-name slot))))
           (slot-initializer (slot)
             (list (slot-keyword slot)
                   `(error-data-value ,(slot-code slot) response))))
    `(progn
       (setf (gethash ,code *specific-errors*) ',condition-name)
       (define-condition ,condition-name (,@superclasses request-error)
         ,(mapcar #'slot-definition slots)
         ,@options)
       (defmethod signal-specific-error ((response amazon-error)
                                         (condition-name (eql ',condition-name)))
         (error ',condition-name
                :request (request response)
                :response response
                :data (error-data response)
                ,@(mapcan #'slot-initializer slots))))))


;;; The specific errors

(define-specific-error (internal-error "InternalError") () ())

(define-specific-error (slow-down "SlowDown") () ())

(define-specific-error (no-such-bucket "NoSuchBucket") ()
  ((bucket-name "BucketName")))

(define-specific-error (no-such-key "NoSuchKey") ()
  ((key-name "Key")))

(define-specific-error (access-denied "AccessDenied") () ())

(define-specific-error (malformed-xml "MalformedXML") () ())

(define-condition redirect-error (error) ())

(define-specific-error (permanent-redirect "PermanentRedirect") (redirect-error)
  ((endpoint "Endpoint")))

(define-specific-error (temporary-redirect "TemporaryRedirect") (redirect-error)
  ((endpoint "Endpoint")))

(define-specific-error (signature-mismatch "SignatureDoesNotMatch") ()
  ((string-to-sign "StringToSign")
   (canonical-request "CanonicalRequest"))
  (:report (lambda (condition stream)
             (report-request-error condition stream)
             (format stream "You signed: ~S~%Amazon signed: ~S~%and~%~S"
                            (signed-string (request-error-request condition))
                            (request-error-string-to-sign condition)
                            (request-error-canonical-request condition)))))

(define-specific-error (precondition-failed "PreconditionFailed") ()
  ((condition "Condition")))

(define-specific-error (authorization-header-malformed
                        "AuthorizationHeaderMalformed") ()
  ((region "Region")))


(define-condition linked ()
  ((url
    :initarg :url
    :reader linked-url))
  (:report (lambda (condition stream)
             (report-request-error condition stream)
             (format stream "~&For more information, see:~%  ~A"
                            (linked-url condition)))))


(define-condition bucket-restrictions (linked)
  ()
  (:default-initargs
   :url "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"))

(define-specific-error (invalid-bucket-name "InvalidBucketName")
    (bucket-restrictions)
  ())

(define-specific-error (bucket-exists "BucketAlreadyExists")
    (bucket-restrictions)
  ())

(define-specific-error (too-many-buckets "TooManyBuckets")
    (bucket-restrictions)
  ())


(define-specific-error (ambiguous-grant "AmbiguousGrantByEmailAddress") (linked)
  ()
  (:default-initargs
   :url "http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTacl.html"))

(define-specific-error (bucket-not-empty "BucketNotEmpty") (linked)
  ()
  (:default-initargs
   :url "http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html"))

(define-specific-error (invalid-logging-target "InvalidTargetBucketForLogging")
    () ())

(define-specific-error (key-too-long "KeyTooLong") (linked)
  ()
  (:default-initargs
   :url "http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html"))

(define-specific-error (request-time-skewed "RequestTimeTooSkewed") (linked)
  ()
  (:default-initargs
   :url "http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationTimeStamp"))

(define-specific-error (operation-aborted "OperationAborted") () ())
