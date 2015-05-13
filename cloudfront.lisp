;;;;
;;;; Copyright (c) 2009 Zachary Beane, All Rights Reserved
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
;;;; cloudfront.lisp

(in-package #:zs3)

(defvar *canonical-bucket-name-suffix*
  ".s3.amazonaws.com")

(defparameter *caller-reference-counter* 8320208)

(defparameter *cloudfront-base-url*
  "https://cloudfront.amazonaws.com/2010-08-01/distribution")

;;; Errors

(defparameter *distribution-specific-errors*
  (make-hash-table :test 'equal)
  "This table is used to signal the most specific error possible for
distribution request error responses.")

(defbinder distribution-error-response
  ("ErrorResponse"
   ("Error"
    ("Type" (bind :type))
    ("Code" (bind :code))
    ("Message" (bind :message))
    (optional
     ("Detail" (bind :detail))))
   ("RequestId" (bind :request-id))))

(define-condition distribution-error (error)
  ((error-type
    :initarg :error-type
    :accessor distribution-error-type)
   (error-code
    :initarg :error-code
    :accessor distribution-error-code)
   (http-status-code
    :initarg :http-status-code
    :accessor distribution-error-http-status-code)
   (error-message
    :initarg :error-message
    :accessor distribution-error-message)
   (error-detail
    :initarg :error-detail
    :accessor distribution-error-detail))
  (:report (lambda (condition stream)
             (format stream "~A error ~A: ~A"
                     (distribution-error-type condition)
                     (distribution-error-code condition)
                     (distribution-error-message condition)))))

(defmacro define-specific-distribution-error (error-xml-code error-name)
  `(progn
     (setf (gethash ,error-xml-code *distribution-specific-errors*)
           ',error-name)
     (define-condition ,error-name (distribution-error) ())))

(define-specific-distribution-error "InvalidIfMatchVersion"
    invalid-if-match-version)

(define-specific-distribution-error "PreconditionFailed"
    distribution-precondition-failed)

(define-specific-distribution-error "DistributionNotDisabled"
    distribution-not-disabled)

(define-specific-distribution-error "CNAMEAlreadyExists"
    cname-already-exists)

(define-specific-distribution-error "TooManyDistributions"
    too-many-distributions)

(defun maybe-signal-distribution-error (http-status-code content)
  (when (and content
             (plusp (length content))
             (string= (xml-document-element content) "ErrorResponse"))
    (let* ((bindings (xml-bind 'distribution-error-response
                               content))
           (condition (gethash (bvalue :code bindings)
                               *distribution-specific-errors*
                               'distribution-error)))
      (error condition
             :http-status-code http-status-code
             :error-type (bvalue :type bindings)
             :error-code (bvalue :code bindings)
             :error-message (bvalue :message bindings)
             :error-detail (bvalue :detail bindings)))))


;;; Distribution objects

(defun canonical-distribution-bucket-name (name)
  (if (ends-with *canonical-bucket-name-suffix* name)
      name
      (concatenate 'string name *canonical-bucket-name-suffix*)))

(defun generate-caller-reference ()
  (format nil "~D.~D"
          (get-universal-time)
          (incf *caller-reference-counter*)))

(defclass distribution ()
  ((origin-bucket
    :initarg :origin-bucket
    :accessor origin-bucket
    :documentation
    "The S3 bucket that acts as the source of objects for the distribution.")
   (caller-reference
    :initarg :caller-reference
    :accessor caller-reference
    :initform (generate-caller-reference)
    :documentation
    "A unique value provided by the caller to prevent replays. See
    http://docs.amazonwebservices.com/AmazonCloudFront/2008-06-30/DeveloperGuide/index.html?AboutCreatingDistributions.html")
   (enabledp
    :initarg :enabledp
    :initform t
    :accessor enabledp
    :documentation
    "Whether this distribution should be enabled at creation time or not.")
   (cnames
    :initarg :cnames
    :accessor cnames)
   (default-root-object
    :initarg :default-root-object
    :accessor default-root-object
    :initform nil)
   (comment
    :initarg :comment
    :initform nil
    :accessor comment)
   (logging-bucket
    :initarg :logging-bucket
    :initform nil
    :accessor logging-bucket)
   (logging-prefix
    :initarg :logging-prefix
    :initform nil
    :accessor logging-prefix)
   (id
    :initarg :id
    :accessor id
    :documentation
    "Amazon's assigned unique ID.")
   (domain-name
    :initarg :domain-name
    :accessor domain-name
    :documentation
    "Amazon's assigned domain name.")
   (etag
    :initarg :etag
    :accessor etag
    :initform nil)
   (status
    :initarg :status
    :accessor status
    :initform nil
    :documentation "Assigned by Amazon.")
   (last-modified
    :initarg :last-modified
    :accessor last-modified
    :documentation "Assigned by Amazon.")))

(defmethod print-object ((distribution distribution) stream)
  (print-unreadable-object (distribution stream :type t)
    (format stream "~A for ~S~@[ [~A]~]"
            (id distribution)
            (origin-bucket distribution)
            (status distribution))))

(defmethod initialize-instance :after ((distribution distribution)
                                       &rest initargs
                                       &key &allow-other-keys)
  (declare (ignore initargs))
  (setf (origin-bucket distribution)
        (canonical-distribution-bucket-name (origin-bucket distribution))))


;;; Distribution-related requests

(defun distribution-document (distribution)
  (with-xml-output
    (with-element "DistributionConfig"
      (attribute "xmlns" "http://cloudfront.amazonaws.com/doc/2010-08-01/")
      (with-element "Origin"
        (text (origin-bucket distribution)))
      (with-element "CallerReference"
        (text (caller-reference distribution)))
      (dolist (cname (cnames distribution))
        (with-element "CNAME"
          (text cname)))
      (when (comment distribution)
        (with-element "Comment"
          (text (comment distribution))))
      (with-element "Enabled"
        (text (if (enabledp distribution)
                  "true"
                  "false")))
      (when (default-root-object distribution)
        (with-element "DefaultRootObject"
          (text (default-root-object distribution))))
      (let ((logging-bucket (logging-bucket distribution))
            (logging-prefix (logging-prefix distribution)))
        (when (and logging-bucket logging-prefix)
          (with-element "Logging"
            (with-element "Bucket" (text logging-bucket))
            (with-element "Prefix" (text logging-prefix))))))))

(defun distribution-request-headers (distribution)
  (let* ((date (http-date-string))
         (signature (sign-string (secret-key *credentials*)
                                 date)))
    (parameters-alist :date date
                      :authorization
                      (format nil "AWS ~A:~A"
                              (access-key *credentials*)
                              signature)
                      :if-match (and distribution (etag distribution)))))


(defun distribution-request (&key distribution (method :get)
                             parameters url-suffix content
                             ((:credentials *credentials*) *credentials*))
  (let ((url (format nil "~A~@[~A~]" *cloudfront-base-url* url-suffix)))
    (multiple-value-bind (content code headers uri stream must-close-p phrase)
        (drakma:http-request url
                             :method method
                             :parameters parameters
                             :content-length t
                             :keep-alive nil
                             :want-stream nil
                             :content-type "text/xml"
                             :additional-headers (distribution-request-headers distribution)
                             :content
                             (or content
                                 (and distribution
                                      (member method '(:post :put))
                                      (distribution-document distribution))))
      (declare (ignore uri must-close-p))
      (ignore-errors (close stream))
      (maybe-signal-distribution-error code content)
      (values content headers code phrase))))

(defbinder distribution-config
  ("DistributionConfig"
   ("Origin" (bind :origin))
   ("CallerReference" (bind :caller-reference))
   (sequence :cnames
             ("CNAME" (bind :cname)))
   (optional ("Comment" (bind :comment)))
   ("Enabled" (bind :enabled))
   (optional
    ("Logging"
     ("Bucket" (bind :logging-bucket))
     ("Prefix" (bind :logging-prefix))))
   (optional
    ("DefaultRootObject" (bind :default-root-object)))))

(defbinder distribution
  ("Distribution"
    ("Id" (bind :id))
    ("Status" (bind :status))
    ("LastModifiedTime" (bind :last-modified-time))
    ("InProgressInvalidationBatches" (bind :in-progress-invalidation-batches))
    ("DomainName" (bind :domain-name))
    (include distribution-config)))

(defun bindings-distribution (bindings)
  (let ((timestamp (bvalue :last-modified-time bindings)))
    (make-instance 'distribution
                   :id (bvalue :id bindings)
                   :status (bvalue :status bindings)
                   :caller-reference (bvalue :caller-reference bindings)
                   :domain-name (bvalue :domain-name bindings)
                   :origin-bucket (bvalue :origin bindings)
                   :cnames (mapcar (lambda (b) (bvalue :cname b))
                                   (bvalue :cnames bindings))
                   :comment (bvalue :comment bindings)
                   :logging-bucket (bvalue :logging-bucket bindings)
                   :logging-prefix (bvalue :logging-prefix bindings)
                   :default-root-object (bvalue :default-root-object bindings)
                   :enabledp (equal (bvalue :enabled bindings) "true")
                   :last-modified (and timestamp
                                       (parse-amazon-timestamp timestamp)))))

;;; Distribution queries, creation, and manipulation

(defun put-config (distribution)
  "Post DISTRIBUTION's configuration to AWS. Signals an error and does
not retry in the event of an etag match problem."
  (multiple-value-bind (document headers code)
      (distribution-request :distribution distribution
                            :url-suffix (format nil "/~A/config"
                                                (id distribution))
                            :method :put)
    (declare (ignore document headers))
    (<= 200 code 299)))

(defun latest-version (distribution)
  (multiple-value-bind (document headers)
      (distribution-request :url-suffix (format nil "/~A" (id distribution)))
    (let ((new (bindings-distribution (xml-bind 'distribution
                                                document))))
      (setf (etag new) (bvalue :etag headers))
      new)))

(defun merge-into (distribution new)
  "Copy slot values from NEW into DISTRIBUTION."
  (macrolet ((sync (accessor)
               `(setf (,accessor distribution) (,accessor new))))
    (sync origin-bucket)
    (sync caller-reference)
    (sync etag)
    (sync enabledp)
    (sync cnames)
    (sync comment)
    (sync default-root-object)
    (sync logging-bucket)
    (sync logging-prefix)
    (sync domain-name)
    (sync status)
    (sync last-modified))
  distribution)

(defgeneric refresh (distribution)
  (:documentation
   "Pull down the latest data from AWS for DISTRIBUTION and update its slots.")
  (:method ((distribution distribution))
    (merge-into distribution (latest-version distribution))))

(defun call-with-latest (fun distribution)
  "Call FUN on DISTRIBUTION; if there is an ETag-related error,
retries after REFRESHing DISTRIBUTION. FUN should not have side
effects on anything but the DISTRIBUTION itself, as it may be re-tried
multiple times."
  (block nil
    (tagbody
     retry
       (handler-bind
           (((or invalid-if-match-version distribution-precondition-failed)
             (lambda (c)
               (declare (ignore c))
               (setf distribution (refresh distribution))
               (go retry))))
         (return (funcall fun distribution))))))

(defun modify-and-save (fun distribution)
  "Call the modification function FUN with DISTRIBUTION as its only
argument, and push the modified configuration to Cloudfront. May
refresh DISTRIBUTION if needed. FUN should not have side effects on
anything but the DISTRIBUTION itself, as it may be re-tried multiple
times."
  (call-with-latest (lambda (distribution)
                      (multiple-value-prog1
                          (funcall fun distribution)
                        (put-config distribution)))
                    distribution))

(defmacro with-saved-modifications ((var distribution) &body body)
  "Make a series of changes to DISTRIBUTION and push the final result
to AWS. BODY should not have side-effects on anything but the
DISTRIBUTION itself, as it may be re-tried multiple times."
  `(modify-and-save (lambda (,var)
                      ,@body)
    ,distribution))

(defbinder distribution-list
  ("DistributionList"
   ("Marker" (bind :marker))
   (optional
    ("NextMarker" (bind :next-marker)))
   ("MaxItems" (bind :max-items))
   ("IsTruncated" (bind :is-truncateD))
   (sequence :distributions
             ("DistributionSummary"
              ("Id" (bind :id))
              ("Status" (bind :status))
              ("LastModifiedTime" (bind :last-modified-time))
              ("DomainName" (bind :domain-name))
              ("Origin" (bind :origin))
              (sequence :cnames ("CNAME" (bind :cname)))
              (optional ("Comment" (bind :comment)))
              ("Enabled" (bind :enabled))))))

(defun all-distributions (&key ((:credentials *credentials*) *credentials*))
  (let* ((document (distribution-request))
         (bindings (xml-bind 'distribution-list document)))
    (mapcar (lambda (b)
              (bindings-distribution b))
            (bvalue :distributions bindings))))

(defun create-distribution (bucket-name &key cnames (enabled t) comment)
  (unless (listp cnames)
    (setf cnames (list cnames)))
  (let ((distribution (make-instance 'distribution
                                     :origin-bucket bucket-name
                                     :enabledp enabled
                                     :comment comment
                                     :cnames cnames)))
    (let* ((document (distribution-request :method :post
                                           :distribution distribution))
           (bindings (xml-bind 'distribution document)))
      (bindings-distribution bindings))))

(defun %delete-distribution (distribution)
  (multiple-value-bind (document headers code)
      (distribution-request :url-suffix (format nil "/~A" (id distribution))
                            :distribution distribution
                            :method :delete)
    (declare (ignore document headers))
    (= code 204)))

(defgeneric delete-distribution (distribution)
  (:method ((distribution distribution))
    (call-with-latest #'%delete-distribution distribution)))

(defgeneric enable (distribution)
  (:documentation
   "Mark DISTRIBUTION as enabled. Enabling can take time to take
   effect; the STATUS of DISTRIBUTION will change from \"InProgress\"
   to \"Deployed\" when fully enabled.")
  (:method ((distribution distribution))
    (with-saved-modifications (d distribution)
      (setf (enabledp d) t))))


(defgeneric disable (distribution)
  (:documentation
   "Mark DISTRIBUTION as disabled. Like ENABLE, DISABLE may take some
   time to take effect.")
  (:method ((distribution distribution))
    (with-saved-modifications (d distribution)
      (setf (enabledp d) nil)
      t)))

(defgeneric ensure-cname (distribution cname)
  (:documentation
   "Add CNAME to DISTRIBUTION's list of CNAMEs, if not already
   present.")
  (:method ((distribution distribution) cname)
    (with-saved-modifications (d distribution)
      (pushnew cname (cnames d)
               :test #'string-equal))))

(defgeneric remove-cname (distribution cname)
  (:method (cname (distribution distribution))
    (with-saved-modifications (d distribution)
      (setf (cnames d)
            (remove cname (cnames distribution)
                    :test #'string-equal)))))

(defgeneric set-comment (distribution comment)
  (:method ((distribution distribution) comment)
    (with-saved-modifications (d distribution)
      (setf (comment d) comment))))

(defun distributions-for-bucket (bucket-name)
  "Return a list of distributions that are associated with BUCKET-NAME."
  (setf bucket-name (canonical-distribution-bucket-name bucket-name))
  (remove bucket-name
          (all-distributions)
          :test-not #'string-equal
          :key #'origin-bucket))


;;; Invalidation

(defclass invalidation ()
  ((id
    :initarg :id
    :accessor id
    :initform "*unset*"
    :documentation "Amazon's assigned unique ID.")
   (distribution
    :initarg :distribution
    :accessor distribution
    :initform nil)
   (create-time
    :initarg :create-time
    :initform 0
    :accessor create-time)
   (status
    :initarg :status
    :accessor status
    :initform "InProgress")
   (caller-reference
    :initarg :caller-reference
    :initform (generate-caller-reference)
    :accessor caller-reference)
   (paths
    :initarg :paths
    :accessor paths
    :initform '())))

(defmethod print-object ((invalidation invalidation) stream)
  (print-unreadable-object (invalidation stream :type t)
    (format stream "~S [~A]"
            (id invalidation)
            (status invalidation))))


(defbinder invalidation-batch
  ("InvalidationBatch"
   (sequence :paths ("Path" (bind :path)))
   ("CallerReference" (bind :caller-reference))))

(defbinder invalidation
  ("Invalidation"
   ("Id" (bind :id))
   ("Status" (bind :status))
   ("CreateTime" (bind :create-time))
   (include invalidation-batch)))

(defmethod merge-bindings ((invalidation invalidation) bindings)
  (setf (id invalidation) (bvalue :id bindings)
        (status invalidation) (bvalue :status bindings)
        (create-time invalidation) (parse-amazon-timestamp
                                    (bvalue :create-time bindings))
        (paths invalidation)
        (mapcar #'url-decode
                (mapcar (bfun :path) (bvalue :paths bindings))))
  invalidation)

(defgeneric distribution-id (object)
  (:method ((invalidation invalidation))
    (id (distribution invalidation))))

(defun invalidation-request (invalidation &key (url-suffix "")
                             (method :get) content)
  (distribution-request :method method
                        :url-suffix (format nil "/~A/invalidation~A"
                                            (distribution-id invalidation)
                                            url-suffix)
                        :content content))

(defun invalidation-batch-document (invalidation)
  (with-xml-output
    (with-element "InvalidationBatch"
      (attribute "xmlns" "http://cloudfront.amazonaws.com/doc/2010-08-01/")
      (dolist (path (paths invalidation))
        (with-element "Path"
          (text path)))
      (with-element "CallerReference"
        (text (caller-reference invalidation))))))


(defun invalidate-paths (distribution paths)
  (let* ((invalidation (make-instance 'invalidation
                                      :distribution distribution
                                      :paths paths))
         (response
          (invalidation-request invalidation
                                :method :post
                                :content (invalidation-batch-document invalidation))))
    (merge-bindings invalidation (xml-bind 'invalidation response))))


(defmethod refresh ((invalidation invalidation))
  (let ((document
         (invalidation-request invalidation
                               :url-suffix (format nil "/~A"
                                                   (id invalidation)))))
    (merge-bindings invalidation (xml-bind 'invalidation document))))
