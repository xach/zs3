;;;;
;;;; Copyright (c) 2012 Zachary Beane, All Rights Reserved
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
;;;; lifecycle.lisp

(in-package #:zs3)

;;; Object expiration for buckets

(defbinder lifecycle-configuration
  ("LifecycleConfiguration"
   (sequence :rules
             ("Rule"
              ("ID" (bind :id))
              ("Prefix" (bind :prefix))
              ("Status" (bind :status))
              (alternate
               ("Expiration"
                (alternate
                 ("Days" (bind :days))
                 ("Date" (bind :date))))
               ("Transition"
                (alternate
                 ("Days" (bind :days))
                 ("Date" (bind :date)))
                ("StorageClass" (bind :storage-class))))))))

(defclass lifecycle-rule ()
  ((id
    :initarg :id
    :accessor id)
   (prefix
    :initarg :prefix
    :accessor prefix)
   (enabledp
    :initarg :enabledp
    :accessor enabledp)
   (days
    :documentation
    "The number of days after which the rule action will take
    effect. Can be zero, meaning that it should take effect the next
    time Amazon's periodic transitioning process runs. One of DAYS or
    DATE must be provided."
    :initarg :days
    :accessor days)
   (date
    :documentation
    "The date at [XXX after?] which the rule takes effect. One of DAYS
    or DATE must be provided."
    :initarg :date
    :accessor date)
   (action
    :documentation
    "The action of this rule; must be either :EXPIRE (the default)
    or :TRANSITION. :TRANSITION means matching objects will transition
    to Glacier storage."
    :initarg :action
    :accessor action))
  (:documentation
   "A lifecycle rule. See
   http://docs.amazonwebservices.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html#intro-lifecycle-rules.")
  (:default-initargs
   :prefix (string (gensym))
   :enabledp t
   :days nil
   :date nil
   :action :expire))

(defmethod print-object ((rule lifecycle-rule) stream)
  (print-unreadable-object (rule stream :type t)
    (format stream "~S ~(~A~) prefix ~S ~
                    ~:[on ~A~;in ~:*~D day~:P~*~] ~
                    (~:[disabled~;enabled~])"
            (id rule)
            (action rule)
            (prefix rule)
            (days rule)
            (date rule)
            (enabledp rule))))

;;; FIXME: The GFs for ENABLE and DISABLE should really be moved
;;; somewhere out of cloudfront.lisp now that I'm adding more methods.

(defmethod disable ((rule lifecycle-rule))
  (setf (enabledp rule) nil))

(defmethod enable ((rule lifecycle-rule))
  (setf (enabledp rule) t))

(defun lifecycle-rule (&key id prefix (enabled t) days date
                       (action :expire))
  (unless id
    (setf id (string (gensym))))
  (unless prefix
    (error "Missing PREFIX argument"))
  (when (or (not (or days date))
            (and days date))
    (error "Exactly one of :DAYS or :DATE must be provided"))
  (make-instance 'lifecycle-rule
                 :id id
                 :prefix prefix
                 :enabledp enabled
                 :days days
                 :date date
                 :action action))

(defun lifecycle-document (rules)
  "Return an XML document that can be posted as the lifecycle
configuration of a bucket. See
http://docs.amazonwebservices.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html#intro-lifecycle-rules
for details."
  (flet ((timeframe-element (rule)
           (if (days rule)
               (with-element "Days"
                 (text (princ-to-string (days rule))))
               (with-element "Date"
                 (text (date rule))))))
    (with-xml-output
      (with-element "LifecycleConfiguration"
        (dolist (rule rules)
          (with-element "Rule"
            (with-element "ID"
              (text (id rule)))
            (with-element "Prefix"
              (text (prefix rule)))
            (with-element "Status"
              (text (if (enabledp rule)
                        "Enabled"
                        "Disabled")))
            (ecase (action rule)
              (:expire
               (with-element "Expiration"
                 (timeframe-element rule)))
              (:transition
               (with-element "Transition"
                 (timeframe-element rule)
                 (with-element "StorageClass"
                   (text "GLACIER")))))))))))

(defun bindings-lifecycle-rules (bindings)
  "Create a list of lifecycle rules from BINDINGS, which are obtained
by xml-binding the LIFECYCLE-CONFIGURATION binder with a document."
  (let ((rules '()))
    (dolist (rule-bindings (bvalue :rules bindings) (nreverse rules))
      (alist-bind (id prefix status days date storage-class)
          rule-bindings
        (push (make-instance 'lifecycle-rule
                             :id id
                             :prefix prefix
                             :enabledp (string= status "Enabled")
                             :action (if storage-class
                                         :transition
                                         :expire)
                             :date date
                             :days (and days (parse-integer days)))
              rules)))))

(define-specific-error (no-such-lifecycle-configuration
                        "NoSuchLifecycleConfiguration")
    () ())

(defun bucket-lifecycle (bucket
                         &key ((:credentials *credentials*) *credentials*)
                           ((:backoff *backoff*) *backoff*))
  "Return the bucket lifecycle rules for BUCKET. Signals
NO-SUCH-LIFECYCLE-CONFIGURATION if the bucket has no lifecycle
configuration."
  (let ((response
         (submit-request (make-instance 'request
                                        :method :get
                                        :bucket bucket
                                        :sub-resource "lifecycle"))))
    (bindings-lifecycle-rules
     (xml-bind 'lifecycle-configuration (body response)))))

(defun delete-bucket-lifecycle (bucket
                                &key
                                  ((:credentials *credentials*) *credentials*)
                                  ((:backoff *backoff*) *backoff*))
  "Delete the lifecycle configuration of BUCKET."
  (submit-request (make-instance 'request
                                 :method :delete
                                 :bucket bucket
                                 :sub-resource "lifecycle")))

(defun (setf bucket-lifecycle) (rules bucket
                                &key
                                  ((:credentials *credentials*) *credentials*)
                                  ((:backoff *backoff*) *backoff*))
  "Set the lifecycle configuration of BUCKET to RULES. RULES is
coerced to a list if needed. If RULES is NIL, the lifecycle
configuration is deleted with DELETE-BUCKET-LIFECYCLE."
  (when (null rules)
    (return-from bucket-lifecycle
      (delete-bucket-lifecycle bucket)))
  (unless (listp rules)
    (setf rules (list rules)))
  (let* ((content (lifecycle-document rules))
         (md5 (vector-md5/b64 content)))
    (values
     rules
     (submit-request (make-instance 'request
                                    :method :put
                                    :bucket bucket
                                    :sub-resource "lifecycle"
                                    :content-md5 md5
                                    :content content)))))

;;; Restoring from glacier

(defun restore-request-document (days)
  (with-xml-output
    (with-element "RestoreRequest"
      (with-element "Days"
        (text (princ-to-string days))))))

(defun restore-object (bucket key
                       &key
                         days
                         ((:credentials *credentials*) *credentials*)
                         ((:backoff *backoff*) *backoff*))
  (let* ((content (restore-request-document days))
         (md5 (vector-md5/b64 content)))
    (submit-request (make-instance 'request
                                   :method :post
                                   :content-md5 md5
                                   :sub-resource "restore"
                                   :bucket bucket
                                   :key key
                                   :content content))))

(defun object-restoration-status (bucket key
                                  &key
                                    ((:credentials *credentials*) *credentials*)
                                    ((:backoff *backoff*) *backoff*))
  (let ((headers (head :bucket bucket :key key)))
    (cdr (assoc :x-amz-restore headers))))

(define-specific-error (restore-already-in-progress
                        "RestoreAlreadyInProgress")
    () ())
