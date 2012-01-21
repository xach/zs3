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
              ("Expiration"
               ("Days" (bind :days)))))))

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
    :initarg :days
    :accessor days)))

(defmethod print-object ((rule lifecycle-rule) stream)
  (print-unreadable-object (rule stream :type t)
    (format stream "~S expire prefix ~S in ~D day~:P (~:[disabled~;enabled~])"
            (id rule)
            (prefix rule)
            (days rule)
            (enabledp rule))))

;;; FIXME: The GFs for ENABLE and DISABLE should really be moved
;;; somewhere out of cloudfront.lisp now that I'm adding more methods.

(defmethod disable ((rule lifecycle-rule))
  (setf (enabledp rule) nil))

(defmethod enable ((rule lifecycle-rule))
  (setf (enabledp rule) t))

(defun lifecycle-rule (&key id prefix (enabled t) days)
  (unless id
    (setf id (string (gensym))))
  (unless prefix
    (error "Missing PREFIX argument"))
  (unless days
    (error "Missing DAYS argument"))
  (make-instance 'lifecycle-rule
                 :id id
                 :prefix prefix
                 :enabledp enabled
                 :days days))

(defun lifecycle-document (rules)
  (cxml:with-xml-output (cxml:make-octet-vector-sink)
    (cxml:with-element "LifecycleConfiguration"
      (dolist (rule rules)
        (cxml:with-element "Rule"
          (cxml:with-element "ID"
            (cxml:text (id rule)))
          (cxml:with-element "Prefix"
            (cxml:text (prefix rule)))
          (cxml:with-element "Status"
            (cxml:text (if (enabledp rule)
                           "Enabled"
                           "Disabled")))
          (cxml:with-element "Expiration"
            (cxml:with-element "Days"
              (cxml:text (princ-to-string (days rule))))))))))

(defun bindings-lifecycle-rules (bindings)
  (let ((rules '()))
    (dolist (rule-bindings (bvalue :rules bindings) (nreverse rules))
      (alist-bind (id prefix status days)
          rule-bindings
        (push (make-instance 'lifecycle-rule
                             :id id
                             :prefix prefix
                             :enabledp (string= status "Enabled")
                             :days (parse-integer days))
              rules)))))

(define-specific-error (no-such-lifecycle-configuration
                        "NoSuchLifecycleConfiguration")
    () ())

(defun bucket-lifecycle (bucket)
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

(defun delete-bucket-lifecycle (bucket)
  "Delete the lifecycle configuration of BUCKET."
  (submit-request (make-instance 'request
                                 :method :delete
                                 :bucket bucket
                                 :sub-resource "lifecycle")))

(defun (setf bucket-lifecycle) (rules bucket)
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
    (submit-request (make-instance 'request
                                   :method :put
                                   :bucket bucket
                                   :sub-resource "lifecycle"
                                   :content-md5 md5
                                   :content content))))
