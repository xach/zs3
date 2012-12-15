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
;;;; logging.lisp

(in-package #:zs3)

(defclass logging-setup ()
  ((target-bucket
    :initarg :target-bucket
    :accessor target-bucket)
   (target-prefix
    :initarg :target-prefix
    :accessor target-prefix)
   (target-grants
    :initarg :target-grants
    :accessor target-grants))
  (:default-initargs
   :target-bucket nil
   :target-prefix nil
   :target-grants nil))

(defclass logging (response)
  ((setup
    :initarg :setup
    :accessor setup)))

(defbinder bucket-logging-status
  ("BucketLoggingStatus"
   (optional
    ("LoggingEnabled"
     ("TargetBucket" (bind :target-bucket))
     ("TargetPrefix" (bind :target-prefix))
     (optional
      ("TargetGrants"
       (sequence :target-grants
                 ("Grant"
                  ("Grantee"
                   (elements-alist :grantee))
                  ("Permission" (bind :permission))))))))))


(defun bindings-logging-setup (bindings)
  (alist-bind (target-bucket target-prefix target-grants)
      bindings
    (make-instance 'logging-setup
                   :target-bucket target-bucket
                   :target-prefix target-prefix
                   :target-grants (mapcar 'alist-grant target-grants))))

(defgeneric log-serialize (object)
  (:method ((logging-setup logging-setup))
    (with-xml-output
      (with-element "BucketLoggingStatus"
        (when (target-bucket logging-setup)
          (with-element "LoggingEnabled"
            (simple-element "TargetBucket" (target-bucket logging-setup))
            (simple-element "TargetPrefix" (target-prefix logging-setup))
            (when (target-grants logging-setup)
              (with-element "TargetGrants"
                (dolist (grant (target-grants logging-setup))
                  (acl-serialize grant))))))))))


(set-element-class "BucketLoggingStatus" 'logging)

(defmethod specialized-initialize ((response logging) source)
  (let ((bindings (xml-bind 'bucket-logging-status source)))
    (setf (setup response)
          (bindings-logging-setup bindings))
    response))

