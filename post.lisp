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
;;;; post.lisp

(in-package #:zs3)

(defclass post-policy ()
  ((expires
    :initarg :expires
    :accessor expires)
   (conditions
    :initarg :conditions
    :accessor conditions)))

(defgeneric policy-serialize (object stream))

(defmethod policy-serialize ((condition cons) stream)
  (destructuring-bind (type field value &optional value2)
      condition
    (ecase type
      ((:eq :starts-with)
       (format stream "[~S, \"$~A\", ~S]"
               (string-downcase type)
               field
               value))
      (:range
       (format stream "[~S, ~D, ~D]" field value value2)))))

(defmethod policy-serialize ((policy post-policy) stream)
  (format stream "{\"expiration\": ~S, \"conditions\": ["
          (iso8601-date-string (expires policy)))
  (when (conditions policy)
    (destructuring-bind (first &rest rest)
        (conditions policy)
      (when first
        (policy-serialize first stream)
        (dolist (condition rest)
          (format stream ",")
          (policy-serialize condition stream)))))
  (format stream "]}"))

(defun policy-string64 (policy)
  (string64
   (with-output-to-string (stream)
     (policy-serialize policy stream))))

(defun policy-signature (key policy)
  (sign-string key (policy-string64 policy)))


