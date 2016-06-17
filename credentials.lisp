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
;;;; credentials.lisp

(in-package #:zs3)

(defvar *credentials* nil
  "Used as the default initarg value of :CREDENTIALS when creating a
request.")

(define-condition unsupported-credentials (error)
  ((object
    :initarg :object
    :accessor unsupported-credentials-object))
  (:report (lambda (c s)
             (format s "The value ~A is unsupported as S3 credentials. (Did you set *CREDENTIALS*?)~@
                         See http://www.xach.com/lisp/zs3/#*credentials* ~
                         for supported credentials formats."
                     (unsupported-credentials-object c)))))

(defgeneric access-key (credentials)
  (:method (object)
    (error 'unsupported-credentials :object object))
  (:method ((list cons))
    (first list)))

(defgeneric secret-key (credentials)
  (:method (object)
    (error 'unsupported-credentials :object object))
  (:method ((list cons))
    (second list)))

(defgeneric security-token (credentials)
  (:method ((object t))
    nil)
  (:method ((list cons))
    (third list)))


;;; Lazy-loading credentials

(defclass lazy-credentials-mixin () ())

(defmethod slot-unbound ((class t) (credentials lazy-credentials-mixin)
                         (slot (eql 'access-key)))
  (nth-value 0 (initialize-lazy-credentials credentials)))

(defmethod slot-unbound ((class t) (credentials lazy-credentials-mixin)
                         (slot (eql 'secret-key)))
  (nth-value 1 (initialize-lazy-credentials credentials)))

(defmethod slot-unbound ((class t) (credentials lazy-credentials-mixin)
                         (slot (eql 'security-token)))
  (nth-value 2 (initialize-lazy-credentials credentials)))


;;; Loading credentials from a file

(defclass file-credentials (lazy-credentials-mixin)
  ((file
    :initarg :file
    :accessor file)
   (access-key
    :accessor access-key)
   (secret-key
    :accessor secret-key)
   (security-token
    :accessor security-token)))

(defgeneric initialize-lazy-credentials (credentials)
  (:method ((credentials file-credentials))
    (with-open-file (stream (file credentials))
      (values (setf (access-key credentials) (read-line stream))
              (setf (secret-key credentials) (read-line stream))
              (setf (security-token credentials) (read-line stream nil))))))

(defun file-credentials (file)
  (make-instance 'file-credentials
                 :file file))
