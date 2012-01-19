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
;;;; objects.lisp

(in-package #:zs3)

(defclass person ()
  ((id
    :initarg :id
    :accessor id)
   (display-name
    :initarg :display-name
    :accessor display-name)))

(defmethod print-object ((person person) stream)
  (print-unreadable-object (person stream :type t)
    (format stream "~S" (display-name person))))

(defclass bucket ()
  ((name
    :initarg :name
    :accessor name)
   (creation-date
    :initarg :creation-date
    :accessor creation-date)))

(defmethod print-object ((bucket bucket) stream)
  (print-unreadable-object (bucket stream :type t)
    (format stream "~S" (name bucket))))

(defmethod name ((string string))
  string)

(defclass key ()
  ((name
    :initarg :name
    :accessor name)
   (last-modified
    :initarg :last-modified
    :accessor last-modified)
   (etag
    :initarg :etag
    :accessor etag)
   (size
    :initarg :size
    :accessor size)
   (owner
    :initarg :owner
    :accessor owner)
   (storage-class
    :initarg :storage-class
    :accessor storage-class)))

(defmethod print-object ((key key) stream)
  (print-unreadable-object (key stream :type t)
    (format stream "~S ~D" (name key) (size key))))
