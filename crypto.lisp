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
;;;; crypto.lisp

(in-package #:zs3)

(defparameter *empty-string-sha256*
  (ironclad:byte-array-to-hex-string
   (ironclad:digest-sequence :sha256 (make-array 0 :element-type 'octet))))

(defparameter *newline-vector*
  (make-array 1 :element-type 'octet :initial-element 10))

(defclass digester ()
  ((hmac
    :initarg :hmac
    :accessor hmac)
   (newline
    :initarg :newline
    :accessor newline
    :allocation :class)
   (signed-stream
    :initarg :signed-stream
    :accessor signed-stream))
  (:default-initargs
   :signed-stream (make-string-output-stream)
   :newline *newline-vector*))

(defun make-digester (key &key (digest-algorithm :sha1))
  (when (stringp key)
    (setf key (string-octets key)))
  (make-instance 'digester
                 :hmac (ironclad:make-hmac key digest-algorithm)))

(defgeneric add-string (string digester)
  (:method (string digester)
    (write-string string (signed-stream digester))
    (ironclad:update-hmac (hmac digester) (string-octets string))))

(defgeneric add-newline (digester)
  (:method (digester)
    (terpri (signed-stream digester))
    (ironclad:update-hmac (hmac digester) (newline digester))))

(defgeneric add-line (string digester)
  (:method (string digester)
    (add-string string digester)
    (add-newline digester)))

(defgeneric digest (digester)
  (:method (digester)
    (ironclad:hmac-digest (hmac digester))))

(defgeneric digest64 (digester)
  (:method (digester)
    (base64:usb8-array-to-base64-string
     (ironclad:hmac-digest (hmac digester)))))

(defun file-md5 (file)
   (ironclad:digest-file :md5 file))

(defun file-md5/b64 (file)
  (base64:usb8-array-to-base64-string (file-md5 file)))

(defun file-md5/hex (file)
  (ironclad:byte-array-to-hex-string (file-md5 file)))

(defun file-sha256 (file)
  (ironclad:digest-file :sha256 file))

(defun file-sha256/hex (file)
  (ironclad:byte-array-to-hex-string (file-sha256 file)))

(defun vector-sha256 (vector)
  (ironclad:digest-sequence :sha256 vector))

(defun vector-sha256/hex (vector)
  (ironclad:byte-array-to-hex-string (vector-sha256 vector)))

(defun strings-sha256/hex (strings)
  (when strings
    (let ((digest (ironclad:make-digest :sha256)))
      (ironclad:update-digest digest (string-octets (first strings)))
      (dolist (string (rest strings))
        (ironclad:update-digest digest *newline-vector*)
        (ironclad:update-digest digest (string-octets string)))
      (ironclad:byte-array-to-hex-string (ironclad:produce-digest digest)))))

(defun strings-hmac-sha256/hex (key strings)
  (when strings
    (when (stringp key)
      (setf key (string-octets key)))
    (let ((digest (ironclad:make-hmac key :sha256)))
      (ironclad:update-hmac digest (string-octets (first strings)))
      (dolist (string (rest strings))
        (ironclad:update-hmac digest *newline-vector*)
        (ironclad:update-hmac digest (string-octets string)))
      (ironclad:byte-array-to-hex-string (ironclad:hmac-digest digest)))))

(defun vector-md5/b64 (vector)
  (base64:usb8-array-to-base64-string
   (ironclad:digest-sequence :md5 vector)))

(defun file-etag (file)
  (format nil "\"~A\"" (file-md5/hex file)))

(defun sign-string (key string)
  (let ((digester (make-digester key)))
    (add-string string digester)
    (digest64 digester)))

(defun hmac-sha256 (key strings)
  (let ((digester (make-digester key :digest-algorithm :sha256)))
    (if (consp strings)
        (dolist (s strings)
          (add-string s digester))
        (add-string strings digester))
    (digest digester)))
