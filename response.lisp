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
;;;; response.lisp

(in-package #:zs3)

(defvar *response-element-classes*
  (make-hash-table :test 'equal))

(defun set-element-class (element-name class)
  (setf (gethash element-name *response-element-classes*) class))

(defclass response ()
  ((request
    :initarg :request
    :accessor request)
   (body
    :initarg :body
    :accessor body)
   (http-code
    :initarg :http-code
    :accessor http-code)
   (http-phrase
    :initarg :http-phrase
    :accessor http-phrase)
   (http-headers
    :initarg :http-headers
    :accessor http-headers))
  (:default-initargs
   :request nil
   :body nil
   :http-code 999
   :http-phrase "<uninitialized>"
   :http-headers nil))


(defmethod print-object ((response response) stream)
  (print-unreadable-object (response stream :type t :identity t)
    (format stream "~D ~S" (http-code response) (http-phrase response))))

(defgeneric xml-string (response)
  (:method (response)
    (flexi-streams:octets-to-string (body response) :external-format :utf-8)))

(defgeneric response-specialized-class (name)
  (:method (name)
    (gethash name *response-element-classes*)))

(defgeneric specialized-initialize (object source)
  (:method (object source)
    object))

(defgeneric content-length (response)
  (:method (response)
    (parse-integer (bvalue :content-length (http-headers response)))))

(defgeneric specialize-response (response)
  (:method ((response response))
    (cond ((or (null (body response))
               (and (not (streamp (body response))) 
                    (zerop (length (body response)))))
           response)
          (t
           (let* ((source (xml-source (body response)))
                  (type (xml-document-element source))
                  (class (response-specialized-class type)))
             (when class
               (change-class response class)
               (specialized-initialize response source))
             response)))))

(defun request-response (request &key
                         body-stream
                         keep-stream
                         (handler 'specialize-response))
  (setf (endpoint request) (redirected-endpoint (endpoint request)
                                                (bucket request)))
  (multiple-value-bind (body code headers uri stream must-close phrase)
      (send request :want-stream body-stream)
    (declare (ignore uri must-close))
    (let ((response
           (make-instance 'response
                          :request request
                          :body body
                          :http-code code
                          :http-phrase phrase
                          :http-headers headers)))
      (if keep-stream
          (funcall handler response)
          (with-open-stream (stream stream)
            (funcall handler response))))))

(defun submit-request (request
                       &key body-stream keep-stream
                       (handler 'specialize-response))
  (loop
   (handler-case
       (let ((response (request-response request
                                         :keep-stream keep-stream
                                         :body-stream body-stream
                                         :handler handler)))
         (maybe-signal-error response)
         (setf (request response) request)
         (return response))
     (temporary-redirect (condition)
       (setf (endpoint request)
             (request-error-endpoint condition)))
     (permanent-redirect (condition)
       ;; Remember the new endpoint long-term
       (let ((new-endpoint (request-error-endpoint condition)))
         (setf (redirected-endpoint (endpoint request)
                                    (bucket request))
               new-endpoint)
         (setf (endpoint request) new-endpoint)))
     (internal-error ()
       ;; Per the S3 docs, InternalErrors should simply be retried
       ))))
