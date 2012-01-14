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
;;;; request.lisp

(in-package #:zs3)

(defvar *s3-endpoint* "s3.amazonaws.com")
(defvar *use-ssl* nil)
(defvar *use-content-md5* t)

(defclass request ()
  ((credentials
    :initarg :credentials
    :accessor credentials
    :documentation "An object that has methods for ACCESS-KEY and
    SECRET-KEY. A list of two strings (the keys) suffices.")
   (endpoint
    :initarg :endpoint
    :accessor endpoint)
   (ssl
    :initarg :ssl
    :accessor ssl)
   (method
    :initarg :method
    :accessor method
    :documentation "e.g. :GET, :PUT, :DELETE")
   (bucket
    :initarg :bucket
    :accessor bucket
    :documentation
    "A string naming the bucket to address in the request. If NIL,
    request is not directed at a specific bucket.")
   (key
    :initarg :key
    :accessor key
    :documentation
    "A string naming the key to address in the request. If NIL,
    request is not directed at a specific key.")
   (sub-resource
    :initarg :sub-resource
    :accessor sub-resource
    :documentation "A sub-resource to address as part of the request,
    without a leading \"?\", e.g. \"acl\", \"torrent\". If PARAMETERS
    is set, this must be NIL.")
   (parameters
    :initarg :parameters
    :accessor parameters
    :documentation
    "An alist of string key/value pairs to send as CGI-style GET
    parameters with the request. If SUB-RESOURCE is set, these must be
    NIL.")
   (content-type
    :initarg :content-type
    :accessor content-type)
   (content-md5
    :initarg :content-md5
    :accessor content-md5)
   (content-length
    :initarg :content-length
    :accessor content-length)
   (content
    :initarg :content
    :accessor content)
   (metadata
    :initarg :metadata
    :accessor metadata
    :documentation
    "An alist of Amazon metadata to attach to a request. These should
    be straight string key/value pairs, WITHOUT any \"x-amz-meta-\"
    prefix.")
   (amz-headers
    :initarg :amz-headers
    :accessor amz-headers
    :documentation
    "An alist of extra Amazon request headers. These should be
    straight string key/value pairs, WITHOUT any \"x-amz-\" prefix.")
   (date
    :initarg :date
    :accessor date)
   (signed-string
    :initarg :signed-string
    :accessor signed-string)
   (extra-http-headers
    :initarg :extra-http-headers
    :accessor extra-http-headers
    :documentation "An alist of extra HTTP headers to include in the request."))
  (:default-initargs
   ;; :date and :content-md5 are specially treated, should not be nil
   :credentials *credentials*
   :method :get
   :endpoint *s3-endpoint*
   :ssl *use-ssl*
   :bucket nil
   :key nil
   :sub-resource nil
   :parameters nil
   :content-type nil
   :content-length t
   :content nil
   :metadata nil
   :amz-headers nil
   :extra-http-headers nil))

(defmethod slot-unbound ((class t) (request request) (slot (eql 'date)))
  (setf (date request) (get-universal-time)))

(defmethod slot-unbound ((class t) (request request) (slot (eql 'content-md5)))
  (setf (content-md5 request)
        (and *use-content-md5*
             (pathnamep (content request))
             (file-md5/b64 (content request)))))

(defmethod initialize-instance :after ((request request)
                                       &rest initargs &key
                                       &allow-other-keys)
  (declare (ignore initargs))
  (unless (integerp (content-length request))
    (let ((content (content request)))
      (setf (content-length request)
            (etypecase content
              (null 0)
              (pathname (file-size content))
              (vector (length content)))))))

(defgeneric http-method (request)
  (:method (request)
    (string-upcase (method request))))

(defun puri-canonicalized-path (path)
  (let ((parsed (puri:parse-uri (format nil "http://dummy~A" path))))
    (with-output-to-string (stream)
      (if (puri:uri-path parsed)
          (write-string (puri:uri-path parsed) stream)
          (write-string "/" stream))
      (when (puri:uri-query parsed)
        (write-string "?" stream)
        (write-string (puri:uri-query parsed) stream)))))

(defgeneric signed-path (request)
  (:method (request)
    (let ((*print-pretty* nil))
      (puri-canonicalized-path
       (with-output-to-string (stream)
         (write-char #\/ stream)
         (when (bucket request)
           (write-string (url-encode (name (bucket request))) stream)
           (write-char #\/ stream))
         (when (key request)
           (write-string (url-encode (name (key request))) stream))
         (when (sub-resource request)
           (write-string "?" stream)
           (write-string (url-encode (sub-resource request)) stream)))))))

(defgeneric request-path (request)
  (:method (request)
    (let ((*print-pretty* nil))
      (with-output-to-string (stream)
        (write-char #\/ stream)
        (when (and (bucket request)
                   (string= (endpoint request) *s3-endpoint*))
          (write-string (url-encode (name (bucket request))) stream)
          (write-char #\/ stream))
        (when (key request)
          (write-string (url-encode (name (key request))) stream))
        (when (sub-resource request)
          (write-string "?" stream)
          (write-string (url-encode (sub-resource request)) stream))))))

(defgeneric all-amazon-headers (request)
  (:method (request)
    (nconc
     (loop for ((key . value)) on (amz-headers request)
           collect (cons (format nil "x-amz-~(~A~)" key)
                         value))
     (loop for ((key . value)) on (metadata request)
           collect (cons (format nil "x-amz-meta-~(~A~)" key)
                         value)))))

(defgeneric amazon-header-signing-lines (request)
  (:method (request)
    ;; FIXME: handle values with commas, and repeated headers
    (let* ((headers (all-amazon-headers request))
           (sorted (sort headers #'string< :key #'car)))
      (loop for ((key . value)) on sorted
            collect (format nil "~A:~A" key value)))))

(defgeneric date-string (request)
  (:method (request)
    (http-date-string (date request))))

(defgeneric signature (request)
  (:method (request)
    (let ((digester (make-digester (secret-key request))))
      (flet ((maybe-add-line (string digester)
               (if string
                   (add-line string digester)
                   (add-newline digester))))
        (add-line (http-method request) digester)
        (maybe-add-line (content-md5 request) digester)
        (maybe-add-line (content-type request) digester)
        (add-line (date-string request) digester)
        (dolist (line (amazon-header-signing-lines request))
          (add-line line digester))
        (add-string (signed-path request) digester)
        (setf (signed-string request)
              (get-output-stream-string (signed-stream digester)))
        (digest64 digester)))))
        
(defgeneric drakma-headers (request)
  (:method (request)
    (let ((base
           (list* (cons "Date" (http-date-string (date request)))
                  (cons "Authorization"
                        (format nil "AWS ~A:~A"
                                (access-key request)
                                (signature request)))
                  (all-amazon-headers request))))
      (when (content-md5 request)
        (push (cons "Content-MD5" (content-md5 request)) base))
      (append (extra-http-headers request) base))))

(defgeneric url (request)
  (:method (request)
    (format nil "http~@[s~*~]://~A~A"
            (ssl request)
            (endpoint request)
            (request-path request))))

(defun send-file-content (fun request)
  (with-open-file (stream (content request)
                          :element-type '(unsigned-byte 8))
    (let* ((buffer-size 8000)
           (buffer (make-octet-vector buffer-size)))
      (flet ((read-exactly (size)
               (assert (= size (read-sequence buffer stream)))))
        (multiple-value-bind (loops rest)
            (truncate (content-length request) buffer-size)
          (dotimes (i loops)
            (read-exactly buffer-size)
            (funcall fun buffer t))
          (read-exactly rest)
          (funcall fun (subseq buffer 0 rest) nil))))))

(defgeneric send (request &key want-stream)
  (:method (request &key want-stream)
    (let ((continuation
           (drakma:http-request (url request)
                                :redirect nil
                                :want-stream want-stream
                                :content-type (content-type request)
                                :additional-headers (drakma-headers request)
                                :method (method request)
                                :force-binary t
                                :content-length (content-length request)
                                :parameters (parameters request)
                                :content :continuation)))
      (let ((content (content request)))
        (if (pathnamep content)
            (send-file-content continuation request)
            (funcall continuation content nil))))))
    
(defmethod access-key ((request request))
  (access-key (credentials request)))

(defmethod secret-key ((request request))
  (secret-key (credentials request)))

