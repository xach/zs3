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
;;;; bucket-listing.lisp

(in-package #:zs3)

(defbinder all-buckets
  ("ListAllMyBucketsResult"
   ("Owner"
    ("ID" (bind :owner-id))
    ("DisplayName" (bind :display-name)))
   ("Buckets"
    (sequence :buckets
              ("Bucket"
               ("Name" (bind :name))
               ("CreationDate" (bind :creation-date)))))))

(defclass all-buckets (response)
  ((owner
    :initarg :owner
    :accessor owner)
   (buckets
    :initarg :buckets
    :accessor buckets)))

(set-element-class "ListAllMyBucketsResult" 'all-buckets)

(defmethod specialized-initialize ((response all-buckets) source)
  (let ((bindings (xml-bind 'all-buckets source)))
    (setf (owner response)
          (make-instance 'person
                         :id (bvalue :owner-id bindings)
                         :display-name (bvalue :display-name bindings)))
    (let* ((bucket-bindings (bvalue :buckets bindings))
           (buckets (make-array (length bucket-bindings))))
      (setf (buckets response) buckets)
      (loop for i from 0
            for ((nil . name) (nil . timestamp)) in bucket-bindings
            do (setf (aref buckets i)
                     (make-instance 'bucket
                                    :name name
                                    :creation-date (parse-amazon-timestamp timestamp)))))))


(defbinder list-bucket-result
  ("ListBucketResult"
   ("Name" (bind :bucket-name))
   ("Prefix" (bind :prefix))
   ("Marker" (bind :marker))
   (optional
    ("NextMarker" (bind :next-marker)))
   ("MaxKeys" (bind :max-keys))
   (optional
    ("Delimiter" (bind :delimiter)))
   ("IsTruncated" (bind :truncatedp))
   (sequence :keys
             ("Contents"
              ("Key" (bind :key))
              ("LastModified" (bind :last-modified))
              ("ETag" (bind :etag))
              ("Size" (bind :size))
              (optional
               ("Owner"
                ("ID" (bind :owner-id))
                (optional ("DisplayName" (bind :owner-display-name)))))
              ("StorageClass" (bind :storage-class))))
   (sequence :common-prefixes
             ("CommonPrefixes"
              ("Prefix" (bind :prefix))))))

(defclass bucket-listing (response)
  ((bucket-name
    :initarg :bucket-name
    :accessor bucket-name)
   (prefix
    :initarg :prefix
    :accessor prefix)
   (marker
    :initarg :marker
    :accessor marker)
   (next-marker
    :initarg :next-marker
    :accessor next-marker)
   (max-keys
    :initarg :max-keys
    :accessor max-keys)
   (delimiter
    :initarg :delimiter
    :accessor delimiter)
   (truncatedp
    :initarg :truncatedp
    :accessor truncatedp)
   (keys
    :initarg :keys
    :accessor keys)
   (common-prefixes
    :initarg :common-prefixes
    :accessor common-prefixes))
  (:default-initargs
   :next-marker nil
   :delimiter nil
   :prefix nil
   :max-keys nil))

(defmethod print-object ((response bucket-listing) stream)
  (print-unreadable-object (response stream :type t)
    (format stream "~S~@[ (truncated)~]"
            (bucket-name response)
            (truncatedp response))))


(set-element-class "ListBucketResult" 'bucket-listing)

(defun key-binding-key (binding)
  (alist-bind (key
               last-modified etag size
               owner-id owner-display-name
               storage-class)
      binding
    (make-instance 'key
                   :name key
                   :last-modified (parse-amazon-timestamp last-modified)
                   :etag etag
                   :size (parse-integer size)
                   :owner (when owner-id
                            (make-instance 'person
                                           :id owner-id
                                           :display-name owner-display-name))
                   :storage-class storage-class)))

(defmethod specialized-initialize ((response bucket-listing) source)
  (let* ((bindings (xml-bind 'list-bucket-result source))
         (bucket-name (bvalue :bucket-name bindings)))
    (setf (bucket-name response) bucket-name)
    (setf (prefix response) (bvalue :prefix bindings))
    (setf (marker response) (bvalue :marker bindings))
    (setf (next-marker response) (bvalue :next-marker bindings))
    (setf (max-keys response) (parse-integer (bvalue :max-keys bindings)))
    (setf (delimiter response) (bvalue :delimiter bindings))
    (setf (truncatedp response) (equal (bvalue :truncatedp bindings)
                                       "true"))
    (setf (keys response)
          (map 'vector
               (lambda (key-binding)
                 (key-binding-key key-binding))
               (bvalue :keys bindings)))
    (setf (common-prefixes response)
          (map 'vector #'cdar (bvalue :common-prefixes bindings))))) 

(defgeneric successive-marker (response)
  (:method ((response bucket-listing))
    (when (truncatedp response)
      (let* ((k1 (next-marker response))
             (k2 (last-entry (keys response)))
             (k3 (last-entry (common-prefixes response))))
        (cond (k1)
              ((and k2 (not k3)) (name k2))
              ((not k2) nil)
              ((string< (name k3) (name k2)) (name k2))
              (t (name k3)))))))

(defgeneric successive-request (response)
  (:method ((response bucket-listing))
    (when (truncatedp response)
      (make-instance 'request
                     :credentials (credentials (request response))
                     :method :get
                     :bucket (bucket-name response)
                     :parameters
                     (parameters-alist :max-keys (max-keys response)
                                       :delimiter (delimiter response)
                                       :marker (successive-marker response)
                                       :prefix (prefix response))))))


