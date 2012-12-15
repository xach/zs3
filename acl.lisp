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
;;;; acl.lisp

(in-package #:zs3)

(defclass access-control-list ()
  ((owner
    :initarg :owner
    :accessor owner)
   (grants
    :initarg :grants
    :accessor grants)))

(defmethod print-object ((object access-control-list) stream)
  (print-unreadable-object (object stream :type t)
    (format stream "owner ~S, ~D grant~:P"
            (display-name (owner object))
            (length (grants object)))))

(defclass grant ()
  ((permission
    :initarg :permission
    :accessor permission)
   (grantee
    :initarg :grantee
    :accessor grantee)))

(defclass acl-person (person) ())

(defmethod slot-unbound ((class t) (object acl-person) (slot (eql 'display-name)))
  (setf (display-name object) (id object)))

(defclass acl-email ()
  ((email
    :initarg :email
    :accessor email)))

(defmethod print-object ((email acl-email) stream)
  (print-unreadable-object (email stream :type t)
    (prin1 (email email) stream)))

(defclass acl-group ()
  ((label
    :initarg :label
    :accessor label)
   (uri
    :initarg :uri
    :accessor uri)))

(defmethod slot-unbound ((class t) (group acl-group) (slot (eql 'label)))
  (setf (label group) (uri group)))

(defmethod print-object ((group acl-group) stream)
  (print-unreadable-object (group stream :type t)
    (prin1 (label group) stream)))

(defgeneric grantee-for-print (grantee)
  (:method ((grantee person))
    (display-name grantee))
  (:method ((grantee acl-group))
    (label grantee))
  (:method ((grantee acl-email))
    (email grantee)))

(defmethod print-object ((grant grant) stream)
  (print-unreadable-object (grant stream :type t)
    (format stream "~S to ~S"
            (permission grant)
            (grantee-for-print (grantee grant)))))

(defparameter *permissions*
  '((:read . "READ")
    (:write . "WRITE")
    (:read-acl . "READ_ACP")
    (:write-acl . "WRITE_ACP")
    (:full-control . "FULL_CONTROL")))

(defun permission-name (permission)
  (or (cdr (assoc permission *permissions*))
      (error "Unknown permission - ~S" permission)))

(defun permission-keyword (permission)
  (or (car (rassoc permission *permissions* :test 'string=))
      (error "Unknown permission - ~S" permission)))
           
(defparameter *all-users*
  (make-instance 'acl-group
                 :label "AllUsers"
                 :uri "http://acs.amazonaws.com/groups/global/AllUsers"))

(defparameter *aws-users*
  (make-instance 'acl-group
                 :label "AWSUsers"
                 :uri "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"))

(defparameter *log-delivery*
  (make-instance 'acl-group
                 :label "LogDelivery"
                 :uri "http://acs.amazonaws.com/groups/s3/LogDelivery"))

(defgeneric acl-serialize (object))

(defmethod acl-serialize ((person person))
  (with-element "ID" (text (id person)))
  (with-element "DisplayName" (text (display-name person))))

(defvar *xsi* "http://www.w3.org/2001/XMLSchema-instance")

(defgeneric xsi-type (grantee)
  (:method ((grantee acl-group))
    "Group")
  (:method ((grantee person))
    "CanonicalUser")
  (:method ((grantee acl-email))
    "AmazonCustomerByEmail"))

(defmethod acl-serialize ((grantee acl-group))
  (simple-element "URI" (uri grantee)))

(defmethod acl-serialize ((grantee acl-email))
  (simple-element "EmailAddress" (email grantee)))

(defmethod acl-serialize ((grant grant))
  (with-element "Grant"
    (with-element "Grantee"
      (attribute* "xmlns" "xsi" *xsi*)
      (attribute* "xsi" "type" (xsi-type (grantee grant)))
      (acl-serialize (grantee grant)))
    (simple-element "Permission" (permission-name (permission grant)))))

(defmethod acl-serialize ((acl access-control-list))
  (with-xml-output
    (with-element "AccessControlPolicy"
      (attribute "xmlns" "http://s3.amazonaws.com/doc/2006-03-01/")
      (with-element "Owner"
        (acl-serialize (owner acl)))
      (with-element "AccessControlList"
        (dolist (grant (remove-duplicates (grants acl) :test #'acl-eqv))
          (acl-serialize grant))))))


;;; Parsing XML ACL responses

(defbinder access-control-policy
  ("AccessControlPolicy"
   ("Owner"
    ("ID" (bind :owner-id))
    ("DisplayName" (bind :owner-display-name)))
   ("AccessControlList"
    (sequence :grants
              ("Grant"
               ("Grantee"
                (elements-alist :grantee))
               ("Permission" (bind :permission)))))))

(defclass acl-response (response)
  ((acl
    :initarg :acl
    :accessor acl)))

(set-element-class "AccessControlPolicy" 'acl-response)

(defgeneric acl-eqv (a b)
  (:method (a b)
    (eql a b))
  (:method ((a acl-group) (b acl-group))
    (string= (uri a) (uri b)))
  (:method ((a person) (b person))
    (string= (id a) (id b)))
  (:method ((a grant) (b grant))
    (and (eql (permission a) (permission b))
         (acl-eqv (grantee a) (grantee b)))))

(defun ensure-acl-group (uri)
  (cond ((string= uri (uri *all-users*))
         *all-users*)
        ((string= uri (uri *aws-users*))
         *aws-users*)
        ((string= uri (uri *log-delivery*))
         *log-delivery*)
        (t
         (make-instance 'acl-group :uri uri))))

(defun alist-grant (bindings)
  (let* ((permission (bvalue :permission bindings))
         (alist (bvalue :grantee bindings))
         (group-uri (assoc "URI" alist :test 'string=))
         (user-id (assoc "ID" alist :test 'string=))
         (email (assoc "EmailAddress" alist :test 'string=))
         (display-name (assoc "DisplayName" alist :test 'string=)))
    (make-instance 'grant
                   :permission (permission-keyword permission)
                   :grantee (cond (group-uri
                                   (ensure-acl-group (cdr group-uri)))
                                  (user-id
                                   (make-instance 'acl-person
                                                  :id (cdr user-id)
                                                  :display-name
                                                  (cdr display-name)))
                                  (email
                                   (make-instance 'acl-email
                                                  :email (cdr email)))))))

(defmethod specialized-initialize ((response acl-response) source)
  (let* ((bindings (xml-bind 'access-control-policy source))
         (owner (make-instance 'acl-person
                               :id (bvalue :owner-id bindings)
                               :display-name (bvalue :owner-display-name bindings)))
         (grants (mapcar 'alist-grant (bvalue :grants bindings))))
    (setf (acl response)
          (make-instance 'access-control-list
                         :owner owner
                         :grants grants))
    response))


(defun grant (permission &key to)
  (make-instance 'grant :permission permission :grantee to))

(defun acl-email (address)
  (make-instance 'acl-email :email address))

(defun acl-person (id &optional display-name)
  (make-instance 'acl-person
                 :id id
                 :display-name (or display-name id)))
