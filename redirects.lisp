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
;;;; redirects.lisp

(in-package #:zs3)

(defvar *permanent-redirects*
  (make-hash-table :test 'equalp)
  "Some bucket operations make permanent redirects to different
endpoints. This table stores access-key/bucket redirects for use when
creating requests.")

(defun redirect-key (endpoint bucket &key
                     ((:credentials *credentials*) *credentials*))
  (list endpoint bucket (access-key *credentials*)))


(defun redirection-data (endpoint bucket
                         &key ((:credentials *credentials*) *credentials*))
  (gethash (redirect-key endpoint bucket) *permanent-redirects*))

(defun redirected-endpoint (endpoint bucket
                            &key ((:credentials *credentials*) *credentials*))
  (or (first (redirection-data endpoint bucket)) endpoint))

(defun redirected-region (endpoint bucket &key
                          ((:credentials *credentials*) *credentials*))
  (second (redirection-data endpoint bucket)))

(defun (setf redirection-data) (new-value endpoint bucket
                                &key ((:credentials *credentials*) *credentials*))
  (check-type new-value list)
  (let ((key (redirect-key endpoint bucket)))
    (if (not new-value)
        (progn (remhash key *permanent-redirects*) new-value)
        (setf (gethash key *permanent-redirects*) new-value))))

(defun clear-redirects ()
  (clrhash *permanent-redirects*))

