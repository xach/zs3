;;;; tests.lisp
;;;;
;;;; This is for simple prerelase sanity testing, not general
;;;; use. Please ignore.

(defpackage #:zs3-tests
  (:use #:cl #:zs3))

(in-package #:zs3-tests)

(setf *credentials* (file-credentials "~/.aws"))

(defparameter *test-bucket* "zs3-tests33")

(when (bucket-exists-p *test-bucket*)
  (delete-bucket *test-bucket*))

(create-bucket *test-bucket*)

(put-file "/etc/issue" *test-bucket* "printcap")
(put-string "Hello, world" *test-bucket* "hello")
(put-string "Plus good" *test-bucket* "plus+good")
(put-vector (octet-vector 8 6 7 5 3 0 9) *test-bucket* "jenny")

(all-buckets)
(all-keys *test-bucket*)

(delete-object *test-bucket* "printcap")
(delete-object *test-bucket* "hello")
(delete-object *test-bucket* "plus+good")
(delete-object *test-bucket* "jenny")

(put-string "Hello, world" *test-bucket* "hello" :start 1 :end 5)
(string= (get-string *test-bucket* "hello")
         (subseq "Hello, world" 1 5))

(put-file "tests.lisp" *test-bucket* "self" :start 1 :end 5)
(string= (get-string *test-bucket* "self")
         ";;; ")

(defparameter *jenny* (octet-vector 8 6 7 5 3 0 9))
(put-vector *jenny* *test-bucket* "jenny" :start 1 :end 6 :public t)

(equalp (get-vector *test-bucket* "jenny")
        (subseq *jenny* 1 6))

(drakma:http-request (resource-url :bucket *test-bucket* :key "jenny"))

(delete-object *test-bucket* "hello")
(delete-object *test-bucket* "self")
(delete-object *test-bucket* "jenny")

(put-string "Tildedot" *test-bucket* "slash~dot")
(put-string "Spacedot" *test-bucket* "slash dot")

(delete-object *test-bucket* "slash/dot")
(delete-object *test-bucket* "slash~dot")
(delete-object *test-bucket* "slash dot")

;;; Subresources

(put-string "Fiddle dee dee" *test-bucket* "fiddle")
(make-public :bucket *test-bucket* :key "fiddle")
(make-private :bucket *test-bucket* :key "fiddle")
(delete-object *test-bucket* "fiddle")

;;; Different regions

(delete-bucket *test-bucket*)

(create-bucket *test-bucket* :location "eu-central-1")
(put-string "Hello, world" *test-bucket* "hello")

;;; CloudFront distributions

(defparameter *distro*
  (create-distribution *test-bucket*
                       :cnames "zs3-tests.cdn.wigflip.com"
                       :enabled nil
                       :comment "Testing, 1 2 3"))

(progn
  (sleep 240)
  (delete-distribution *distro*))

(delete-bucket *test-bucket*)
