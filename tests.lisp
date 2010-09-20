;;;; tests.lisp
;;;;
;;;; This is for simple prerelase sanity testing, not general
;;;; use. Please ignore.

(defpackage #:zs3-tests
  (:use #:cl #:zs3))

(in-package #:zs3-tests)

(setf *credentials* (file-credentials "~/.aws"))

(when (bucket-exists-p "zs3-tests")
  (delete-bucket "zs3-tests"))

(create-bucket "zs3-tests")

(put-file "/etc/issue" "zs3-tests" "printcap")
(put-string "Hello, world" "zs3-tests" "hello")
(put-vector (octet-vector 8 6 7 5 3 0 9) "zs3-tests" "jenny")

(all-buckets)
(all-keys "zs3-tests")

(delete-object "zs3-tests" "printcap")
(delete-object "zs3-tests" "hello")
(delete-object "zs3-tests" "jenny")

(put-string "Hello, world" "zs3-tests" "hello" :start 1 :end 5)
(string= (get-string "zs3-tests" "hello")
         (subseq "Hello, world" 1 5))

(put-file "tests.lisp" "zs3-tests" "self" :start 1 :end 5)
(string= (get-string "zs3-tests" "self")
         ";;; ")

(defparameter *jenny* (octet-vector 8 6 7 5 3 0 9))
(put-vector *jenny* "zs3-tests" "jenny" :start 1 :end 6)

(equalp (get-vector "zs3-tests" "jenny")
        (subseq *jenny* 1 6))


(delete-object "zs3-tests" "hello")
(delete-object "zs3-tests" "self")
(delete-object "zs3-tests" "jenny")


;;; Testing signing issues

(put-string "Slashdot" "zs3-tests" "slash/dot")
(put-string "Tildedot" "zs3-tests" "slash~dot")
(put-string "Spacedot" "zs3-tests" "slash dot")

(delete-object "zs3-tests" "slash/dot")
(delete-object "zs3-tests" "slash~dot")
(delete-object "zs3-tests" "slash dot")

;;; Subresources

(put-string "Fiddle dee dee" "zs3-tests" "fiddle")
(make-public :bucket "zs3-tests" :key "fiddle")
(make-private :bucket "zs3-tests" :key "fiddle")
(delete-object "zs3-tests" "fiddle")

;;; CloudFront distributions

(defparameter *distro*
  (create-distribution "zs3-tests"
                       :cnames "zs3-tests.cdn.wigflip.com"
                       :enabled nil
                       :comment "Testing, 1 2 3"))

(progn
  (sleep 240)
  (delete-distribution *distro*))

(delete-bucket "zs3-tests")
