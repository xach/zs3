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
;;;; utils.lisp

(in-package #:zs3)

(defvar *months* #("Jan" "Feb" "Mar" "Apr" "May" "Jun"
                   "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"))

(defvar *days* #("Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun"))

(deftype octet ()
  '(unsigned-byte 8))

(deftype octet-vector (&optional size)
  `(simple-array octet (,size)))

(deftype empty-vector ()
  `(vector * 0))

(defun http-date-string (&optional (time (get-universal-time)))
  "Return a HTTP-style date string."
  (multiple-value-bind (second minute hour day month year day-of-week)
      (decode-universal-time time 0)
    (let ((*print-pretty* nil))
      (format nil "~A, ~2,'0D ~A ~4,'0D ~2,'0D:~2,'0D:~2,'0D GMT"
              (aref *days* day-of-week)
              day
              (aref *months* (1- month))
              year
              hour
              minute
              second))))

(defun iso8601-date-string (&optional (time (get-universal-time)))
  "Return an ISO8601-style date string."
  (multiple-value-bind (second minute hour day month year)
      (decode-universal-time time 0)
    (let ((*print-pretty* nil))
      (format nil "~4,'0D-~2,'0D-~2,'0DT~2,'0D:~2,'0D:~2,'0DZ"
              year month day hour minute second))))

(defun iso8601-basic-timestamp-string (&optional (time (get-universal-time)))
  "Return an ISO8601-style basic date string."
  (multiple-value-bind (second minute hour day month year)
      (decode-universal-time time 0)
    (let ((*print-pretty* nil))
      (format nil "~4,'0D~2,'0D~2,'0DT~2,'0D~2,'0D~2,'0DZ"
              year month day hour minute second))))

(defun iso8601-basic-date-string (&optional (time (get-universal-time)))
  "Return an ISO8601-style basic date string."
  (multiple-value-bind (second minute hour day month year)
      (decode-universal-time time 0)
    (declare (ignore second minute hour))
    (let ((*print-pretty* nil))
      (format nil "~4,'0D~2,'0D~2,'0D"
              year month day))))

(defun string-octets (string)
  "Return the UTF-8 encoding of STRING as a vector of octets."
  (flexi-streams:string-to-octets string :external-format :utf-8))

(defun string64 (string)
  (cl-base64:usb8-array-to-base64-string (string-octets string)))

(defun url-decode (string)
  (with-output-to-string (out)
    (let ((code 0))
      (labels ((in-string (char)
                 (case char
                   (#\%
                    #'h1)
                   (t
                    (write-char char out)
                    #'in-string)))
               (weight (char)
                 (let ((weight (digit-char-p char 16)))
                   (unless weight
                     (error "~S is not a hex digit" char))
                   weight))
               (h1 (char)
                 (setf code (ash (weight char) 4))
                 #'h2)
               (h2 (char)
                 (incf code (weight char))
                 (write-char (code-char code) out)
                 #'in-string))
        (let ((state #'in-string))
          (loop for char across string
                do (setf state (funcall state char))))))))

;;; The following two functions were adatpted from Drakma source. It
;;; has been adapted to more closely follow the description of
;;; UriEncode() in
;;; http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

(defun url-encode (string &key (encode-slash t))
  "Returns a URL-encoded version of the string STRING using the
LispWorks external format EXTERNAL-FORMAT."
  (let ((*print-pretty* nil))
    (with-output-to-string (out)
      (loop for octet across (string-octets (or string ""))
            for char = (code-char octet)
            ;;'A'-'Z', 'a'-'z', '0'-'9', '-', '.', '_', and '~'
            do (cond ((or (char<= #\0 char #\9)
                          (char<= #\a char #\z)
                          (char<= #\A char #\Z)
                          (and (not encode-slash) (char= char #\/))
                          (find char "-._~" :test #'char=))
                      (write-char char out))
                     ((or (char= char #\Space)
                          (char= char #\+))
                      (write-string "%20" out))
                     (t (format out "~:@(%~2,'0x~)" octet)))))))

(defun alist-to-url-encoded-string (alist)
  "ALIST is supposed to be an alist of name/value pairs where both
names and values are strings.  This function returns a string where
this list is represented as for the content type
`application/x-www-form-urlencoded', i.e. the values are URL-encoded
using the external format EXTERNAL-FORMAT, the pairs are joined with a
#\\& character, and each name is separated from its value with a #\\=
character."
  (let ((*print-pretty* nil))
    (with-output-to-string (out)
      (loop for first = t then nil
            for (name . value) in alist
            unless first do (write-char #\& out)
            do (format out "~A=~A"
                       (url-encode name)
                       (url-encode value))))))

(defun save (response file)
  "Write a sequence of octets RESPONSE to FILE."
  (with-open-file (stream file :direction :output
                               :if-exists :supersede
                               :element-type 'octet)
    (write-sequence response stream))
  (probe-file file))

(defun parse-amazon-timestamp (string)
  "Convert the ISO 8601-format STRING to a universal time."
  (flet ((number-at (start length)
           (parse-integer string :start start :end (+ start length))))
    (let ((year (number-at 0 4))
          (month (number-at 5 2))
          (day (number-at 8 2))
          (hour (number-at 11 2))
          (minute (number-at 14 2))
          (second (number-at 17 2)))
      (encode-universal-time second minute hour day month year 0))))

(defun stringify (thing)
  (typecase thing
    (string thing)
    (symbol (symbol-name thing))
    (t (princ-to-string thing))))

(defun parameters-alist (&rest args &key &allow-other-keys)
  "Construct an ALIST based on all keyword arguments passed to the
function. Keywords are converted to their lowercase symbol name and
values are converted to strings."
  (loop for (key value) on args by #'cddr
        when value
        collect (cons (if (symbolp key)
                          (string-downcase (symbol-name key))
                          key)
                      (stringify value))))

(defun last-entry (array)
  "If ARRAY has one ore more entries, return the last one. Otherwise,
return NIL."
  (and (plusp (length array))
       (aref array (1- (length array)))))

(defun file-size (file)
  (with-open-file (stream file :element-type 'octet)
    (file-length stream)))

(defvar +unix-time-difference+
  (encode-universal-time 0 0 0 1 1 1970 0))

(defun unix-time (&optional (universal-time (get-universal-time)))
  (- universal-time +unix-time-difference+))

(defun octet-vector (&rest octets)
  (make-array (length octets) :element-type 'octet
              :initial-contents octets))

(defun keywordify (string-designator)
  (intern (string string-designator) :keyword))

(defun make-octet-vector (size)
  (make-array size :element-type 'octet))

(defun now+ (delta)
  (+ (get-universal-time) delta))

(defun now- (delta)
  (- (get-universal-time) delta))

(defun copy-n-octets (count input output)
  "Copy the first N octets from the stream INPUT to the stream OUTPUT."
  (let ((buffer (make-octet-vector 4096)))
    (multiple-value-bind (loops rest)
        (truncate count 4096)
      (dotimes (i loops)
        (read-sequence buffer input)
        (write-sequence buffer output))
      (let ((trailing-count (read-sequence buffer input :end rest)))
        (assert (= trailing-count rest))
        (write-sequence buffer output :end rest)))))

(defun starts-with (prefix string)
  (and (<= (length prefix) (length string))
       (string= prefix string :end2 (length prefix))))

(defun ends-with (suffix string)
  (and (<= (length suffix) (length string))
       (string= suffix string :start2 (- (length string)
                                         (length suffix)))))

;;; Getting stream/file subset vectors

(defparameter *file-buffer-size* 8192)

(defun make-file-buffer ()
  (make-octet-vector *file-buffer-size*))

(defun read-exactly-n-octets (stream n &optional buffer)
  "Read exactly N octets from STREAM into BUFFER. If fewer than N
octets are read, signal an CL:END-OF-FILE error. If BUFFER is not
supplied or is NIL, create a fresh buffer of length N and return it."
  (unless buffer
    (setf buffer (make-octet-vector n)))
  (let ((end (min (length buffer) n)))
    (let ((count (read-sequence buffer stream :end end)))
      (unless (= n count)
        (error 'end-of-file :stream stream))
      buffer)))

(defun read-complete-file-buffer (stream &optional buffer)
  "Read a complete buffer of size *FILE-BUFFER-SIZE*."
  (read-exactly-n-octets stream *file-buffer-size* buffer))

(defun merge-file-buffers (buffers size)
  "Create one big vector from BUFFERS and TRAILER."
  (let ((output (make-octet-vector size))
        (start 0))
    (dotimes (i (ceiling size *file-buffer-size*))
      (replace output (pop buffers) :start1 start)
      (incf start *file-buffer-size*))
    output))

(defun skip-stream-octets (stream count)
  "Read and discard COUNT octets from STREAM."
  (let ((buffer (make-file-buffer)))
    (multiple-value-bind (loops rest)
        (truncate count *file-buffer-size*)
      (dotimes (i loops)
        (read-complete-file-buffer stream buffer))
      (read-exactly-n-octets stream rest buffer)))
  t)

(defun drained-stream-vector (stream)
  "Read octets from STREAM until EOF and them as an octet vector."
  (let ((buffers '())
        (size 0))
    (loop
     (let* ((buffer (make-file-buffer))
            (count (read-sequence buffer stream)))
       (incf size count)
       (push buffer buffers)
       (when (/= count *file-buffer-size*)
         (return (merge-file-buffers (nreverse buffers) size)))))))


(defun partial-stream-vector (stream n)
  "Read N octets from STREAM and return them in an octet vector."
  (let ((buffers '()))
    (multiple-value-bind (loops rest)
        (truncate n *file-buffer-size*)
      (dotimes (i loops)
        (let ((buffer (make-file-buffer)))
          (read-complete-file-buffer stream buffer)
          (push buffer buffers)))
      (push (read-exactly-n-octets stream rest) buffers)
      (merge-file-buffers (nreverse buffers) n))))

(defun stream-subset-vector (stream start end)
  (unless start
    (setf start 0))
  (when (minusp start)
    (error "START must be non-negative"))
  (when (and end (< end start))
    (error "END must be greater than START"))
  (when (plusp start)
    (skip-stream-octets stream start))
  (if (not end)
      (drained-stream-vector stream)
      (partial-stream-vector stream (- end start))))

(defun file-subset-vector (file start end)
  (with-open-file (stream file :element-type 'octet)
    (stream-subset-vector stream start end)))

(defun alist-plist (alist)
  (loop for (key . value) in alist
        collect key collect value))
