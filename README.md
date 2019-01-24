# Rubis

Redis (incompletely) implemented in Ruby. Built in order to learn and understand Ruby.
Not meant for any real use.

Server
```
$ bundle
$ ruby lib/server.rb
```

Client
```
$ telnet localhost 6369
set test 1
get test
"1"
```
