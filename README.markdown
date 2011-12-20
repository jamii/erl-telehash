Current state of the erlang switch

* Stuff that is solid and well tested

  * Nothing!

* Stuff that probably works

  * Sending and receiving telexes

  * Dialing

  * Routing

  * Taps

* Stuff that is yet to come

  * Sane configuration

  * Startup scripts

  * ring/line/br

  * Tests!

  * Example apps

* Stuff that would one day be nice

  * Admin console

  * Simplified localhost interface

  * More tests!

Startup:

    ./rebar get-deps
    ./rebar compile
    erl -pa ebin/ deps/proper/ebin/ -s telehash -port 42424
