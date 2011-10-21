Couch Incarnate
===============

CouchDB views in DB-form; for chaining map-reduce operations.


(This is by no means production-ready. See 'caveats' at the bottom.)

## Installation ##
*   create the 'log' and 'home' directories as detailed in the 'conf' file
*   run:

       ./bin/incarnate.js

### Requirements ###

*   Node.js (tried on 0.4.5)
*   CouchDB (tried on 1.0.1)
*   Node's 'request' module

## Incarnation - the basic concept ##

An incarnation is a CouchDB that is equivalent to a CouchDB view. Each document in it is of the form '{key: ..., value: ...}'.

An incarnation is based on a source DB, a map-function, a reduce function, and a group-level. 

Contents are equivalent to the following query, if instead of an incarnation, a normal view were used, with the same map and reduce functions:

    GET /SOURCE_DB/_design/views/MAP_REDUCE_VIEW/?reduce=true&group_level=GROUP_LEVEL

### Incarnation Updates ###

When trying to access an incarnation, it is first updated from the source DB, and only then does the actual access takes place.

This is much like the way views are queried without the 'stale' option.

## Incarnator ##

An incarnator defines incarnations. All incarnations defined by it use the same source DB and map function, as follows:

    {
      "source": "SOURCE_DB_URL",
      "map": "function (doc) { ... }",
      "reduces": {
        "REDUCE_1_NAME": {
          "function": "function (key, values, rereduce) { ... }",
          "group_levels": [ GROUP_LEVEL1, GROUP_LEVEL2, ... ]
        },
        "INC_2_NAME": {
          ...
        },
        ...
      }
    }

With the above configuration, at least two incarnations would be created. Both would have the same map and reduce functions, but each would have a different group-level.

### Example ###

Let's say your application holds documents in a DB called 'my_db', which look like this:

    {
      type: "user",
      id: "blaster77",
      join_date: "2011/05/29", // YYYY/MM/DD
    }

Now say you want to have an incarnation holding the number of users that join each year. The incarnator configuration would look like this:

    {
      "source": "http://localhost:5984/my_db",
      "map": "function (doc) { if (doc.type === "user") emit(doc.join_date.split('/')[0], null); }",
      "reduces": {
        "count": {
          "function": "function (key, values, rereduce) { if (!rereduce) return {count: values.length}; var count = 0; for (var i = 0; i < values.length; i++) { count += values[i]; }; return count; }",
          "group_levels": ['exact']
        }
      }
    }

If you wanted also to have incarnations for the number of users that join each month, and each day, the incarnator configuration could look like this:

    {
      "source": "http://localhost:5984/my_db",
      "map": "function (doc) { if (doc.type === "user") emit(doc.join_date.split('/'), null); }",
      "reduces": {
        "count": {
          "function": "function (key, values, rereduce) { if (!rereduce) return {count: values.length}; var count = 0; for (var i = 0; i < values.length; i++) { count += values[i]; }; return count; }",
          "group_levels": [1, 2, 'exact']
        }
      }
    }

## Incarnate server ##

An HTTP server that administrates and maintains incarnations 

### Configuration ###

Configuration file - ./conf :

    {
      "home": "/var/lib/incarnate/",
      "port": 4895,
      "couch": "http://localhost:5984",
      "log": {
        "path": "/var/log/incarnate/incarnate.log"
      }
    }

### API ###

#### Set incarnator ####

    PUT /INCARNATOR_NAME
    {
      "source": "SOURCE_DB_URL",
      "map": "function (doc) { ... }",
      "reduces": {
        "REDUCE_1_NAME": {
          "reduce": "function (key, values, rereduce) { ... }",
          "group_levels": [ GROUP_LEVEL1, GROUP_LEVEL2, ... ]
        },
        "REDUCE_2_NAME": {
          ...
        },
        ...
      }
    }

If INCARNATOR\_NAME exists, overwrite it.


#### Get incarnator status ####

    GET /INCARNATOR_NAME


#### Delete incarnator ####

    DELETE /INCARNATOR_NAME


#### Access incarnation ####

    METHOD_NAME /INCARNATOR_NAME/REDUCE_NAME/GROUP_LEVEL/[...]

## TODO ##

- tests. Does this thing really work?
- input validation
- verify persistence
- throttle couchdb multi-queries
- sandbox JS map functions
- _changes

## Caveats ##

- not properly tested
- limited input validation
- limited error-handling
- persistence still requires a little more work
- big source DB changes between incarnation updates probably wouldn't work
- only JS map and reduce functions are supported
- map functions aren't sandboxed
- no SSL
- no continuous changes-feed for incarnations

## Copyright ##

Copyright (c) 2011 Alon Keren <alon.keren@gmail.com>

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

