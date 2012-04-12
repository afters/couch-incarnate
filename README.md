Couch Incarnate
===============

A CouchDB tool for maintaining views of views (AKA chaining map-reduce operations).

## Requirements ##

*   Node.js (tried on 0.6.2)
*   CouchDB (tried on 1.1.1)
*   NPM - Node's package manager (https://github.com/isaacs/npm)

## Installation ##

*   run:

         npm install couch-incarnate

*   edit 'conf.json' file. Remove the "log" entry to write to stdout.
*   done! Now simply run:

         incarnate

## Incarnation - the basic concept ##

An incarnation is a CouchDB that is equivalent to a CouchDB view. Like a view, this DB is based on map and reduce functions, and each document in it is of the form '{key: ..., value: ...}'.

Once an incarnation is set up, you could add to it a design-doc with a view (or several), ending up with what is effectively a view of a view.

To be more precise, an incarnation is based on a source DB, a map-function, a reduce function, and a group-level. 

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


#### Rename incarnator ####

    MOVE /INCARNATOR_NAME
    Destination: NEW_INCARNATOR_NAME


#### Access incarnation ####

    METHOD_NAME /INCARNATOR_NAME/REDUCE_NAME/GROUP_LEVEL/[...]

## Persistence ##

Persistence is aimed to be on par with CouchDB's. 

Note: currently, unless running on Mac OS X, both CouchDB and Incarnate would provide absolute disk-persistence only if the disk has no cache on-board, or if the disk has a back-up power supply, in case of a power failure. See here for more: http://lwn.net/Articles/270891/

## TODO ##

- tests. Does this thing really work?
- input validation
- _changes

## Caveats ##

- not properly tested
- limited input validation
- limited error-handling
- only JS map and reduce functions are supported
- no SSL
- no continuous changes-feed for incarnations

## Copyright ##

Copyright (c) 2011 Alon Keren <alon.keren@gmail.com>

You can redistribute this software and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

A copy of the GNU Affero General Public License is in the file named 'LICENSE.txt'. 
It should be also available at: <http://www.gnu.org/licenses/>

