# Bazel Conduit #

Target to generate a go proto library for BEP:
`bazel build //:go_build_proto`

`go_grpc_library` target that is available in `@googleapis` isn't supported via bzlmod. That's why we need to create our own.


## Build Event Stream Workflow ##
### Lifecycle Events ###
1. `started` marks the beginning of the build. We can skip all fields except for `started`
2. `unstructuredCommandLine` contains `unstructuredCommandLine` as a payload that contains current bazel command
along with its arguments
3. `build_metadata` is empty in our case, so we can ignore this event type for now
4. `options_parsed` is a contiunation of `unstructuredCommandLine` that splits unstructured command into startup options, explicit startup options, etc.
5. `structuredCommandLine` is again a contiunation of 2 and 4 that. Can be skipped for the time being.
6. `pattern` contains the list of configured targets, it's got `children` attribute that has the full list of all bazel targets that were configured (to be built).
7. `workspace` contains information about current workspace and its root.
8. `configuration` contains platform information and `makeVariable` map.
9. `workspaceStatus` contains some basic information about the user, timestamp, etc.
10. `convenienceSymlinksIdentified` - list of typical bazel convenience links (bazel-bin, bazel-out, etc.)
11. `buildFinished` is when the build is indeed finished and its overall status.
12. `buildMetrics` - some metadata about the build (how many actions of which kind, etc.), also declares the end of BEP's GRPC stream.

So the relations are:
```
one pattern -> many targets configured
                     one target configured -> one target completed
                                            one target completed -> one named set
```
also, at least lifecycle and build process are loosely coupled with one another. For instance, lifecycle message `pattern` contains the expanded list of configured targets . Then each `configured target` message will have the list of children with `target completed` message. And then there will be a separate `target completed` message that is linked to `named set` message that contains a list of outputs of that particular target.


### Real Build Events ###
1. `targetConfigured` is the main event that contains the information about target. In `children` we will
see `TargetCompleted` with the target's label and configuration id, in `configured` -> `targetKind`.
2. `namedSet` contains `namedSetOfFiles`. They are linked to `targetCompleted` messages via `fileSets` that contain
ids of `namedSets`. Most probably, there is a correlation between children in `targetConfigured` and separate
`TargetCompleted` build events. It looks like as soon as we have a target configured we need to track
children to add `targetCompleted` build events to them along with `namedSet`. So `targetConfigured` is the main
span, children are subspans and namedSets should just add additional attribute with the list of output files.
So namedSets shouldn't have their spans (in OTel terms) at all.
3. `targetCompleted` contains the information about completion, wheter it was successful or not and an output group that corresponds 
to `namedSet`'s id. They also have a section whether it was in fact completed or aborted/skipped.

Duration can be computed by a simple formula `targetCompleted event time - targetConfigured event time`

### Progress Events ###
`progress` messages just have stdout and stderr of the build process (INFO lines, compiler warnings, etc.).
Some of them are pretty much empty and contain no useful information, therefore, should be ignored entirely.
For simplicity we can ignore `children` for now and only extract the content of `progress` field.
Example of a garbage progress event:
```json
  {
    "eventTime": "2025-12-03T09:43:51.130Z",
    "bazelEvent": {
      "@type": "type.googleapis.com/build_event_stream.BuildEvent",
      "id": {
        "progress": {
          "opaqueCount": 1
        }
      },
      "children": [
        {
          "progress": {
            "opaqueCount": 2
          }
        },
        {
          "configuration": {
            "id": "096a4b319133cdcd98a221c3f6665f696be55030d03063bfae0cdd290e1a4323"
          }
        }
      ],
      "progress": {} <- no content
    }
  }
```

Okay, so progress messages with an empty progress attribute do correlate with other events reporting 
that they are still in progress (just a theory). I saw another one that contained id of `namedSet`.

```json
  {
    "eventTime": "2025-12-03T09:43:51.281Z",
    "bazelEvent": {
      "@type": "type.googleapis.com/build_event_stream.BuildEvent",
      "id": {
        "progress": {
          "opaqueCount": 6
        }
      },
      "children": [
        {
          "progress": {
            "opaqueCount": 7
          }
        },
        {
          "namedSet": {
            "id": "3"
          }
        }
      ],
      "progress": {}
    }
  }
```