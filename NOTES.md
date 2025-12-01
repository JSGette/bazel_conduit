# Bazel Conduit #

Target to generate a go proto library for BEP:
`bazel build //:go_build_proto`

`go_grpc_library` targets aren't supported via bzlmod. That's why we need to create our own.