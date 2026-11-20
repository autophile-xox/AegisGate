FROM golang:1.24.4-alpine3.22 AS build
WORKDIR /src
COPY go.mod faults.go main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /upstream-simulator .

FROM alpine:3.22
COPY --from=build /upstream-simulator /usr/local/bin/upstream-simulator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/upstream-simulator"]
