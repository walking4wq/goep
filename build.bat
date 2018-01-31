set GOPATH=D:\coding\ztesoft\golang\goep;D:\coding\ztesoft\blockchain\ethereum\geth\go_get_github
set GO15VENDOREXPERIMENT=1
rem set GOBIN=D:\coding\ztesoft\golang\goep\bin

SET CGO_ENABLED=0
SET GOOS=linux
rem SET GOOS=windows linux android darwin
SET GOARCH=amd64
rem SET GOARCH=arm 386

go install -x walking/cmd/goep
go install -x walking/cmd/cdr_tapin

rem go build -o bin/gofast.exe ./src/walking/cmd/gofast
rem go test walking/svr -v -run="Json"
rem go test walking/test -v -run="Chan" -bench=Chan_.Chan -benchmem
