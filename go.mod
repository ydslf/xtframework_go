module xtframework

go 1.25.2

require xtnet v1.0.0

require (
	google.golang.org/protobuf v1.36.6
	gopkg.in/yaml.v3 v3.0.1
)

//replace xtnet => github.com/ydslf/xtnet_go v1.0.1-0.20260817093338-9ecf0fa1d358

replace xtnet => ../xtnet_go
