module git.pinquest.cn/base/aiadminserver

go 1.23.4

toolchain go1.24.1

replace (
	github.com/cloudwego/eino => ./impl/third_party/github.com/cloudwego/eino
	github.com/coreos/go-systemd => git.pinquest.cn/qlb/extrpkg/github.com/coreos/go-systemd v0.0.0-20210302022218-b498ef3236d3
	go.etcd.io/bbolt => github.com/coreos/bbolt v1.3.5
	google.golang.org/grpc/naming => git.pinquest.cn/qlb/extrpkg/google.golang.org/grpc/naming v0.0.0-20210302015939-5eeb3473985c
)

require (
	git.pinquest.cn/base/aiadmin v0.0.0-20250605175137-90e822355e41
	git.pinquest.cn/base/log v0.0.0-20250513100215-78c978529f79
	git.pinquest.cn/base/xls v0.0.0-20220129070904-b9d371335fd7
	git.pinquest.cn/qlb/brick v0.0.0-20250522061943-21a5e6a5999d
	git.pinquest.cn/qlb/commonv2 v0.0.0-20250113080903-268faef6fc8d
	git.pinquest.cn/qlb/core v0.0.0-20240802034734-87c03aface13
	git.pinquest.cn/qlb/envconfig v0.0.0-20230713091515-44d9abb0e73d
	git.pinquest.cn/qlb/qmsg v0.0.0-20250528031442-22b171de1456
	git.pinquest.cn/qlb/quan v0.0.0-20250520093733-61fb0369f685
	git.pinquest.cn/qlb/qwhale v0.0.0-20250523070923-ae10d41ed721
	git.pinquest.cn/qlb/storage v0.0.0-20250206070050-6c351636b4cb
	github.com/apex/log v1.9.0
	github.com/bytedance/sonic v1.13.2
	github.com/cloudwego/eino v0.3.27
	github.com/cloudwego/eino-ext/components/embedding/openai v0.0.0-20250512035704-1e06fdfda207
	github.com/cloudwego/eino-ext/components/model/openai v0.0.0-20250522060253-ddb617598b09
	github.com/darabuchi/log v0.0.0-20230323042819-e71eb8ef731c
	github.com/google/uuid v1.6.0
	github.com/milvus-io/milvus-sdk-go/v2 v2.4.2
	github.com/stretchr/testify v1.10.0
	github.com/tmc/langchaingo v0.1.13
	github.com/xuri/excelize/v2 v2.5.0
)

require (
	git.pinquest.cn/qlb/account v0.0.0-20230713091642-cab7dae0172f // indirect
	git.pinquest.cn/qlb/alipay v0.0.0-20230720094913-d131071c7136 // indirect
	git.pinquest.cn/qlb/customhttpproxy v0.0.0-20220713062530-ac065e3a27b0 // indirect
	git.pinquest.cn/qlb/excel v0.0.0-20220311045932-426de573bb7a // indirect
	git.pinquest.cn/qlb/extrpkg v0.0.0-20250410100419-9709a03cb901 // indirect
	git.pinquest.cn/qlb/extrpkg/github.com/coreos/etcd v0.0.0-20250410100419-9709a03cb901 // indirect
	git.pinquest.cn/qlb/extrpkg/google.golang.org/grpc v0.0.0-20250410100419-9709a03cb901 // indirect
	git.pinquest.cn/qlb/extrpkg/google.golang.org/grpc/naming v0.0.0-20250410100419-9709a03cb901 // indirect
	git.pinquest.cn/qlb/featswitch v0.0.0-20231221073242-dff8c91aee46 // indirect
	git.pinquest.cn/qlb/guiji v0.0.0-20220311050654-898a67553c2a // indirect
	git.pinquest.cn/qlb/iquan v0.0.0-20231215063507-3d28cd123dbe // indirect
	git.pinquest.cn/qlb/kuaishou v0.0.0-20230403140217-5ad31734a029 // indirect
	git.pinquest.cn/qlb/omsv2 v0.0.0-20210419080517-d7e83d693d69 // indirect
	git.pinquest.cn/qlb/orm v0.0.0-20230720042404-e776eeefaeb8 // indirect
	git.pinquest.cn/qlb/pindef v0.0.0-20210419080518-e4e4801ffdc0 // indirect
	git.pinquest.cn/qlb/qaf v0.0.0-20240121032458-ebf645db0f43 // indirect
	git.pinquest.cn/qlb/qaicaller v0.0.0-20220704174228-649b7bec5f13 // indirect
	git.pinquest.cn/qlb/qauth v0.0.0-20230317091402-3a1374d65a59 // indirect
	git.pinquest.cn/qlb/qcorpuser v0.0.0-20230221032431-17f2608cf8b6 // indirect
	git.pinquest.cn/qlb/qorder v0.0.0-20240205102605-0044bb48f022 // indirect
	git.pinquest.cn/qlb/qperm v0.0.0-20230203030328-29eb117cdc2c // indirect
	git.pinquest.cn/qlb/qrc v0.0.0-20210419080526-3fc8e875f21c // indirect
	git.pinquest.cn/qlb/qris v0.0.0-20250425082658-ee202804b6db // indirect
	git.pinquest.cn/qlb/qrobot v0.0.0-20240318103159-5a5e9d9f2371 // indirect
	git.pinquest.cn/qlb/qsm v0.0.0-20240111124122-59833262d8bb // indirect
	git.pinquest.cn/qlb/qycfile v0.0.0-20220330103042-536193df5ee9 // indirect
	git.pinquest.cn/qlb/rbacv2 v0.0.0-20250103021750-47033852df7b // indirect
	git.pinquest.cn/qlb/schedulerv2 v0.0.0-20230713081752-00b4f5dff3d9 // indirect
	git.pinquest.cn/qlb/tbapi v0.0.0-20230828075906-fb01019bab2c // indirect
	git.pinquest.cn/qlb/tencentcloudmall v0.0.0-20220816041506-2b99455af63a // indirect
	git.pinquest.cn/qlb/umspay v0.0.0-20210402024325-c38aa19c6874 // indirect
	git.pinquest.cn/qlb/websocket v0.0.0-20240328063713-cc73cef766cc // indirect
	git.pinquest.cn/qlb/wechat v0.0.0-20250331094822-9db65304330e // indirect
	git.pinquest.cn/qlb/wechatpay v0.0.0-20220816041228-167979ad1d35 // indirect
	git.pinquest.cn/qlb/wework v0.0.0-20241205124345-f27a686a34bd // indirect
	git.pinquest.cn/qlb/yc v0.0.0-20240117064431-964a122e24e7 // indirect
	git.pinquest.cn/qlb/yunfu v0.0.0-20220704173255-887c5e9ca1ea // indirect
	github.com/360EntSecGroup-Skylar/excelize/v2 v2.4.0 // indirect
	github.com/BurntSushi/toml v1.3.2 // indirect
	github.com/Luoxin/faker v0.0.0-20210916064730-fb0777f90022 // indirect
	github.com/agiledragon/gomonkey/v2 v2.13.0 // indirect
	github.com/aliyun/aliyun-oss-go-sdk v2.1.6+incompatible // indirect
	github.com/andybalholm/brotli v1.0.5 // indirect
	github.com/aws/aws-sdk-go v1.44.83 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/brahma-adshonor/gohook v1.1.9 // indirect
	github.com/bytedance/sonic/loader v0.2.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/cloudwego/eino-ext/libs/acl/openai v0.0.0-20250519084852-38fafa73d9ea // indirect
	github.com/cockroachdb/errors v1.9.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/coreos/etcd v3.3.27+incompatible // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/coreos/pkg v0.0.0-20230601102743-20bbbf26f4d8 // indirect
	github.com/darabuchi/utils v0.0.0-20230323042840-e89c2789cb92 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dlclark/regexp2 v1.10.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.10 // indirect
	github.com/elliotchance/pie v1.39.0 // indirect
	github.com/extrame/goyymmdd v0.0.0-20210114090516-7cc815f00d1a // indirect
	github.com/extrame/ole2 v0.0.0-20160812065207-d69429661ad7 // indirect
	github.com/gabriel-vasile/mimetype v1.4.2 // indirect
	github.com/getkin/kin-openapi v0.118.0 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.21.1 // indirect
	github.com/go-openapi/swag v0.23.1 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.14.1 // indirect
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/gofiber/fiber/v2 v2.47.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/gookit/color v1.5.2 // indirect
	github.com/goph/emperror v0.17.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/howeyc/fsnotify v0.9.0 // indirect
	github.com/invopop/yaml v0.1.0 // indirect
	github.com/jchavannes/go-pgp v0.0.0-20200131171414-e5978e6d02b4 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.6 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mcuadros/go-defaults v1.2.0 // indirect
	github.com/meguminnnnnnnnn/go-openai v0.0.0-20250408071642-761325becfd6 // indirect
	github.com/milvus-io/milvus-proto/go-api/v2 v2.4.10-0.20240819025435-512e3b98866a // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/gls v0.0.0-20220109145502-612d0167dce5 // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/mozillazg/go-httpheader v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/nikolalohinski/gonja v1.5.3 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/petermattis/goid v0.0.0-20250508124226-395b08cebbdb // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkoukk/tiktoken-go v0.1.6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.16.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.0 // indirect
	github.com/qiniu/api.v7/v7 v7.8.2 // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.1 // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/savsgio/dictpool v0.0.0-20221023140959-7bf2e61cea94 // indirect
	github.com/savsgio/gotils v0.0.0-20230208104028-c358bd845dee // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/slongfield/pyfmt v0.0.0-20220222012616-ea85ff4c361f // indirect
	github.com/tealeg/xlsx v1.0.5 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.48.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/xuri/efp v0.0.0-20220216053911-6d8731f62184 // indirect
	github.com/yargevad/filepathx v1.0.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	gitlab.com/golang-commonmark/html v0.0.0-20191124015941-a22733972181 // indirect
	gitlab.com/golang-commonmark/linkify v0.0.0-20191026162114-a0c2df6c8f82 // indirect
	gitlab.com/golang-commonmark/markdown v0.0.0-20211110145824-bf3e522c626a // indirect
	gitlab.com/golang-commonmark/mdurl v0.0.0-20191124015652-932350d1cb84 // indirect
	gitlab.com/golang-commonmark/puny v0.0.0-20191124015043-9f83538fa04f // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/arch v0.15.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
	golang.org/x/net v0.37.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250428153025-10db94c68c34 // indirect
	google.golang.org/grpc v1.71.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
