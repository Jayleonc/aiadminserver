package impl

import (
	"fmt"
	"testing"
	"time"
)

func TestGetAccountFollowList(t *testing.T) {
	fmt.Println(time.Unix(1634217446, 0).Hour())
	tt, _ := time.Parse("2006-01-02 15:04:05", "2021-01-01 11:25:00")
	fmt.Println(tt)
}
