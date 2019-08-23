package outputs

import (
	"github.com/sundy-li/burrowx/model"
)

type Output interface {
	Start() error
	SaveMessage(msg *model.ConsumerFullOffset) error
	Stop() error
}
