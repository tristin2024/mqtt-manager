package mq

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gitlab.beeps.cn/common/logs"
)

type PreModel map[string]func(client mqtt.Client, message mqtt.Message)

var (
	onConnectCallBack mqtt.OnConnectHandler = func(client mqtt.Client) {
		options := client.OptionsReader()
		clientId := options.ClientID()
		logs.Std.Debug("mqtt " + clientId + " client connect success ")
		for topic, callback := range PreList {

			token := client.Subscribe(topic, 2, callback)
			// if token.WaitTimeout(5 * time.Second) {
			// 	logs.Std.Errorf("subscribe topic %s timeout", topic)
			// }
			if token.Error() != nil {
				logs.Std.Error(token.Error())
			}

		}
	}

	deleteRetained   = "delete_retained"
	deleteDelayQueue = "delete_delay_queue"
	PreList          PreModel
	mClient          mqtt.Client
	emqAddr          = ""
	emqUser          = ""
	emqPwd           = ""
)

func getMqttOpts(ClientId string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().AddBroker(emqAddr).SetClientID(ClientId)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetConnectTimeout(60 * time.Second)
	opts.SetUsername(emqUser)
	opts.SetPassword(emqPwd)
	opts.SetProtocolVersion(4)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(onConnectCallBack)
	opts.AutoReconnect = true
	return opts
}

// 预订阅
func PreSub(preList PreModel) {
	PreList = preList
}

// 初始化mqtt
func InitMqtt(clientId, host, user, passwd string) {
	emqAddr = host
	emqUser = user
	emqPwd = passwd
	opts := getMqttOpts(clientId)
	mClient = mqtt.NewClient(opts)
	token := mClient.Connect()

	// if token.WaitTimeout(1500 * time.Millisecond) {
	// 	logs.Std.Errorf("mqtt connect timeout")
	// 	return
	// }
	if token.Error() != nil {
		logs.Std.Error(token.Error())
	}

}

func parseDelayTopic(topic string, delayTimes int64) string {
	return fmt.Sprintf("$delayed/%d/%s", delayTimes, topic)
}

// 延迟发送
func PublishWithDelay(topic string, payload interface{}, delayTimes int64, isRetained bool) error {
	if delayTimes < 0 || delayTimes > 4294967 {
		return fmt.Errorf("times range error")
	}
	token := mClient.Publish(parseDelayTopic(topic, delayTimes), 2, isRetained, payload)
	// if token.WaitTimeout(5 * time.Second) {
	// 	err := fmt.Errorf("publish topic %s timeout", topic)
	// 	logs.Std.Error(err)
	// 	return err
	// }
	if token.Error() != nil {
		logs.Std.Error(token.Error())
		return token.Error()
	}
	if isRetained && delayTimes > 0 {
		err := RemoveRetained(topic, delayTimes)
		if err != nil {
			return err
		}
	}
	return nil
}

// 移除保留消息,非延迟消息delayTimes填0
func RemoveRetained(topic string, delayTimes int64) error {
	if delayTimes == 0 {
		return Publish(deleteRetained, []byte(topic))
	} else {
		return Publish(deleteRetained, []byte(parseDelayTopic(topic, delayTimes)))
	}
}

// 移除延迟队列
func RemoveDelayQueue(topic string) error {
	return Publish(deleteDelayQueue, []byte(topic))
}

// 推送保留消息
func PublishRetained(topic string, payload interface{}) error {
	token := mClient.Publish(topic, 2, true, payload)
	// if token.WaitTimeout(5 * time.Second) {
	// 	err := fmt.Errorf("publish topic %s timeout", topic)
	// 	logs.Std.Error(err)
	// 	return err
	// }
	if token.Error() != nil {
		logs.Std.Error(token.Error())
		return token.Error()
	}
	return nil
}

// 推送消息
func Publish(topic string, payload interface{}) error {
	token := mClient.Publish(topic, 2, false, payload)
	// if token.WaitTimeout(5 * time.Second) {
	// 	err := fmt.Errorf("publish topic %s timeout", topic)
	// 	logs.Std.Error(err)
	// 	return err
	// }
	if token.Error() != nil {
		logs.Std.Error(token.Error())
		return token.Error()
	}
	return nil
}

// 订阅
func Subscribe(topic string, callback func(client mqtt.Client, message mqtt.Message)) error {
	token := mClient.Subscribe(topic, 2, callback)
	// if token.WaitTimeout(5 * time.Second) {
	// 	err := fmt.Errorf("publish topic %s timeout", topic)
	// 	logs.Std.Error(err)
	// 	return err
	// }
	if token.Error() != nil {
		logs.Std.Error(token.Error())
		return token.Error()
	}
	return nil
}
